# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Tests for gcs_import_to_cloud_sql.py"""
import json
import os
from datetime import date
from http import HTTPStatus
from typing import Any, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import pytest
from googleapiclient.errors import HttpError, InvalidJsonError
from httplib2 import Response
from sqlalchemy import Column, Index, Integer, String, Table, Text
from sqlalchemy.orm import DeclarativeMeta, declarative_base
from sqlalchemy.sql.ddl import CreateIndex, CreateTable, DDLElement, SetColumnComment

from recidiviz.application_data_import.server import _insights_bucket
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import (
    ModelSQL,
    build_temporary_sqlalchemy_table,
    import_gcs_csv_to_cloud_sql,
    import_gcs_file_to_cloud_sql,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.persistence.database.schema.insights.schema import (
    SupervisionClientEvent,
    SupervisionOfficer,
)
from recidiviz.persistence.database.schema.pathways.schema import (
    PrisonPopulationProjection,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_all_table_classes_in_schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.auth.helpers import add_entity_to_database_session
from recidiviz.tools.insights import fixtures as insights_fixtures
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


def build_list_constraints_query(schema: str, table: str) -> str:
    return f"""
        SELECT pg_constraint.*
        FROM pg_catalog.pg_constraint
            INNER JOIN pg_catalog.pg_class ON pg_class.oid = pg_constraint.conrelid
            INNER JOIN pg_catalog.pg_namespace ON pg_namespace.oid = connamespace
        WHERE pg_namespace.nspname = '{schema}'
            AND pg_class.relname = '{table}';
     """


def build_list_unique_indexes_query(schema: str, table: str) -> str:
    return f"""
        SELECT *
        FROM pg_index
          JOIN pg_class pg_class_index  ON pg_class_index.oid = pg_index.indexrelid
          JOIN pg_namespace ON pg_namespace.oid = pg_class_index.relnamespace
          JOIN pg_class pg_class_table ON pg_class_table.oid = pg_index.indrelid
          JOIN pg_namespace pg_namespace_table ON pg_namespace_table.oid = pg_class_table.relnamespace
        WHERE pg_index.indisunique
          AND pg_namespace_table.nspname = '{schema}'
          AND pg_class_table.relname = '{table}';
  """


FakeBase: DeclarativeMeta = declarative_base()


class FakeModel(FakeBase):
    __tablename__ = "fake_model"
    id = Column(Integer, primary_key=True)
    comment = Column(Text)

    __table_args__ = (Index("fake_model_pk", "id", unique=True),)


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@pytest.mark.uses_db
class TestGCSImportToCloudSQL(TestCase):
    """Tests for gcs_import_to_cloud_sql.py."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def setUp(self) -> None:
        self.state_code = StateCode.US_MO.value
        self.mock_instance_id = "mock_instance_id"
        self.cloud_sql_client_patcher = patch(
            "recidiviz.cloud_sql.gcs_import_to_cloud_sql.CloudSQLClientImpl"
        )
        self.mock_cloud_sql_client = MagicMock()
        self.cloud_sql_client_patcher.start().return_value = self.mock_cloud_sql_client

        self.sqlalchemy_engine_patcher = patch(
            "recidiviz.cloud_sql.gcs_import_to_cloud_sql.SQLAlchemyEngineManager.get_stripped_cloudsql_instance_id",
            return_value=self.mock_instance_id,
        )
        self.sqlalchemy_engine_patcher.start()

        self.database_key = SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, db_name="us_mo")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        self.model = PrisonPopulationProjection
        self.table_name = self.model.__tablename__
        self.columns = [col.name for col in self.model.__table__.columns]
        self.export_uri = GcsfsFilePath.from_absolute_path(
            f"{self.state_code}/{self.table_name}.csv"
        )
        self.now = "2023-08-30T13:33:09.109433"

    def tearDown(self) -> None:
        self.cloud_sql_client_patcher.stop()
        self.sqlalchemy_engine_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def _mock_load_data_from_csv(
        self, values: Optional[List[str]] = None, **_kwargs: Any
    ) -> str:
        with SessionFactory.using_database(self.database_key) as session:
            csv_values = ["('US_MO',2024,1,'HISTORICAL','ALL','ALL',23354,23354,23354)"]
            if values:
                csv_values = csv_values + values
            session.execute(
                f"INSERT INTO tmp__{self.table_name} ({','.join(self.columns)}) "
                f"VALUES {','.join(csv_values)}"
            )
            return "fake-id"

    @patch(f"{import_gcs_csv_to_cloud_sql.__module__}.SessionFactory.using_database")
    def test_import_gcs_csv_queries(self, session_mock: MagicMock) -> None:
        """Smoke test of queries issued to a database"""
        execute_mock = session_mock.return_value.__enter__.return_value.execute

        import_gcs_csv_to_cloud_sql(
            database_key=self.database_key,
            model=FakeModel,
            gcs_uri=GcsfsFilePath.from_absolute_path(
                "gs://recidiviz-test-dashboard-event-level-data/US_TN/fake_model.csv"
            ),
            columns=["id", "comment"],
        )

        issued_ddl_statements = [
            str(call.args[0]).strip()
            for call in execute_mock.mock_calls
            if len(call.args) and isinstance(call.args[0], (str, DDLElement))
        ]

        self.assertEqual(
            [
                "DROP TABLE IF EXISTS tmp__fake_model",
                "CREATE TABLE tmp__fake_model (\n\tid INTEGER NOT NULL, \n\tcomment TEXT, \n\tPRIMARY KEY (id)\n)",
                "CREATE UNIQUE INDEX tmp__fake_model_pk ON tmp__fake_model (id)",
                "DROP TABLE IF EXISTS fake_model",
                "ALTER TABLE tmp__fake_model RENAME TO fake_model",
                "ALTER INDEX tmp__fake_model_pk RENAME TO fake_model_pk",
            ],
            issued_ddl_statements,
        )

    def test_import_gcs_csv_to_cloud_sql_swaps_tables(self) -> None:
        """Assert that the temp table and destination are successfully swapped."""
        self.mock_cloud_sql_client.import_gcs_csv.side_effect = (
            self._mock_load_data_from_csv
        )
        self.mock_cloud_sql_client.wait_until_operation_completed.return_value = True

        import_gcs_csv_to_cloud_sql(
            database_key=self.database_key,
            model=self.model,
            gcs_uri=self.export_uri,
            columns=self.columns,
        )
        self.mock_cloud_sql_client.import_gcs_csv.assert_called_with(
            instance_name=self.mock_instance_id,
            db_name="us_mo",
            table_name=f"tmp__{self.table_name}",
            gcs_uri=self.export_uri,
            columns=self.columns,
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            destination_table_rows = session.query(self.model).all()

        self.assertEqual(len(destination_table_rows), 1)
        self.assertEqual(destination_table_rows[0].state_code, self.state_code)

    def test_import_gcs_csv_to_cloud_sql_with_region_code(self) -> None:
        """Assert that rows are copied to the temp table for every region code before being swapped to
        the destination table."""
        row_1 = PrisonPopulationProjection(
            state_code="US_PA",
            year=2024,
            month=1,
            simulation_tag="HISTORICAL",
            gender="ALL",
            legal_status="ALL",
            total_population=1930,
            total_population_min=1930,
            total_population_max=1930,
        )
        row_2 = PrisonPopulationProjection(
            state_code="US_PA",
            year=2024,
            month=2,
            simulation_tag="HISTORICAL",
            gender="ALL",
            legal_status="ALL",
            total_population=1930,
            total_population_min=1930,
            total_population_max=1930,
        )
        add_entity_to_database_session(self.database_key, [row_1, row_2])

        def _mock_side_effect(**_kwargs: Any) -> str:
            return self._mock_load_data_from_csv(
                values=[
                    "('US_MO',2024,3,'HISTORICAL','ALL','ALL',23354,23354,23354)",
                    "('US_MO',2024,4,'HISTORICAL','ALL','ALL',23354,23354,23354)",
                ]
            )

        self.mock_cloud_sql_client.import_gcs_csv.side_effect = _mock_side_effect
        self.mock_cloud_sql_client.wait_until_operation_completed.return_value = True

        import_gcs_csv_to_cloud_sql(
            database_key=self.database_key,
            model=self.model,
            gcs_uri=self.export_uri,
            columns=self.columns,
            region_code="US_MO",
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            destination_table_rows = session.query(PrisonPopulationProjection).all()
            state_codes = [row.state_code for row in destination_table_rows]
            self.assertEqual(len(destination_table_rows), 5)
            self.assertEqual(set(state_codes), {"US_MO", "US_PA"})

    def test_import_gcs_csv_to_cloud_sql_client_error(self) -> None:
        """Assert that CloudSQLClient errors raise an error and roll back the session."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            row_1 = PrisonPopulationProjection(
                state_code="US_PA",
                year=2024,
                month=1,
                simulation_tag="HISTORICAL",
                gender="ALL",
                legal_status="ALL",
                total_population=1930,
                total_population_min=1930,
                total_population_max=1930,
            )
            add_entity_to_database_session(self.database_key, [row_1])
            self.mock_cloud_sql_client.import_gcs_csv.side_effect = Exception(
                "Error while importing CSV to temp table"
            )
            with self.assertRaisesRegex(
                Exception, "^Error while importing CSV to temp table$"
            ):
                import_gcs_csv_to_cloud_sql(
                    database_key=self.database_key,
                    model=self.model,
                    gcs_uri=self.export_uri,
                    columns=self.columns,
                )
            destination_table_rows = session.query(PrisonPopulationProjection).all()
            self.assertEqual(len(destination_table_rows), 1)

    def test_import_gcs_csv_to_cloud_sql_session_error(self) -> None:
        """Assert that session errors raise an error and roll back the session."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            row_1 = PrisonPopulationProjection(
                state_code="US_PA",
                year=2024,
                month=1,
                simulation_tag="HISTORICAL",
                gender="ALL",
                legal_status="ALL",
                total_population=1930,
                total_population_min=1930,
                total_population_max=1930,
            )
            add_entity_to_database_session(self.database_key, [row_1])

            def _mock_side_effect(**_kwargs: Any) -> str:
                raise ValueError("An error occurred!")

            self.mock_cloud_sql_client.import_gcs_csv.side_effect = _mock_side_effect

            with self.assertRaisesRegex(Exception, "An error occurred!"):
                import_gcs_csv_to_cloud_sql(
                    database_key=self.database_key,
                    model=self.model,
                    gcs_uri=self.export_uri,
                    columns=self.columns,
                )

            destination_table_rows = session.query(PrisonPopulationProjection).all()
            self.assertEqual(len(destination_table_rows), 1)

    def test_retry(self) -> None:
        # Client first raises a Conflict error, then returns a successful operation ID.
        self.mock_cloud_sql_client.import_gcs_csv.side_effect = [
            HttpError(
                Response({"status": str(HTTPStatus.CONFLICT.value)}), b"This will retry"
            ),
            "op_id",
        ]

        import_gcs_csv_to_cloud_sql(
            database_key=self.database_key,
            model=FakeModel,
            gcs_uri=GcsfsFilePath.from_absolute_path(
                "gs://recidiviz-test-dashboard-event-level-data/US_TN/fake_model.csv"
            ),
            columns=["id", "comment"],
        )

        # should not crash!
        self.mock_cloud_sql_client.wait_until_operation_completed.assert_called()

    def test_retry_with_fatal_error(self) -> None:
        # Client first raises a Conflict error, then on retry will raise a InvalidJsonError
        self.mock_cloud_sql_client.import_gcs_csv.side_effect = [
            HttpError(
                Response({"status": str(HTTPStatus.CONFLICT.value)}), b"This will retry"
            ),
            InvalidJsonError((), b"This will fail"),
        ]

        with self.assertRaises(InvalidJsonError):
            import_gcs_csv_to_cloud_sql(
                database_key=self.database_key,
                model=FakeModel,
                gcs_uri=GcsfsFilePath.from_absolute_path(
                    "gs://recidiviz-test-dashboard-event-level-data/US_TN/fake_model.csv"
                ),
                columns=["id", "comment"],
            )


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@pytest.mark.uses_db
class TestGCSImportToCloudSQLWithGeneratedPrimaryKey(TestCase):
    """Tests for gcs_import_to_cloud_sql.py when a table has a generated primary key."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def setUp(self) -> None:
        self.mock_instance_id = "mock_instance_id"
        self.cloud_sql_client_patcher = patch(
            "recidiviz.cloud_sql.gcs_import_to_cloud_sql.CloudSQLClientImpl"
        )
        self.mock_cloud_sql_client = MagicMock()
        self.cloud_sql_client_patcher.start().return_value = self.mock_cloud_sql_client

        self.mock_sqlalchemy_engine_manager = SQLAlchemyEngineManager
        setattr(
            self.mock_sqlalchemy_engine_manager,
            "get_stripped_cloudsql_instance_id",
            Mock(return_value=self.mock_instance_id),
        )
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.INSIGHTS, db_name="us_mi")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        self.model = SupervisionClientEvent
        self.table_name = self.model.__tablename__
        self.columns = [
            col.name for col in self.model.__table__.columns if col.name != "_id"
        ]
        self.export_uri = GcsfsFilePath.from_absolute_path(
            "US_MI/supervision_client_events.csv"
        )

    def tearDown(self) -> None:
        self.cloud_sql_client_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def _mock_load_data_from_csv(self, values: List[str], **_kwargs: Any) -> str:
        with SessionFactory.using_database(self.database_key) as session:
            session.execute(
                f"INSERT INTO tmp__{self.table_name} ({','.join(self.columns)}) "
                f"VALUES {','.join(values)}"
            )
            return "fake-id"

    def test_import_gcs_file_to_cloud_sql_with_generated_primary_key(self) -> None:
        existing_row = SupervisionClientEvent(
            state_code="US_MI",
            metric_id="violations",
            event_date=date(2023, 1, 1),
            client_id="34567",
            client_name={"given_names": "fname"},
            officer_id="BCDEF",
            pseudonymized_client_id="34567::hashed",
            pseudonymized_officer_id="BCDEF::hashed",
        )
        add_entity_to_database_session(self.database_key, [existing_row])

        values_to_import = [
            # 2 duplicate rows
            "('US_MI', 'violations', '2022-12-01', '12345', '{\"given_names\": \"fname\"}', 'ABCDE', NULL, NULL, NULL, NULL, NULL, NULL, '12345::hashed', 'ABCDE::hashed')",
            "('US_MI', 'violations', '2022-12-01', '12345', '{\"given_names\": \"fname\"}', 'ABCDE', NULL, NULL, NULL, NULL, NULL, NULL, '12345::hashed', 'ABCDE::hashed')",
            # existing entry
            "('US_MI', 'violations', '2022-12-15', '23456', '{\"given_names\": \"fname\"}', 'BCDEF', NULL, NULL, NULL, NULL, NULL, NULL, '23456::hashed', 'BCDEF::hashed')",
            # new entry
            "('US_MI', 'violations', '2023-01-01', '34567', '{\"given_names\": \"fname\"}', 'BCDEF', NULL, NULL, NULL, NULL, NULL, NULL, '34567::hashed', 'BCDEF::hashed')",
        ]

        def _mock_side_effect(**_kwargs: Any) -> str:
            return self._mock_load_data_from_csv(values=values_to_import)

        self.mock_cloud_sql_client.import_gcs_csv.side_effect = _mock_side_effect
        self.mock_cloud_sql_client.wait_until_operation_completed.return_value = True

        import_gcs_file_to_cloud_sql(
            database_key=self.database_key,
            model=self.model,
            gcs_uri=self.export_uri,
            columns=self.columns,
            region_code="US_MI",
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            destination_table_rows = session.query(self.model).all()
            self.assertEqual(len(destination_table_rows), len(values_to_import))


def build_table(name: str, *args: List[Any]) -> Table:
    base = declarative_base()

    return Table(name, base.metadata, *args)


class TestModelSQL(TestCase):
    """Tests for the ModelSQL class"""

    def setUp(self) -> None:
        self.id_column = Column("id", Integer, primary_key=True)
        self.name_column = Column(
            "name", String(255), unique=True, index=True, comment="The person's name"
        )

    def test_rename(self) -> None:
        """Build a list of queries that can rename a table"""
        test_table = build_table("test_table", self.id_column, self.name_column)
        model_sql = ModelSQL(table=test_table)
        rename_statements = model_sql.build_rename_ddl_queries("new_test_table")

        self.assertEqual(
            rename_statements,
            [
                "ALTER TABLE test_table RENAME TO new_test_table",
                "ALTER INDEX ix_test_table_name RENAME TO ix_new_test_table_name",
            ],
        )

    def test_rename_custom_index_index(self) -> None:
        """Renaming indexes that don't contain the old table name is not implemented"""
        test_table = build_table(
            "test_table",
            self.id_column,
            self.name_column,
            Index("custom_named_index", self.id_column, self.name_column),
        )

        model_sql = ModelSQL(table=test_table)
        with self.assertRaisesRegex(
            NotImplementedError,
            r"Cannot rename index \(custom_named_index\) that does not contain the table's name \(test_table\)",
        ):
            model_sql.build_rename_ddl_queries("new_name")

    def test_ddl_statement_capture(self) -> None:
        """DDL statements required to create the table are captured and put into the `ddl_statements` attribute"""
        test_table = build_table("test_table", self.id_column, self.name_column)
        model_sql = ModelSQL(table=test_table)

        self.assertEqual(
            [statement.__class__ for statement in model_sql.ddl_statements],
            [CreateTable, CreateIndex, SetColumnComment],
        )

    def test_temporary_table_integration(self) -> None:
        """All pathways tables should be able to be successfully built / renamed"""
        for table in get_all_table_classes_in_schema(SchemaType.PATHWAYS):
            model_sql = ModelSQL(table=build_temporary_sqlalchemy_table(table))
            rename_queries = model_sql.build_rename_ddl_queries("new_base_name")
            self.assertGreater(len(rename_queries), 0)

    def test_outliers_temporary_table_integration(self) -> None:
        """All outlier tables should be able to be successfully built / renamed"""
        for table in get_all_table_classes_in_schema(SchemaType.INSIGHTS):
            model_sql = ModelSQL(table=build_temporary_sqlalchemy_table(table))
            rename_queries = model_sql.build_rename_ddl_queries("new_base_name")
            self.assertGreater(len(rename_queries), 0)


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789"))
@pytest.mark.uses_db
class TestGCSImportFileToCloudSQL(TestCase):
    """Tests for gcs_import_to_cloud_sql.py."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def setUp(self) -> None:
        self.officer_1_id = "45678"
        self.mock_instance_id = "mock_instance_id"
        self.cloud_sql_client_patcher = patch(
            "recidiviz.cloud_sql.gcs_import_to_cloud_sql.CloudSQLClientImpl"
        )
        self.mock_cloud_sql_client = MagicMock()
        self.cloud_sql_client_patcher.start().return_value = self.mock_cloud_sql_client

        self.mock_sqlalchemy_engine_manager = SQLAlchemyEngineManager
        setattr(
            self.mock_sqlalchemy_engine_manager,
            "get_stripped_cloudsql_instance_id",
            Mock(return_value=self.mock_instance_id),
        )
        self.database_key = SQLAlchemyDatabaseKey(SchemaType.INSIGHTS, db_name="us_ca")
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)

        self.model = SupervisionOfficer
        self.table_name = self.model.__tablename__
        self.columns = [col.name for col in self.model.__table__.columns]
        self.state_code = StateCode.US_CA.value
        self.now = "2023-08-30T13:33:09.109433"

        self.fs = FakeGCSFileSystem()
        self.fs_patcher = patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

    def tearDown(self) -> None:
        self.fs_patcher.stop()
        self.cloud_sql_client_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @patch(f"{import_gcs_file_to_cloud_sql.__module__}.SessionFactory.using_database")
    def test_import_gcs_file_queries(self, session_mock: MagicMock) -> None:
        """Smoke test of queries issued to a database"""
        execute_mock = session_mock.return_value.__enter__.return_value.execute

        filename = "fake_model.json"
        filepath = GcsfsFilePath.from_absolute_path(
            os.path.join(
                _insights_bucket(),
                self.state_code + "/" + filename,
            )
        )
        self.fs.upload_from_string(
            filepath, json.dumps({"id": 1, "comment": 2}), content_type="text/json"
        )

        import_gcs_file_to_cloud_sql(
            database_key=self.database_key,
            model=FakeModel,
            gcs_uri=filepath,
            columns=["id", "comment"],
        )

        issued_ddl_statements = [
            str(call.args[0]).strip()
            for call in execute_mock.mock_calls
            if len(call.args) and isinstance(call.args[0], (str, DDLElement))
        ]

        self.assertEqual(
            [
                "DROP TABLE IF EXISTS tmp__fake_model",
                "CREATE TABLE tmp__fake_model (\n\tid INTEGER NOT NULL, \n\tcomment TEXT, \n\tPRIMARY KEY (id)\n)",
                "CREATE UNIQUE INDEX tmp__fake_model_pk ON tmp__fake_model (id)",
                "DROP TABLE IF EXISTS fake_model",
                "ALTER TABLE tmp__fake_model RENAME TO fake_model",
                "ALTER INDEX tmp__fake_model_pk RENAME TO fake_model_pk",
            ],
            issued_ddl_statements,
        )

    def test_import_gcs_file_success(self) -> None:
        """Test that a JSON file is correctly imported to the DB"""
        filename = f"{self.table_name}.json"
        fixture_path = os.path.join(
            os.path.dirname(insights_fixtures.__file__), filename
        )

        filename = f"{self.table_name}.json"
        filepath = GcsfsFilePath.from_absolute_path(
            os.path.join(
                _insights_bucket(),
                self.state_code + "/" + filename,
            )
        )
        with open(fixture_path, "r", encoding="UTF-8") as fixture_file:
            for row in fixture_file:
                self.fs.upload_from_string(filepath, row, content_type="text/json")
                break

        import_gcs_file_to_cloud_sql(
            database_key=self.database_key,
            model=self.model,
            gcs_uri=filepath,
            columns=self.columns,
        )

        with SessionFactory.using_database(self.database_key) as session:
            results = session.query(self.model).all()
            self.assertEqual(len(results), 1)
