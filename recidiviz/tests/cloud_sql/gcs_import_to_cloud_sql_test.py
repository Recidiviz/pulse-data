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
from typing import Any, List, Optional
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import Column, Index, Integer, String, Table
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import CreateIndex, CreateTable, SetColumnComment

from recidiviz.cloud_sql.gcs_import_to_cloud_sql import (
    ModelSQL,
    import_gcs_csv_to_cloud_sql,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
    ETLClient,
    ETLOpportunity,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.auth.helpers import (
    add_users_to_database_session,
    generate_fake_user_restrictions,
)
from recidiviz.tools.postgres import local_postgres_helpers


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
        self.user_1_email = "user-1@test.gov"
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
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        local_postgres_helpers.use_on_disk_postgresql_database(self.database_key)

        self.table_name = DashboardUserRestrictions.__tablename__
        self.model = DashboardUserRestrictions
        self.columns = [col.name for col in DashboardUserRestrictions.__table__.columns]
        self.dashboard_user_restrictions_uri = GcsfsFilePath.from_absolute_path(
            "US_MO/dashboard_user_restrictions.csv"
        )

    def tearDown(self) -> None:
        self.cloud_sql_client_patcher.stop()
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    def _mock_load_data_from_csv(
        self, values: Optional[List[str]] = None, **_kwargs: Any
    ) -> str:
        with SessionFactory.using_database(self.database_key) as session:
            csv_values = [
                f"('US_MO', '{self.user_1_email}', '{{1}}', 'level_1_supervision_location', 'level_1_access_role', true, false, false, 'null')",
            ]
            if values:
                csv_values = csv_values + values
            session.execute(
                f"INSERT INTO tmp__{self.table_name} ({','.join(self.columns)}) "
                f"VALUES {','.join(csv_values)}"
            )
            return "fake-id"

    def test_import_gcs_csv_to_cloud_sql_swaps_tables(self) -> None:
        """Assert that the temp table and destination are successfully swapped."""
        self.mock_cloud_sql_client.import_gcs_csv.side_effect = (
            self._mock_load_data_from_csv
        )
        self.mock_cloud_sql_client.wait_until_operation_completed.return_value = True

        import_gcs_csv_to_cloud_sql(
            schema_type=SchemaType.CASE_TRIAGE,
            model=self.model,
            gcs_uri=self.dashboard_user_restrictions_uri,
            columns=self.columns,
        )
        self.mock_cloud_sql_client.import_gcs_csv.assert_called_with(
            instance_name=self.mock_instance_id,
            table_name=f"tmp__{self.table_name}",
            gcs_uri=self.dashboard_user_restrictions_uri,
            columns=self.columns,
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            destination_table_rows = session.query(DashboardUserRestrictions).all()

        self.assertEqual(len(destination_table_rows), 1)
        self.assertEqual(
            destination_table_rows[0].restricted_user_email, self.user_1_email
        )

    def test_import_gcs_csv_to_cloud_sql_with_region_code(self) -> None:
        """Assert that rows are copied to the temp table for every region code before being swapped to
        the destination table."""
        user_1 = generate_fake_user_restrictions(
            "US_PA",
            "user-4@test.gov",
            allowed_supervision_location_ids="1,2",
        )
        user_2 = generate_fake_user_restrictions(
            "US_PA",
            "user-5@test.gov",
            allowed_supervision_location_ids="AB",
        )
        add_users_to_database_session(self.database_key, [user_1, user_2])

        def _mock_side_effect(**_kwargs: Any) -> str:
            return self._mock_load_data_from_csv(
                values=[
                    "('US_MO', 'user-2@test.gov', '{2}', 'level_1_supervision_location', 'level_1_access_role', true, false, false, 'null')",
                    "('US_MO', 'user-3@test.gov', '{3}', 'level_1_supervision_location', 'level_1_access_role', true, false, false, 'null')",
                ]
            )

        self.mock_cloud_sql_client.import_gcs_csv.side_effect = _mock_side_effect
        self.mock_cloud_sql_client.wait_until_operation_completed.return_value = True

        import_gcs_csv_to_cloud_sql(
            schema_type=SchemaType.CASE_TRIAGE,
            model=self.model,
            gcs_uri=self.dashboard_user_restrictions_uri,
            columns=self.columns,
            region_code="US_MO",
        )
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            destination_table_rows = session.query(DashboardUserRestrictions).all()
            state_codes = [row.state_code for row in destination_table_rows]
            self.assertEqual(len(destination_table_rows), 5)
            self.assertEqual(set(state_codes), {"US_MO", "US_PA"})

    def test_import_gcs_csv_to_cloud_sql_client_error(self) -> None:
        """Assert that CloudSQLClient errors raise an error and roll back the session."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            user_1 = generate_fake_user_restrictions(
                "US_PA",
                "user-3@test.gov",
                allowed_supervision_location_ids="1,2",
            )
            add_users_to_database_session(self.database_key, [user_1])
            self.mock_cloud_sql_client.import_gcs_csv.side_effect = Exception(
                "Error while importing CSV to temp table"
            )
            with self.assertRaisesRegex(
                Exception, "^Error while importing CSV to temp table$"
            ):
                import_gcs_csv_to_cloud_sql(
                    schema_type=SchemaType.CASE_TRIAGE,
                    model=self.model,
                    gcs_uri=self.dashboard_user_restrictions_uri,
                    columns=self.columns,
                )
            destination_table_rows = session.query(DashboardUserRestrictions).all()
            self.assertEqual(len(destination_table_rows), 1)

    def test_import_gcs_csv_to_cloud_sql_session_error(self) -> None:
        """Assert that session errors raise an error and roll back the session."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            user_1 = generate_fake_user_restrictions(
                "US_PA",
                "user-3@test.gov",
                allowed_supervision_location_ids="1,2",
            )
            add_users_to_database_session(self.database_key, [user_1])

            def _mock_side_effect(**_kwargs: Any) -> str:
                raise ValueError("An error occurred!")

            self.mock_cloud_sql_client.import_gcs_csv.side_effect = _mock_side_effect

            with self.assertRaisesRegex(Exception, "An error occurred!"):
                import_gcs_csv_to_cloud_sql(
                    schema_type=SchemaType.CASE_TRIAGE,
                    model=DashboardUserRestrictions,
                    gcs_uri=self.dashboard_user_restrictions_uri,
                    columns=self.columns,
                )

            destination_table_rows = session.query(DashboardUserRestrictions).all()
            self.assertEqual(len(destination_table_rows), 1)

    def test_additional_queries_etl_clients(self) -> None:
        """Assert that no constraints are applied to `etl_clients` (#8579)."""
        self.mock_cloud_sql_client.import_gcs_csv.return_value = []
        self.mock_cloud_sql_client.wait_until_operation_completed.return_value = True

        import_gcs_csv_to_cloud_sql(
            schema_type=SchemaType.CASE_TRIAGE,
            model=ETLClient,
            gcs_uri=GcsfsFilePath.from_absolute_path("US_ID/etl_clients.csv"),
            columns=[],
            region_code="US_ID",
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            constraint_count = len(
                session.execute(
                    build_list_constraints_query("public", "etl_clients")
                ).fetchall()
            )

            unique_index_count = len(
                session.execute(
                    build_list_unique_indexes_query("public", "etl_clients")
                ).fetchall()
            )

            self.assertEqual(constraint_count, 0, "Expected no constraints")
            self.assertEqual(unique_index_count, 0, "Expected no unique indexes")

    def test_additional_queries_etl_opportunities(self) -> None:
        """Assert that no constraints are applied to `etl_opportunities` (#9292)."""
        self.mock_cloud_sql_client.import_gcs_csv.return_value = []
        self.mock_cloud_sql_client.wait_until_operation_completed.return_value = True

        import_gcs_csv_to_cloud_sql(
            schema_type=SchemaType.CASE_TRIAGE,
            model=ETLOpportunity,
            gcs_uri=GcsfsFilePath.from_absolute_path("US_ID/etl_opportunities.csv"),
            columns=[],
            region_code="US_ID",
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            constraint_count = len(
                session.execute(
                    build_list_constraints_query("public", "etl_opportunities")
                ).fetchall()
            )

            unique_index_count = len(
                session.execute(
                    build_list_unique_indexes_query("public", "etl_opportunities")
                ).fetchall()
            )

            self.assertEqual(constraint_count, 0, "Expected no constraints")
            self.assertEqual(unique_index_count, 0, "Expected no unique indexes")


class TestModelSQL(TestCase):
    """Tests for the ModelSQL class"""

    def setUp(self) -> None:
        self.id_column = Column("id", Integer, primary_key=True)
        self.name_column = Column(
            "name", String(255), unique=True, index=True, comment="The person's name"
        )

    def build_table(self, name: str, *args: List[Any]) -> Table:
        base = declarative_base()

        return Table(name, base.metadata, *args)

    def test_rename(self) -> None:
        """Build a list of queries that can rename a table"""
        test_table = self.build_table("test_table", self.id_column, self.name_column)
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
        """Renaming indexes that don't contain the old table name is not implemeneted"""
        test_table = self.build_table(
            "test_table",
            self.id_column,
            self.name_column,
            Index("custom_named_index", self.id_column, self.name_column),
        )

        model_sql = ModelSQL(table=test_table)
        with self.assertRaisesRegex(
            NotImplementedError,
            r"Cannot rename indexes that do not contain the table's name \(test_table\)",
        ):
            model_sql.build_rename_ddl_queries("new_name")

    def test_ddl_statement_capture(self) -> None:
        """DDL statements required to create the table are captured and put into the `ddl_statements` attribute"""
        test_table = self.build_table("test_table", self.id_column, self.name_column)
        model_sql = ModelSQL(table=test_table)

        self.assertEqual(
            [statement.__class__ for statement in model_sql.ddl_statements],
            [CreateTable, CreateIndex, SetColumnComment],
        )
