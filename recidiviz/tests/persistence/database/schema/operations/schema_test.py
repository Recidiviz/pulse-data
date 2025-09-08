# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for the schema defined in operations/schema.py."""
import datetime
import re
import unittest

import pytest
import pytz
from sqlalchemy.exc import IntegrityError

from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


@pytest.mark.uses_db
class OperationsSchemaTest(unittest.TestCase):
    """Tests for the schema defined in operations/schema.py."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_gcs_file_metadata_file_id_fk(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            new_gcs_tag_1 = schema.DirectIngestRawGCSFileMetadata(
                file_id=1,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                file_tag="tag_1",
                normalized_file_name="test.txt",
                update_datetime=datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
                file_discovery_time=datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
                is_invalidated=False,
            )
            session.add(new_gcs_tag_1)

            with self.assertRaisesRegex(
                IntegrityError, r"\(psycopg2\.errors\.ForeignKeyViolation\).*"
            ):
                session.commit()

    def test_one_non_invalidated_normalized_file_name_per_region_instance(
        self,
    ) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            kwargs = {
                "region_code": StateCode.US_XX.value,
                "raw_data_instance": DirectIngestInstance.PRIMARY.value,
                "file_tag": "tag_1",
                "normalized_file_name": "test.txt",
                "update_datetime": datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
                "file_discovery_time": datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
            }
            new_gcs_tag_valid_1 = schema.DirectIngestRawGCSFileMetadata(
                **kwargs, is_invalidated=False
            )
            session.add(new_gcs_tag_valid_1)
            session.commit()

            new_gcs_tag_valid_2 = schema.DirectIngestRawGCSFileMetadata(
                **kwargs, is_invalidated=False
            )
            session.add(new_gcs_tag_valid_2)

            with self.assertRaisesRegex(
                IntegrityError,
                re.escape(
                    '(psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "one_non_invalidated_normalized_file_name_per_region_instance"'
                ),
            ):
                session.commit()
            session.rollback()

            new_gcs_tag_invalid = schema.DirectIngestRawGCSFileMetadata(
                **kwargs, is_invalidated=True
            )
            session.add(new_gcs_tag_invalid)
            session.commit()  # allowed

    def test_all_succeeded_imports_must_have_non_null_rows(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            bq_file = schema.DirectIngestRawBigQueryFileMetadata(
                file_id=1,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                file_tag="tag_1",
                update_datetime=datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
                is_invalidated=False,
            )
            session.add(bq_file)

            import_run = schema.DirectIngestRawFileImportRun(
                import_run_id=1,
                dag_run_id="123",
                import_run_start=datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
            )

            session.add(import_run)

            session.commit()

            fields = {
                "file_id": 1,
                "import_run_id": 1,
                "historical_diffs_active": False,
                "import_status": DirectIngestRawFileImportStatus.SUCCEEDED.value,
                "region_code": StateCode.US_XX.value,
                "raw_data_instance": DirectIngestInstance.PRIMARY.value,
            }
            file_import_with_raw_rows = schema.DirectIngestRawFileImport(
                **fields, raw_rows=0
            )

            session.add(file_import_with_raw_rows)
            session.commit()

            file_import_with_no_raw_rows = schema.DirectIngestRawFileImport(
                **fields, raw_rows=None
            )
            session.add(file_import_with_no_raw_rows)

            with self.assertRaisesRegex(
                IntegrityError,
                re.escape(
                    '(psycopg2.errors.CheckViolation) new row for relation "direct_ingest_raw_file_import" violates check constraint "all_succeeded_imports_must_have_non_null_rows'
                ),
            ):
                session.commit()

    def test_historical_diffs_must_have_non_null_updated_and_deleted(self) -> None:
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            bq_file = schema.DirectIngestRawBigQueryFileMetadata(
                file_id=1,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
                file_tag="tag_1",
                update_datetime=datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
                is_invalidated=False,
            )
            session.add(bq_file)

            import_run = schema.DirectIngestRawFileImportRun(
                import_run_id=1,
                dag_run_id="123",
                import_run_start=datetime.datetime(2011, 11, 11, tzinfo=pytz.UTC),
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY.value,
            )

            session.add(import_run)

            session.commit()

            fields = {
                "file_id": 1,
                "import_run_id": 1,
                "historical_diffs_active": True,
                "import_status": DirectIngestRawFileImportStatus.SUCCEEDED.value,
                "region_code": StateCode.US_XX.value,
                "raw_data_instance": DirectIngestInstance.PRIMARY.value,
            }
            file_import_with_raw_rows = schema.DirectIngestRawFileImport(
                **fields, raw_rows=0, net_new_or_updated_rows=0, deleted_rows=0
            )

            session.add(file_import_with_raw_rows)
            session.commit()

            file_import_with_no_deleted = schema.DirectIngestRawFileImport(
                **fields, raw_rows=10, net_new_or_updated_rows=10, deleted_rows=None
            )
            session.add(file_import_with_no_deleted)

            with self.assertRaisesRegex(
                IntegrityError,
                re.escape(
                    '(psycopg2.errors.CheckViolation) new row for relation "direct_ingest_raw_file_import" violates check constraint "historical_diffs_must_have_non_null_updated_and_deleted'
                ),
            ):
                session.commit()
            session.rollback()

            file_import_with_no_net_new_or_updated = schema.DirectIngestRawFileImport(
                **fields, raw_rows=10, net_new_or_updated_rows=None, deleted_rows=10
            )
            session.add(file_import_with_no_net_new_or_updated)

            with self.assertRaisesRegex(
                IntegrityError,
                re.escape(
                    '(psycopg2.errors.CheckViolation) new row for relation "direct_ingest_raw_file_import" violates check constraint "historical_diffs_must_have_non_null_updated_and_deleted'
                ),
            ):
                session.commit()
