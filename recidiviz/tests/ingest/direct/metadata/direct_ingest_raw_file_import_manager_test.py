# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Implements tests for DirectIngestRawFileImportManager."""
import datetime
import re
from datetime import timedelta
from typing import Type
from unittest import TestCase
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from sqlalchemy.exc import IntegrityError, NoResultFound

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_file_import import (
    DirectIngestRawFileImportStatusBucket,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
    DirectIngestRawFileImportSummary,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawFileImport,
    DirectIngestRawFileImportStatus,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult


def _fake_eq(e1: Entity, e2: Entity) -> bool:
    def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
        return field_name in ("file_import_id", "bq_file", "import_run")

    return entity_graph_eq(e1, e2, _should_ignore_field_cb)


def _make_unprocessed_raw_data_path(
    path_str: str,
    dt: datetime.datetime = datetime.datetime(
        2015, 1, 2, 3, 3, 3, 3, tzinfo=datetime.UTC
    ),
) -> GcsfsFilePath:
    normalized_path_str = to_normalized_unprocessed_raw_file_path(
        original_file_path=path_str, dt=dt
    )
    return GcsfsFilePath.from_absolute_path(normalized_path_str)


@pytest.mark.uses_db
class DirectIngestRawFileImportManagerTest(TestCase):
    """Implements tests for DirectIngestRawFileImportManager."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.operations_key
        )
        self.us_xx_manager = DirectIngestRawFileImportManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        self.us_raw_file_xx_manager = DirectIngestRawFileMetadataManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.base_entity.Entity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        self.project_id_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_start_import_run_no_such_file_id(self) -> None:
        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        with self.assertRaisesRegex(
            IntegrityError, r"\(psycopg2\.errors\.ForeignKeyViolation\).*"
        ):
            self.us_xx_manager.start_file_import(0, import_run.import_run_id, False)

    def test_start_import_run_no_such_import_dag_run(self) -> None:
        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        with self.assertRaisesRegex(
            IntegrityError, r"\(psycopg2\.errors\.ForeignKeyViolation\).*"
        ):
            self.us_xx_manager.start_file_import(file_metadata.file_id, 0, False)

    def test_start_import_run_simple(self) -> None:

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=import_run.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        started_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id, import_run.import_run_id, False
        )

        self.assertEqual(expected_import, started_import)

    def test_start_import_status(self) -> None:

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=import_run.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        started_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            import_run.import_run_id,
            False,
            import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
        )

        self.assertEqual(started_import, expected_import)

    def test_get_import_by_id(self) -> None:
        with self.assertRaisesRegex(
            NoResultFound, r"No row was found when one was required"
        ):
            _ = self.us_xx_manager.get_import_by_id(0)

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=import_run.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        started_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            import_run.import_run_id,
            False,
        )

        retrieved_import = self.us_xx_manager.get_import_by_id(
            started_import.file_import_id
        )

        assert retrieved_import == expected_import

    def test_build_complex_import_tree(self) -> None:

        frozen_dt = datetime.datetime(2015, 1, 2, 3, 3, 3, 3, tzinfo=datetime.UTC)
        # two files to be imported

        raw_unprocessed_path1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv", dt=frozen_dt
        )
        raw_unprocessed_path2 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv", dt=frozen_dt + datetime.timedelta(days=1)
        )

        file1_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path1
        )
        file2_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path2
        )

        assert file1_metadata.file_id is not None
        assert file2_metadata.file_id is not None

        # first import attempt

        import_run1 = self.us_xx_manager.start_import_run(dag_run_id="abc1")

        started_import1 = self.us_xx_manager.start_file_import(
            file1_metadata.file_id,
            import_run1.import_run_id,
            False,
        )
        started_import2 = self.us_xx_manager.start_file_import(
            file2_metadata.file_id,
            import_run1.import_run_id,
            False,
        )

        self.us_xx_manager.update_file_import_by_id(
            started_import1.file_import_id,
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            raw_rows=0,
        )

        self.us_xx_manager.update_file_import_by_id(
            started_import2.file_import_id,
            import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
        )

        expected_import1 = DirectIngestRawFileImport.new_with_defaults(
            file_id=file1_metadata.file_id,
            import_run_id=import_run1.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            raw_rows=0,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        expected_import2 = DirectIngestRawFileImport.new_with_defaults(
            file_id=file2_metadata.file_id,
            import_run_id=import_run1.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        retrieved_import1 = self.us_xx_manager.get_import_by_id(
            started_import1.file_import_id
        )
        retrieved_import2 = self.us_xx_manager.get_import_by_id(
            started_import2.file_import_id
        )

        assert retrieved_import1 == expected_import1
        assert retrieved_import2 == expected_import2

        # on the second import run, we retry the file we failed last time

        import_run2 = self.us_xx_manager.start_import_run(dag_run_id="abc2")

        started_import2_attempt_2 = self.us_xx_manager.start_file_import(
            file2_metadata.file_id,
            import_run2.import_run_id,
            False,
        )

        self.us_xx_manager.update_file_import_by_id(
            started_import2_attempt_2.file_import_id,
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            raw_rows=10,
        )

        expected_import2_attempt2 = DirectIngestRawFileImport.new_with_defaults(
            file_id=file2_metadata.file_id,
            import_run_id=started_import2_attempt_2.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            raw_rows=10,
        )

        retrieved_import2_attempt_2 = self.us_xx_manager.get_import_by_id(
            started_import2_attempt_2.file_import_id
        )

        assert expected_import2_attempt2 == retrieved_import2_attempt_2

    def test_update_import(self) -> None:

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=import_run.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        started_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            import_run.import_run_id,
            False,
        )

        self.assertEqual(expected_import, started_import)

        self.us_xx_manager.update_file_import_by_id(
            started_import.file_import_id,
            import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
        )

        new_import_run = self.us_xx_manager.get_import_by_id(
            started_import.file_import_id
        )

        assert (
            new_import_run.import_status
            == DirectIngestRawFileImportStatus.FAILED_UNKNOWN
        )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_file_import\" "
            r"violates check constraint \"all_succeeded_imports_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_file_import_by_id(
                started_import.file_import_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            )

        self.us_xx_manager.update_file_import_by_id(
            started_import.file_import_id,
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            raw_rows=0,
        )

        new_new_import_run = self.us_xx_manager.get_import_by_id(
            started_import.file_import_id
        )

        assert (
            new_new_import_run.import_status
            == DirectIngestRawFileImportStatus.SUCCEEDED
        )

        assert new_new_import_run.raw_rows == 0

    def test_error_message(self) -> None:

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=import_run.import_run_id,
            historical_diffs_active=False,
            import_status=DirectIngestRawFileImportStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        started_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            import_run.import_run_id,
            False,
        )

        self.assertEqual(expected_import, started_import)

        self.us_xx_manager.update_file_import_by_id(
            started_import.file_import_id,
            import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
            error_message="FAILURE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
        )

        new_import_run = self.us_xx_manager.get_import_by_id(
            started_import.file_import_id
        )

        assert (
            new_import_run.import_status
            == DirectIngestRawFileImportStatus.FAILED_UNKNOWN
        )

        assert (
            new_import_run.error_message
            == "FAILURE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        )

    def test_update_import_historical_diffs(self) -> None:

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=import_run.import_run_id,
            historical_diffs_active=True,
            import_status=DirectIngestRawFileImportStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        started_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            import_run.import_run_id,
            True,
        )

        self.assertEqual(expected_import, started_import)

        self.us_xx_manager.update_file_import_by_id(
            started_import.file_import_id,
            import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
        )

        new_import_run = self.us_xx_manager.get_import_by_id(
            started_import.file_import_id
        )

        assert (
            new_import_run.import_status
            == DirectIngestRawFileImportStatus.FAILED_UNKNOWN
        )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_file_import\" "
            r"violates check constraint \"all_succeeded_imports_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_file_import_by_id(
                started_import.file_import_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_file_import\" "
            r"violates check constraint \"all_succeeded_imports_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_file_import_by_id(
                started_import.file_import_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                net_new_or_updated_rows=1,
                deleted_rows=1,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_file_import\" "
            r"violates check constraint \"historical_diffs_must_have_non_null_updated_and_deleted\"",
        ):

            self.us_xx_manager.update_file_import_by_id(
                started_import.file_import_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                raw_rows=0,
            )

        self.us_xx_manager.update_file_import_by_id(
            started_import.file_import_id,
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            raw_rows=0,
            net_new_or_updated_rows=0,
            deleted_rows=0,
        )

        new_new_import_run = self.us_xx_manager.get_import_by_id(
            started_import.file_import_id
        )

        assert (
            new_new_import_run.import_status
            == DirectIngestRawFileImportStatus.SUCCEEDED
        )

        assert new_new_import_run.raw_rows == 0
        assert new_new_import_run.net_new_or_updated_rows == 0
        assert new_new_import_run.deleted_rows == 0

    def test_update_import_by_file_id(self) -> None:

        old_import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")
        new_import_run = self.us_xx_manager.start_import_run(dag_run_id="def")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        old_expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=old_import_run.import_run_id,
            historical_diffs_active=True,
            import_status=DirectIngestRawFileImportStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )
        new_expected_import = DirectIngestRawFileImport.new_with_defaults(
            file_id=file_metadata.file_id,
            import_run_id=new_import_run.import_run_id,
            historical_diffs_active=True,
            import_status=DirectIngestRawFileImportStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        old_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            old_import_run.import_run_id,
            True,
        )
        newest_import = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            new_import_run.import_run_id,
            True,
        )

        self.assertEqual(old_expected_import, old_import)
        self.assertEqual(new_expected_import, newest_import)

        self.us_xx_manager.update_most_recent_file_import_for_file_id(
            file_metadata.file_id,
            import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
        )

        older_import = self.us_xx_manager.get_import_by_id(old_import.file_import_id)

        new_import = self.us_xx_manager.get_import_by_id(newest_import.file_import_id)

        assert older_import.import_status == DirectIngestRawFileImportStatus.STARTED

        assert (
            new_import.import_status == DirectIngestRawFileImportStatus.FAILED_UNKNOWN
        )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_file_import\" "
            r"violates check constraint \"all_succeeded_imports_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_most_recent_file_import_for_file_id(
                file_metadata.file_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_file_import\" "
            r"violates check constraint \"all_succeeded_imports_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_most_recent_file_import_for_file_id(
                file_metadata.file_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                net_new_or_updated_rows=1,
                deleted_rows=1,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_file_import\" "
            r"violates check constraint \"historical_diffs_must_have_non_null_updated_and_deleted\"",
        ):

            self.us_xx_manager.update_most_recent_file_import_for_file_id(
                file_metadata.file_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                raw_rows=0,
            )

        self.us_xx_manager.update_most_recent_file_import_for_file_id(
            file_metadata.file_id,
            import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
            raw_rows=0,
            net_new_or_updated_rows=0,
            deleted_rows=0,
        )

        new_new_import_run = self.us_xx_manager.get_import_by_id(
            newest_import.file_import_id
        )

        assert (
            new_new_import_run.import_status
            == DirectIngestRawFileImportStatus.SUCCEEDED
        )

        assert new_new_import_run.raw_rows == 0
        assert new_new_import_run.net_new_or_updated_rows == 0
        assert new_new_import_run.deleted_rows == 0

    def test_get_import_runs_for_file_id_none(self) -> None:
        self.assertEqual(self.us_xx_manager.get_import_for_file_id(4), [])

    def test_get_import_runs_for_file_id(self) -> None:

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        expected_imports = [
            DirectIngestRawFileImport.new_with_defaults(
                file_id=file_metadata.file_id,
                import_run_id=import_run.import_run_id,
                historical_diffs_active=False,
                import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            ),
            DirectIngestRawFileImport.new_with_defaults(
                file_id=file_metadata.file_id,
                import_run_id=import_run.import_run_id,
                historical_diffs_active=False,
                import_status=DirectIngestRawFileImportStatus.STARTED,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            ),
        ]
        _ = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            import_run.import_run_id,
            False,
            import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
        )
        _ = self.us_xx_manager.start_file_import(
            file_metadata.file_id,
            import_run.import_run_id,
            False,
        )

        imports = self.us_xx_manager.get_import_for_file_id(file_metadata.file_id)

        assert imports == expected_imports

    def test_get_n_import_runs_for_file_tag_blank(self) -> None:
        self.assertEqual(
            [], self.us_xx_manager.get_n_most_recent_imports_for_file_tag("abc")
        )

    def test_get_n_import_runs_for_file_tag(self) -> None:
        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        with freeze_time(frozen_time):
            import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag_two.csv")
        metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            unprocessed_path
        )
        assert metadata.file_id is not None
        _ = self.us_xx_manager.start_file_import(
            metadata.file_id,
            import_run.import_run_id,
            False,
        )

        summaries = []
        for i in range(0, 11):
            date_i = frozen_time + timedelta(hours=i)
            unprocessed_path = _make_unprocessed_raw_data_path(
                "bucket/file_tag.csv", dt=date_i
            )
            metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
                unprocessed_path
            )

            assert metadata.file_id is not None
            with freeze_time(date_i):
                import_run_i = self.us_xx_manager.start_import_run(
                    dag_run_id=f"import_run_{i}"
                )

            run = self.us_xx_manager.start_file_import(
                metadata.file_id,
                import_run_i.import_run_id,
                False,
            )

            summary = DirectIngestRawFileImportSummary(
                import_run_id=import_run_i.import_run_id,
                file_id=metadata.file_id or 1,
                dag_run_id=import_run_i.dag_run_id,
                update_datetime=metadata.update_datetime,
                import_run_start=import_run_i.import_run_start,
                import_status=run.import_status,
                historical_diffs_active=run.historical_diffs_active,
                raw_rows=run.raw_rows,
                net_new_or_updated_rows=run.net_new_or_updated_rows,
                deleted_rows=run.deleted_rows,
                is_invalidated=False,
            )

            summaries.append(summary)

        summaries = list(reversed(summaries))

        for i in range(len(summaries)):
            i_runs = self.us_xx_manager.get_n_most_recent_imports_for_file_tag(
                "file_tag", n=i
            )
            assert i_runs == summaries[:i]

    def test_transfer_metadata_to_new_instance_empty(self) -> None:
        us_xx_secondary = DirectIngestRawFileImportManager(
            self.us_xx_manager.region_code, DirectIngestInstance.SECONDARY
        )

        with SessionFactory.using_database(self.operations_key) as session:
            self.us_xx_manager.transfer_metadata_to_new_instance(
                us_xx_secondary, session
            )

    def test_transfer_metadata_to_new_instance_invalid(self) -> None:
        us_xx_secondary = DirectIngestRawFileImportManager(
            self.us_xx_manager.region_code, DirectIngestInstance.SECONDARY
        )

        file_metadata_manager = DirectIngestRawFileMetadataManager(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )
        raw_unprocessed_path_1 = _make_unprocessed_raw_data_path(
            "bucket/file_tag.csv",
            dt=datetime.datetime.now(tz=datetime.UTC),
        )

        file = file_metadata_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path_1
        )

        assert file.file_id is not None

        import_run = self.us_xx_manager.start_import_run(dag_run_id="abc")

        self.us_xx_manager.start_file_import(
            file.file_id, import_run.import_run_id, False
        )

        with self.assertRaisesRegex(
            ValueError,
            r"Destination instance should not have any file_ids that reference valid "
            r"raw big query file metadata rows. Found \[2\].",
        ):
            with SessionFactory.using_database(self.operations_key) as session:
                us_xx_secondary.transfer_metadata_to_new_instance(
                    self.us_xx_manager, session
                )

    def test_transfer_metadata_to_new_instance_valid(self) -> None:

        fixed_datetime = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        us_xx_secondary = DirectIngestRawFileImportManager(
            self.us_xx_manager.region_code, DirectIngestInstance.SECONDARY
        )

        file_metadata_manager = DirectIngestRawFileMetadataManager(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )
        file_metadata_manager_secondary = DirectIngestRawFileMetadataManager(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.SECONDARY,
        )

        for i in range(0, 5):

            import_run = self.us_xx_manager.start_import_run(dag_run_id=f"abc_{i}")
            secondary_import_run = us_xx_secondary.start_import_run(
                dag_run_id=f"abc_{i}"
            )

            file_tag_1_path = _make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_1.csv",
                dt=fixed_datetime + timedelta(hours=i),
            )

            file_tag_2_path = _make_unprocessed_raw_data_path(
                path_str="bucket/file_tag_2.csv",
                dt=fixed_datetime + timedelta(hours=i),
            )

            file_1_primary = file_metadata_manager.mark_raw_gcs_file_as_discovered(
                file_tag_1_path
            )
            file_2_primary = file_metadata_manager.mark_raw_gcs_file_as_discovered(
                file_tag_2_path
            )
            file_1_secondary = (
                file_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
                    file_tag_1_path
                )
            )
            file_2_secondary = (
                file_metadata_manager_secondary.mark_raw_gcs_file_as_discovered(
                    file_tag_2_path
                )
            )

            assert file_1_primary.file_id is not None
            assert file_2_primary.file_id is not None
            assert file_1_secondary.file_id is not None
            assert file_2_secondary.file_id is not None

            self.us_xx_manager.start_file_import(
                file_1_primary.file_id, import_run.import_run_id, False
            )
            self.us_xx_manager.start_file_import(
                file_2_primary.file_id, import_run.import_run_id, False
            )
            us_xx_secondary.start_file_import(
                file_1_secondary.file_id, secondary_import_run.import_run_id, False
            )
            us_xx_secondary.start_file_import(
                file_2_secondary.file_id, secondary_import_run.import_run_id, False
            )

        # files exist in primary but are all invalidated
        file_metadata_manager.mark_instance_data_invalidated()

        self.assertEqual(
            5,
            len(us_xx_secondary.get_n_most_recent_imports_for_file_tag("file_tag_1")),
        )

        self.assertEqual(
            5,
            len(
                self.us_xx_manager.get_n_most_recent_imports_for_file_tag("file_tag_1")
            ),
        )

        with SessionFactory.using_database(self.operations_key) as session:
            us_xx_secondary.transfer_metadata_to_new_instance(
                self.us_xx_manager, session
            )

        self.assertEqual(
            [],
            us_xx_secondary.get_n_most_recent_imports_for_file_tag("file_tag_1"),
        )

        migrated = self.us_xx_manager.get_n_most_recent_imports_for_file_tag(
            "file_tag_1"
        )

        invalidated = [m for m in migrated if m.is_invalidated]
        valid = [m for m in migrated if m.is_invalidated]

        assert len(invalidated) == 5
        assert len(valid) == 5

    def test_import_run_summary_empty(self) -> None:
        assert self.us_xx_manager.get_most_recent_import_run_summary().for_api() == {
            "importRunStart": None,
            "countByStatusBucket": [],
        }

    def test_import_run_summary(self) -> None:
        fixed_datetime = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        file_tag_1_path = _make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_1.csv",
            dt=fixed_datetime,
        )

        file_tag_2_path = _make_unprocessed_raw_data_path(
            path_str="bucket/file_tag_2.csv",
            dt=fixed_datetime,
        )

        file_1 = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            file_tag_1_path
        )
        file_2 = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            file_tag_2_path
        )

        assert file_1.file_id is not None
        assert file_2.file_id is not None

        with freeze_time(fixed_datetime + timedelta(hours=1)):
            import_run = self.us_xx_manager.start_import_run("test-run-1")
            file_import_1 = self.us_xx_manager.start_file_import(
                file_1.file_id, import_run.import_run_id, False
            )
            file_import_2 = self.us_xx_manager.start_file_import(
                file_2.file_id, import_run.import_run_id, False
            )

            self.us_xx_manager.update_file_import_by_id(
                file_import_1.file_import_id,
                import_status=DirectIngestRawFileImportStatus.FAILED_LOAD_STEP,
            )
            self.us_xx_manager.update_file_import_by_id(
                file_import_2.file_import_id,
                import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
            )

            summary = self.us_xx_manager.get_most_recent_import_run_summary()

            assert summary is not None
            assert summary.import_run_start == fixed_datetime + timedelta(hours=1)
            assert summary.count_by_status_bucket == {
                DirectIngestRawFileImportStatusBucket.FAILED: 2
            }

        with freeze_time(fixed_datetime + timedelta(hours=2)):
            import_run = self.us_xx_manager.start_import_run("test-run-2")
            file_import_1 = self.us_xx_manager.start_file_import(
                file_1.file_id, import_run.import_run_id, False
            )
            file_import_2 = self.us_xx_manager.start_file_import(
                file_2.file_id, import_run.import_run_id, False
            )

            self.us_xx_manager.update_file_import_by_id(
                file_import_1.file_import_id,
                import_status=DirectIngestRawFileImportStatus.SUCCEEDED,
                raw_rows=1,
            )
            self.us_xx_manager.update_file_import_by_id(
                file_import_2.file_import_id,
                import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
            )

            summary = self.us_xx_manager.get_most_recent_import_run_summary()

            assert summary is not None
            assert summary.import_run_start == fixed_datetime + timedelta(hours=2)
            assert summary.count_by_status_bucket == {
                DirectIngestRawFileImportStatusBucket.FAILED: 1,
                DirectIngestRawFileImportStatusBucket.SUCCEEDED: 1,
            }
        with freeze_time(fixed_datetime + timedelta(hours=2)):
            us_yy_manager = DirectIngestRawFileImportManager(
                region_code="US_YY", raw_data_instance=DirectIngestInstance.PRIMARY
            )
            import_run = us_yy_manager.start_import_run("test-run-2")
            file_import_1 = us_yy_manager.start_file_import(
                file_1.file_id, import_run.import_run_id, False
            )
            file_import_2 = us_yy_manager.start_file_import(
                file_2.file_id, import_run.import_run_id, False
            )

            us_yy_manager.update_file_import_by_id(
                file_import_1.file_import_id,
                import_status=DirectIngestRawFileImportStatus.STARTED,
                raw_rows=1,
            )
            us_yy_manager.update_file_import_by_id(
                file_import_2.file_import_id,
                import_status=DirectIngestRawFileImportStatus.FAILED_UNKNOWN,
            )

            yy_summary = us_yy_manager.get_most_recent_import_run_summary()

            assert yy_summary is not None
            assert yy_summary.import_run_start == fixed_datetime + timedelta(hours=2)
            assert yy_summary.count_by_status_bucket == {
                DirectIngestRawFileImportStatusBucket.IN_PROGRESS: 1,
                DirectIngestRawFileImportStatusBucket.FAILED: 1,
            }

            summary = self.us_xx_manager.get_most_recent_import_run_summary()

            assert summary is not None
            assert summary.import_run_start == fixed_datetime + timedelta(hours=2)
            assert summary.count_by_status_bucket == {
                DirectIngestRawFileImportStatusBucket.FAILED: 1,
                DirectIngestRawFileImportStatusBucket.SUCCEEDED: 1,
            }


class DirectIngestRawFileImportStatusBucketsTest(TestCase):
    def test_all_statuses_covered(self) -> None:
        for status in DirectIngestRawFileImportStatus:
            DirectIngestRawFileImportStatusBucket.from_import_status(status)

    def test_missing_status_fails(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Unrecognized import status: eeek; please add it to the list of values in recidiviz.common.constants.operations.direct_ingest_raw_file_import"
            ),
        ):
            DirectIngestRawFileImportStatusBucket.from_import_status("eeek")  # type: ignore
