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
"""Implements tests for DirectIngestRawDataImportSessionManager."""
import datetime
from datetime import timedelta
from typing import Optional, Type
from unittest import TestCase
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from sqlalchemy.exc import IntegrityError, NoResultFound

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_import_session_manager import (
    DirectIngestRawDataImportSessionManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager_v2 import (
    DirectIngestRawFileMetadataManagerV2,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawDataImportSession,
    DirectIngestRawDataImportSessionStatus,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


def _fake_eq(e1: Entity, e2: Entity) -> bool:
    def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
        return field_name in ("import_session_id", "bq_file")

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
class DirectIngestRawDataImportSessionManagerTest(TestCase):
    """Implements tests for DirectIngestRawDataResourceLockManager."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(self.operations_key)
        self.us_xx_manager = DirectIngestRawDataImportSessionManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        self.us_raw_file_xx_manager = DirectIngestRawFileMetadataManagerV2(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.base_entity.Entity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_start_import_session_no_such_file_id(self) -> None:
        with self.assertRaisesRegex(
            IntegrityError, r"\(psycopg2\.errors\.ForeignKeyViolation\).*"
        ):
            self.us_xx_manager.start_import_session(0, False)

    def test_start_import_session_simple(self) -> None:

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        expected_import_session = DirectIngestRawDataImportSession.new_with_defaults(
            file_id=file_metadata.file_id,
            import_start=frozen_time,
            historical_diffs_active=False,
            import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        with freeze_time(frozen_time):
            import_session = self.us_xx_manager.start_import_session(
                file_metadata.file_id, False
            )

        self.assertEqual(expected_import_session, import_session)

    def test_start_import_session_status(self) -> None:

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        expected_import_session = DirectIngestRawDataImportSession.new_with_defaults(
            file_id=file_metadata.file_id,
            import_start=frozen_time,
            historical_diffs_active=False,
            import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        with freeze_time(frozen_time):
            import_session = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                False,
                import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
            )

        self.assertEqual(expected_import_session, import_session)

    def test_get_import_session_by_id(self) -> None:
        with self.assertRaisesRegex(
            NoResultFound, r"No row was found when one was required"
        ):
            _ = self.us_xx_manager.get_import_session_by_id(0)

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        expected_import_session = DirectIngestRawDataImportSession.new_with_defaults(
            file_id=file_metadata.file_id,
            import_start=frozen_time,
            historical_diffs_active=False,
            import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        with freeze_time(frozen_time):
            started_import_session = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                False,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            )

        retrieved_import_session = self.us_xx_manager.get_import_session_by_id(
            started_import_session.import_session_id
        )

        assert retrieved_import_session == expected_import_session

    def test_update_import_session(self) -> None:

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        expected_import_session = DirectIngestRawDataImportSession.new_with_defaults(
            file_id=file_metadata.file_id,
            import_start=frozen_time,
            historical_diffs_active=False,
            import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        with freeze_time(frozen_time):
            import_session = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                False,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            )

        self.assertEqual(expected_import_session, import_session)

        self.us_xx_manager.update_import_session_by_id(
            import_session.import_session_id,
            import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
        )

        new_import_session = self.us_xx_manager.get_import_session_by_id(
            import_session.import_session_id
        )

        assert (
            new_import_session.import_status
            == DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN
        )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_data_import_session\" "
            r"violates check constraint \"all_succeeded_import_sessions_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_import_session_by_id(
                import_session.import_session_id,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
            )

        self.us_xx_manager.update_import_session_by_id(
            import_session.import_session_id,
            import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
            raw_rows=0,
        )

        new_new_import_session = self.us_xx_manager.get_import_session_by_id(
            import_session.import_session_id
        )

        assert (
            new_new_import_session.import_status
            == DirectIngestRawDataImportSessionStatus.SUCCEEDED
        )

        assert new_new_import_session.raw_rows == 0

    def test_update_import_session_historical_diffs(self) -> None:

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        expected_import_session = DirectIngestRawDataImportSession.new_with_defaults(
            file_id=file_metadata.file_id,
            import_start=frozen_time,
            historical_diffs_active=True,
            import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            region_code=StateCode.US_XX.value,
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        with freeze_time(frozen_time):
            import_session = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                True,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            )

        self.assertEqual(expected_import_session, import_session)

        self.us_xx_manager.update_import_session_by_id(
            import_session.import_session_id,
            import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
        )

        new_import_session = self.us_xx_manager.get_import_session_by_id(
            import_session.import_session_id
        )

        assert (
            new_import_session.import_status
            == DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN
        )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_data_import_session\" "
            r"violates check constraint \"all_succeeded_import_sessions_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_import_session_by_id(
                import_session.import_session_id,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_data_import_session\" "
            r"violates check constraint \"all_succeeded_import_sessions_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_import_session_by_id(
                import_session.import_session_id,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
                net_new_or_updated_rows=1,
                deleted_rows=1,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_data_import_session\" "
            r"violates check constraint \"historical_diffs_must_have_non_null_updated_and_deleted\"",
        ):

            self.us_xx_manager.update_import_session_by_id(
                import_session.import_session_id,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
                raw_rows=0,
            )

        self.us_xx_manager.update_import_session_by_id(
            import_session.import_session_id,
            import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
            raw_rows=0,
            net_new_or_updated_rows=0,
            deleted_rows=0,
        )

        new_new_import_session = self.us_xx_manager.get_import_session_by_id(
            import_session.import_session_id
        )

        assert (
            new_new_import_session.import_status
            == DirectIngestRawDataImportSessionStatus.SUCCEEDED
        )

        assert new_new_import_session.raw_rows == 0
        assert new_new_import_session.net_new_or_updated_rows == 0
        assert new_new_import_session.deleted_rows == 0

    def test_update_import_session_by_file_id(self) -> None:

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        old_expected_import_session = (
            DirectIngestRawDataImportSession.new_with_defaults(
                file_id=file_metadata.file_id,
                import_start=frozen_time,
                historical_diffs_active=True,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
        )
        new_expected_import_session = (
            DirectIngestRawDataImportSession.new_with_defaults(
                file_id=file_metadata.file_id,
                import_start=frozen_time + timedelta(hours=1),
                historical_diffs_active=True,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
        )

        with freeze_time(frozen_time):
            old_import_session = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                True,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            )
        with freeze_time(frozen_time + timedelta(hours=1)):
            newest_import_session = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                True,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            )

        self.assertEqual(old_expected_import_session, old_import_session)
        self.assertEqual(new_expected_import_session, newest_import_session)

        self.us_xx_manager.update_most_recent_import_session_for_file_id(
            file_metadata.file_id,
            import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
        )

        older_import_session = self.us_xx_manager.get_import_session_by_id(
            old_import_session.import_session_id
        )

        new_import_session = self.us_xx_manager.get_import_session_by_id(
            newest_import_session.import_session_id
        )

        assert (
            older_import_session.import_status
            == DirectIngestRawDataImportSessionStatus.STARTED
        )

        assert (
            new_import_session.import_status
            == DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN
        )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_data_import_session\" "
            r"violates check constraint \"all_succeeded_import_sessions_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_most_recent_import_session_for_file_id(
                file_metadata.file_id,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_data_import_session\" "
            r"violates check constraint \"all_succeeded_import_sessions_must_have_non_null_rows\"",
        ):
            self.us_xx_manager.update_most_recent_import_session_for_file_id(
                file_metadata.file_id,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
                net_new_or_updated_rows=1,
                deleted_rows=1,
            )

        with self.assertRaisesRegex(
            IntegrityError,
            r"\(psycopg2\.errors\.CheckViolation\) new row for relation \"direct_ingest_raw_data_import_session\" "
            r"violates check constraint \"historical_diffs_must_have_non_null_updated_and_deleted\"",
        ):

            self.us_xx_manager.update_most_recent_import_session_for_file_id(
                file_metadata.file_id,
                import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
                raw_rows=0,
            )

        self.us_xx_manager.update_most_recent_import_session_for_file_id(
            file_metadata.file_id,
            import_status=DirectIngestRawDataImportSessionStatus.SUCCEEDED,
            raw_rows=0,
            net_new_or_updated_rows=0,
            deleted_rows=0,
        )

        new_new_import_session = self.us_xx_manager.get_import_session_by_id(
            newest_import_session.import_session_id
        )

        assert (
            new_new_import_session.import_status
            == DirectIngestRawDataImportSessionStatus.SUCCEEDED
        )

        assert new_new_import_session.raw_rows == 0
        assert new_new_import_session.net_new_or_updated_rows == 0
        assert new_new_import_session.deleted_rows == 0

    def test_get_import_sesions_for_file_id_none(self) -> None:
        self.assertEqual(self.us_xx_manager.get_import_sesions_for_file_id(4), [])

    def test_get_import_sesions_for_file_id(self) -> None:

        raw_unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag.csv")

        file_metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            raw_unprocessed_path
        )

        assert file_metadata.file_id is not None

        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)

        expected_import_sessions = [
            DirectIngestRawDataImportSession.new_with_defaults(
                file_id=file_metadata.file_id,
                import_start=frozen_time,
                historical_diffs_active=False,
                import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            ),
            DirectIngestRawDataImportSession.new_with_defaults(
                file_id=file_metadata.file_id,
                import_start=frozen_time,
                historical_diffs_active=False,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
                region_code=StateCode.US_XX.value,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            ),
        ]
        with freeze_time(frozen_time):
            _ = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                False,
                import_status=DirectIngestRawDataImportSessionStatus.FAILED_UNKNOWN,
            )
            _ = self.us_xx_manager.start_import_session(
                file_metadata.file_id,
                False,
                import_status=DirectIngestRawDataImportSessionStatus.STARTED,
            )

        import_sessions = self.us_xx_manager.get_import_sesions_for_file_id(
            file_metadata.file_id
        )

        assert import_sessions == expected_import_sessions

    def test_get_n_import_sessions_for_file_tag_blank(self) -> None:
        self.assertEqual(
            [], self.us_xx_manager.get_n_most_recent_import_sessions_for_file_tag("abc")
        )

    def test_get_n_import_sessions_for_file_tag(self) -> None:
        unprocessed_path = _make_unprocessed_raw_data_path("bucket/file_tag_two.csv")
        metadata = self.us_raw_file_xx_manager.mark_raw_gcs_file_as_discovered(
            unprocessed_path
        )
        assert metadata.file_id is not None
        _ = self.us_xx_manager.start_import_session(
            metadata.file_id,
            False,
            import_status=DirectIngestRawDataImportSessionStatus.STARTED,
        )

        sessions = []
        frozen_time = datetime.datetime(2015, 1, 2, 3, 4, 6, tzinfo=datetime.UTC)
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
                session = self.us_xx_manager.start_import_session(
                    metadata.file_id,
                    False,
                    import_status=DirectIngestRawDataImportSessionStatus.STARTED,
                )

                sessions.append(session)

        sessions = list(reversed(sessions))

        for i in range(len(sessions)):
            i_sessions = (
                self.us_xx_manager.get_n_most_recent_import_sessions_for_file_tag(
                    "file_tag", n=i
                )
            )
            assert i_sessions == sessions[:i]

    def test_transfer_metadata_to_new_instance_empty(self) -> None:
        us_xx_secondary = DirectIngestRawDataImportSessionManager(
            self.us_xx_manager.region_code, DirectIngestInstance.SECONDARY
        )

        with SessionFactory.using_database(self.operations_key) as session:
            self.us_xx_manager.transfer_metadata_to_new_instance(
                us_xx_secondary, session
            )

    def test_transfer_metadata_to_new_instance_invalid(self) -> None:
        us_xx_secondary = DirectIngestRawDataImportSessionManager(
            self.us_xx_manager.region_code, DirectIngestInstance.SECONDARY
        )

        file_metadata_manager = DirectIngestRawFileMetadataManagerV2(
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

        self.us_xx_manager.start_import_session(file.file_id, False)

        with self.assertRaisesRegex(
            ValueError,
            r"Destination instance should not have any file_ids that reference valid "
            r"raw big query file metadata rows. Found \[1\].",
        ):
            with SessionFactory.using_database(self.operations_key) as session:
                us_xx_secondary.transfer_metadata_to_new_instance(
                    self.us_xx_manager, session
                )

    def test_transfer_metadata_to_new_instance_valid(self) -> None:

        fixed_datetime = datetime.datetime(2022, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)

        us_xx_secondary = DirectIngestRawDataImportSessionManager(
            self.us_xx_manager.region_code, DirectIngestInstance.SECONDARY
        )

        file_metadata_manager = DirectIngestRawFileMetadataManagerV2(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )
        file_metadata_manager_secondary = DirectIngestRawFileMetadataManagerV2(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.SECONDARY,
        )

        for i in range(0, 5):
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

            self.us_xx_manager.start_import_session(file_1_primary.file_id, False)
            self.us_xx_manager.start_import_session(file_2_primary.file_id, False)
            us_xx_secondary.start_import_session(file_1_secondary.file_id, False)
            us_xx_secondary.start_import_session(file_2_secondary.file_id, False)

        # files exist in primary but are all invalidated
        file_metadata_manager.mark_instance_data_invalidated()

        self.assertEqual(
            5,
            len(
                us_xx_secondary.get_n_most_recent_import_sessions_for_file_tag(
                    "file_tag_1"
                )
            ),
        )

        self.assertEqual(
            5,
            len(
                self.us_xx_manager.get_n_most_recent_import_sessions_for_file_tag(
                    "file_tag_1"
                )
            ),
        )

        with SessionFactory.using_database(self.operations_key) as session:
            us_xx_secondary.transfer_metadata_to_new_instance(
                self.us_xx_manager, session
            )

        self.assertEqual(
            [],
            us_xx_secondary.get_n_most_recent_import_sessions_for_file_tag(
                "file_tag_1"
            ),
        )

        migrated = self.us_xx_manager.get_n_most_recent_import_sessions_for_file_tag(
            "file_tag_1"
        )

        invalidated = [m for m in migrated if m.bq_file and m.bq_file.is_invalidated]
        valid = [m for m in migrated if m.bq_file and m.bq_file.is_invalidated]

        assert len(invalidated) == 5
        assert len(valid) == 5
