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
"""Tests for the invalidate_operations_db_files_controller module."""
import datetime
import unittest
from collections import defaultdict
from unittest.mock import patch

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager_test import (
    make_unprocessed_raw_data_path,
)
from recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller import (
    InvalidateOperationsDBFilesController,
    ProcessingStatusFilterType,
    RawFilesGroupedByTagAndId,
)
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.utils.types import assert_type


class TestRawFilesGroupedByTagAndId(unittest.TestCase):
    """Tests for the RawFilesGroupedByTagAndId class"""

    def setUp(self) -> None:
        self.tuple_list = [
            ("tag1", 1, "file1.csv", 11),
            ("tag1", 1, "file2.csv", 12),
            ("tag1", 2, "file3.csv", 13),
            ("tag2", 3, "file4.csv", 14),
            ("tag2", None, "file5.csv", 15),
        ]

    def test_empty(self) -> None:
        grouped_files = RawFilesGroupedByTagAndId(
            file_tag_to_file_id_dict=defaultdict(lambda: defaultdict(list)),
            gcs_file_id_to_file_name={},
        )
        self.assertTrue(grouped_files.empty())
        non_empty_grouped_files = (
            RawFilesGroupedByTagAndId.from_file_tag_id_name_tuples(self.tuple_list)
        )
        self.assertFalse(non_empty_grouped_files.empty())

    def test_gcs_file_ids_only(self) -> None:
        """Tests RawFilesGroupedByTagAndId for files that only have gcs_file_ids, not
        BQ file_ids.
        """
        grouped_files = RawFilesGroupedByTagAndId.from_file_tag_id_name_tuples(
            [
                (
                    "tag1",
                    None,
                    "unprocessed_2020-01-01T00:00:00:000000_raw_tag1.csv",
                    11,
                ),
                (
                    "tag1",
                    None,
                    "unprocessed_2020-01-02T00:00:00:000000_raw_tag1.csv",
                    12,
                ),
            ]
        )
        self.assertFalse(grouped_files.empty())
        self.assertEqual(set(), grouped_files.file_ids)
        self.assertEqual(
            {"tag1": {None: [11, 12]}}, grouped_files.file_tag_to_file_id_dict
        )
        self.assertEqual({11, 12}, grouped_files.gcs_file_ids)

    def test_get_file_ids(self) -> None:
        grouped_files = RawFilesGroupedByTagAndId.from_file_tag_id_name_tuples(
            self.tuple_list
        )
        self.assertSetEqual({1, 2, 3}, grouped_files.file_ids)

    def test_get_normalized_file_names(self) -> None:
        grouped_files = RawFilesGroupedByTagAndId.from_file_tag_id_name_tuples(
            self.tuple_list
        )
        self.assertSetEqual(
            {"file1.csv", "file2.csv", "file3.csv", "file4.csv", "file5.csv"},
            grouped_files.normalized_file_names,
        )

    def test_get_gcs_file_ids(self) -> None:
        grouped_files = RawFilesGroupedByTagAndId.from_file_tag_id_name_tuples(
            self.tuple_list
        )
        self.assertSetEqual({11, 12, 13, 14, 15}, grouped_files.gcs_file_ids)


@pytest.mark.uses_db
class TestInvalidateOperationsDBFilesController(unittest.TestCase):
    """Tests for the InvalidateOperationsDBFilesController class"""

    temp_db_dir: str | None

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(self.database_key)
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-testing"
        )
        self.project_id_patcher.start()

        self.raw_metadata_manager = DirectIngestRawFileMetadataManager(
            region_code="us_xx",
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    def test_run_no_files_to_invalidate(self) -> None:
        with patch(
            "recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller.logging.info"
        ) as mock_logging:
            controller = InvalidateOperationsDBFilesController.create_controller(
                project_id="test-project",
                state_code=StateCode.US_XX,
                ingest_instance=DirectIngestInstance.PRIMARY,
                file_tag_filters=["tag1"],
                file_tag_regex=None,
                start_date_bound="2024-11-01",
                end_date_bound="2024-11-10",
                dry_run=True,
                skip_prompts=True,
                with_proxy=False,
            )
            controller.run()
            mock_logging.assert_called_with("No files to invalidate.")

    def test_run_execute_invalidation_date_bounds(self) -> None:
        processed_tag_1_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 2, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_1_path
                ).file_id,
                int,
            )
        )
        processed_tag_1_outside_date_bounds_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 12, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_1_outside_date_bounds_path
                ).file_id,
                int,
            )
        )

        processed_tag_2_path = make_unprocessed_raw_data_path(
            "bucket/tag2", dt=datetime.datetime(2024, 11, 4, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_2_path
                ).file_id,
                int,
            )
        )

        controller = InvalidateOperationsDBFilesController.create_controller(
            project_id="test-project",
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            file_tag_filters=["tag1"],
            file_tag_regex=None,
            start_date_bound="2024-11-01",
            end_date_bound="2024-11-10",
            dry_run=False,
            skip_prompts=True,
            with_proxy=False,
        )
        # these are currently valid, will be invalidated
        processed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_1_path
        )
        assert not processed_tag_1.is_invalidated
        # these are currently valid, will be filtered out by invalidator
        processed_tag_2 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_2_path
        )
        assert not processed_tag_2.is_invalidated
        processed_tag_1_outside_date_bounds = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                processed_tag_1_outside_date_bounds_path
            )
        )
        assert not processed_tag_1_outside_date_bounds.is_invalidated

        # run the invalidation
        controller.run()

        # these are currently invalid, were valid before
        processed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_1_path
        )
        assert processed_tag_1.is_invalidated
        # these are currently valid, were filtered out by invalidator
        processed_tag_2 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_2_path
        )
        assert not processed_tag_2.is_invalidated
        processed_tag_1_outside_date_bounds = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                processed_tag_1_outside_date_bounds_path
            )
        )
        assert not processed_tag_1_outside_date_bounds.is_invalidated

    def test_run_execute_invalidation_date_bounds_processed_filter(self) -> None:
        unprocessed_tag_1_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 2, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            unprocessed_tag_1_path
        )
        processed_tag_1_outside_date_bounds_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 12, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_1_outside_date_bounds_path
                ).file_id,
                int,
            )
        )

        processed_tag_2_path = make_unprocessed_raw_data_path(
            "bucket/tag2", dt=datetime.datetime(2024, 11, 4, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_2_path
                ).file_id,
                int,
            )
        )

        controller = InvalidateOperationsDBFilesController.create_controller(
            project_id="test-project",
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            file_tag_filters=["tag1"],
            file_tag_regex=None,
            start_date_bound="2024-11-01",
            end_date_bound="2024-11-10",
            dry_run=False,
            skip_prompts=True,
            processing_status_filter=ProcessingStatusFilterType.UNPROCESSED_ONLY,
            with_proxy=False,
        )
        # these are currently valid, will be invalidated
        unprocessed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            unprocessed_tag_1_path
        )
        assert not unprocessed_tag_1.is_invalidated
        # these are currently valid, will be filtered out by invalidator
        processed_tag_2 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_2_path
        )
        assert not processed_tag_2.is_invalidated
        processed_tag_1_outside_date_bounds = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                processed_tag_1_outside_date_bounds_path
            )
        )
        assert not processed_tag_1_outside_date_bounds.is_invalidated

        # run the invalidation
        controller.run()

        # these are currently invalid, were valid before
        unprocessed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            unprocessed_tag_1_path
        )
        assert unprocessed_tag_1.is_invalidated
        # these are currently valid, were filtered out by invalidator
        processed_tag_2 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_2_path
        )
        assert not processed_tag_2.is_invalidated
        processed_tag_1_outside_date_bounds = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                processed_tag_1_outside_date_bounds_path
            )
        )
        assert not processed_tag_1_outside_date_bounds.is_invalidated

    def test_run_execute_invalidation_date_bounds_processed_filter_true(self) -> None:
        unprocessed_tag_1_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 2, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            unprocessed_tag_1_path
        )
        processed_tag_1_outside_date_bounds_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 12, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_1_outside_date_bounds_path
                ).file_id,
                int,
            )
        )

        processed_tag_1_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 4, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_1_path
                ).file_id,
                int,
            )
        )

        controller = InvalidateOperationsDBFilesController.create_controller(
            project_id="test-project",
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            file_tag_filters=["tag1"],
            file_tag_regex=None,
            start_date_bound="2024-11-01",
            end_date_bound="2024-11-10",
            dry_run=False,
            skip_prompts=True,
            processing_status_filter=ProcessingStatusFilterType.PROCESSED_ONLY,
            with_proxy=False,
        )
        # these are currently valid, will be invalidated
        processed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_1_path
        )
        assert not processed_tag_1.is_invalidated
        # these are currently valid, will be filtered out by invalidator
        unprocessed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            unprocessed_tag_1_path
        )
        assert not unprocessed_tag_1.is_invalidated
        processed_tag_1_outside_date_bounds = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                processed_tag_1_outside_date_bounds_path
            )
        )
        assert not processed_tag_1_outside_date_bounds.is_invalidated

        # run the invalidation
        controller.run()

        # these are currently invalid, were valid before
        processed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_1_path
        )
        assert processed_tag_1.is_invalidated
        # these are currently valid, were filtered out by invalidator
        unprocessed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            unprocessed_tag_1_path
        )
        assert not unprocessed_tag_1.is_invalidated
        processed_tag_1_outside_date_bounds = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                processed_tag_1_outside_date_bounds_path
            )
        )
        assert not processed_tag_1_outside_date_bounds.is_invalidated

    def test_run_execute_invalidation_gcs_file_ids_only(self) -> None:

        not_grouped_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 4, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            not_grouped_path, is_chunked_file=True
        )
        not_grouped_excluded_path = make_unprocessed_raw_data_path(
            "bucket/tag2", dt=datetime.datetime(2024, 11, 22, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            not_grouped_excluded_path, is_chunked_file=True
        )

        controller = InvalidateOperationsDBFilesController.create_controller(
            project_id="test-project",
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            file_tag_filters=["tag1"],
            file_tag_regex=None,
            start_date_bound="2024-11-01",
            end_date_bound="2024-11-10",
            dry_run=False,
            skip_prompts=True,
            with_proxy=False,
        )
        # these are currently valid, will be invalidated
        not_grouped = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_path
        )
        assert not not_grouped.is_invalidated
        # these are currently valid, will be filtered out by invalidator
        not_grouped_excluded = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_excluded_path
        )
        assert not not_grouped_excluded.is_invalidated

        # run the invalidation
        controller.run()

        # these are currently invalid, were valid before
        not_grouped = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_path
        )
        assert not_grouped.is_invalidated
        # these are currently valid, were filtered out by invalidator
        not_grouped_excluded = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_excluded_path
        )
        assert not not_grouped_excluded.is_invalidated

    def test_run_execute_invalidation_gcs_file_ids_only_processed_filter(self) -> None:

        not_grouped_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 4, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            not_grouped_path, is_chunked_file=True
        )
        not_grouped_excluded_by_date_path = make_unprocessed_raw_data_path(
            "bucket/tag2", dt=datetime.datetime(2024, 11, 22, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
            not_grouped_excluded_by_date_path, is_chunked_file=True
        )

        processed_true_controller = (
            InvalidateOperationsDBFilesController.create_controller(
                project_id="test-project",
                state_code=StateCode.US_XX,
                ingest_instance=DirectIngestInstance.PRIMARY,
                file_tag_filters=["tag1"],
                file_tag_regex=None,
                start_date_bound="2024-11-01",
                end_date_bound="2024-11-10",
                dry_run=False,
                skip_prompts=True,
                processing_status_filter=ProcessingStatusFilterType.PROCESSED_ONLY,
                with_proxy=False,
            )
        )

        # these are currently valid, will be filtered out by invalidator
        not_grouped = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_path
        )
        assert not not_grouped.is_invalidated
        not_grouped_excluded_by_date = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                not_grouped_excluded_by_date_path
            )
        )
        assert not not_grouped_excluded_by_date.is_invalidated

        # run!
        processed_true_controller.run()

        # these are currently valid, were filtered out by invalidator
        not_grouped = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_path
        )
        assert not not_grouped.is_invalidated
        not_grouped_excluded_by_date = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                not_grouped_excluded_by_date_path
            )
        )
        assert not not_grouped_excluded_by_date.is_invalidated

        processed_false_controller = (
            InvalidateOperationsDBFilesController.create_controller(
                project_id="test-project",
                state_code=StateCode.US_XX,
                ingest_instance=DirectIngestInstance.PRIMARY,
                file_tag_filters=["tag1"],
                file_tag_regex=None,
                start_date_bound="2024-11-01",
                end_date_bound="2024-11-10",
                dry_run=False,
                skip_prompts=True,
                processing_status_filter=ProcessingStatusFilterType.UNPROCESSED_ONLY,
                with_proxy=False,
            )
        )

        # these are currently valid, will be invalidated
        not_grouped = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_path
        )
        assert not not_grouped.is_invalidated
        # these are currently valid, will be filtered out by invalidator
        not_grouped_excluded_by_date = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                not_grouped_excluded_by_date_path
            )
        )
        assert not not_grouped_excluded_by_date.is_invalidated

        # run the invalidation
        processed_false_controller.run()

        # these are currently invalid, were valid before
        not_grouped = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            not_grouped_path
        )
        assert not_grouped.is_invalidated
        # these are currently valid, were filtered out by invalidator
        not_grouped_excluded_by_date = (
            self.raw_metadata_manager.get_raw_gcs_file_metadata(
                not_grouped_excluded_by_date_path
            )
        )
        assert not not_grouped_excluded_by_date.is_invalidated

    def test_run_execute_invalidation_filename_filter(self) -> None:
        processed_tag_1_path = make_unprocessed_raw_data_path(
            "bucket/tag1", dt=datetime.datetime(2024, 11, 2, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_1_path
                ).file_id,
                int,
            )
        )
        processed_tag_2_path = make_unprocessed_raw_data_path(
            "bucket/tag2", dt=datetime.datetime(2024, 11, 9, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_2_path
                ).file_id,
                int,
            )
        )

        processed_tag_3_path = make_unprocessed_raw_data_path(
            "bucket/tag3", dt=datetime.datetime(2024, 11, 4, 3, 3, 3, 3)
        )
        self.raw_metadata_manager.mark_raw_big_query_file_as_processed(
            assert_type(
                self.raw_metadata_manager.mark_raw_gcs_file_as_discovered(
                    processed_tag_3_path
                ).file_id,
                int,
            )
        )

        controller = InvalidateOperationsDBFilesController.create_controller(
            project_id="test-project",
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            file_tag_filters=None,
            file_tag_regex="tag1|tag2",
            start_date_bound="2024-11-01",
            end_date_bound="2024-11-10",
            dry_run=False,
            skip_prompts=True,
            with_proxy=False,
        )
        # these are currently valid, will be invalidated
        processed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_1_path
        )
        assert not processed_tag_1.is_invalidated
        processed_tag_2 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_2_path
        )
        assert not processed_tag_2.is_invalidated
        # these are currently valid, will be filtered out by invalidator
        processed_tag_3 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_3_path
        )
        assert not processed_tag_3.is_invalidated

        # run the invalidation
        controller.run()

        # these are currently invalid, were valid before
        processed_tag_1 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_1_path
        )
        assert processed_tag_1.is_invalidated
        processed_tag_2 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_2_path
        )
        assert processed_tag_2.is_invalidated
        # these are currently valid, were filtered out by invalidator
        processed_tag_3 = self.raw_metadata_manager.get_raw_gcs_file_metadata(
            processed_tag_3_path
        )
        assert not processed_tag_3.is_invalidated
