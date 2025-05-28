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
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller import (
    InvalidateOperationsDBFilesController,
    RawFilesGroupedByTagAndId,
)


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


class TestInvalidateOperationsDBFilesController(unittest.TestCase):
    """Tests for the InvalidateOperationsDBFilesController class"""

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id", return_value="recidiviz-testing"
        )
        self.project_id_patcher.start()
        self.controller = InvalidateOperationsDBFilesController.create_controller(
            project_id="test-project",
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            file_tag_filters=["tag1"],
            file_tag_regex=None,
            start_date_bound="2024-11-01",
            end_date_bound="2024-11-10",
            dry_run=True,
            skip_prompts=True,
        )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    @patch(
        "recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller.SessionFactory.for_proxy"
    )
    def test_run_no_files_to_invalidate(self, mock_session_factory: MagicMock) -> None:
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = []
        mock_session_factory.return_value.__enter__.return_value = mock_session

        with patch(
            "recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller.logging.info"
        ) as mock_logging:
            self.controller.run()
            mock_logging.assert_called_with("No files to invalidate.")

    @patch(
        "recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller.SessionFactory.for_proxy"
    )
    def test_run_execute_invalidation(self, mock_session_factory: MagicMock) -> None:
        file_data = [
            ("tag1", 1, "file1.csv", 6),
            ("tag1", None, "file3.csv", 7),
            ("tag2", 2, "file2.csv", 8),
        ]
        mock_session = MagicMock()
        mock_session.execute.side_effect = [
            MagicMock(fetchall=lambda: file_data),
            None,
            None,
        ]
        mock_session_factory.return_value.__enter__.return_value = mock_session
        self.controller.dry_run = False

        self.controller.run()

        expected_bq_query = """
UPDATE direct_ingest_raw_big_query_file_metadata
SET is_invalidated = True
WHERE file_id in (1, 2)
"""
        expected_gcs_query = """
UPDATE direct_ingest_raw_gcs_file_metadata
SET is_invalidated = True
WHERE gcs_file_id in (8, 6, 7)
"""

        # Assertions to check if both queries are in the captured execute calls
        actual_queries = [
            call[0][0].text for call in mock_session.execute.call_args_list
        ]
        assert any(
            expected_bq_query in query for query in actual_queries
        ), "BQ query not found in execute calls."
        assert any(
            expected_gcs_query in query for query in actual_queries
        ), "GCS query not found in execute calls."

    @patch(
        "recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller.SessionFactory.for_proxy"
    )
    def test_run_execute_invalidation_gcs_file_ids_only(
        self, mock_session_factory: MagicMock
    ) -> None:
        file_data = [
            ("tag1", None, "file1.csv", 6),
            ("tag1", None, "file3.csv", 7),
            ("tag2", None, "file2.csv", 8),
        ]
        mock_session = MagicMock()
        mock_session.execute.side_effect = [
            MagicMock(fetchall=lambda: file_data),
            None,
            None,
        ]
        mock_session_factory.return_value.__enter__.return_value = mock_session
        self.controller.dry_run = False

        self.controller.run()

        expected_select_query = """
SELECT file_tag, file_id, normalized_file_name, gcs_file_id
FROM direct_ingest_raw_gcs_file_metadata
WHERE region_code = 'US_XX' 
    AND raw_data_instance = 'PRIMARY' 
    AND is_invalidated IS NOT True
    AND file_tag IN ('tag1')
    AND DATE(update_datetime) >= '2024-11-01' AND DATE(update_datetime) <= '2024-11-10'
"""
        expected_gcs_query = """
UPDATE direct_ingest_raw_gcs_file_metadata
SET is_invalidated = True
WHERE gcs_file_id in (8, 6, 7)
"""

        # Assertions to check if both queries are in the captured execute calls
        actual_queries = [
            call[0][0].text for call in mock_session.execute.call_args_list
        ]
        assert len(actual_queries) == 2

        # We query and run the deprecation query for direct_ingest_raw_gcs_file_metadata
        # but not direct_ingest_raw_big_query_file_metadata
        assert [expected_select_query, expected_gcs_query] == sorted(actual_queries)

    @patch(
        "recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller.SessionFactory.for_proxy"
    )
    def test_run_execute_invalidation_filename_filter(
        self, mock_session_factory: MagicMock
    ) -> None:
        file_data = [
            ("tag1", 1, "file1.csv", 6),
        ]
        mock_session = MagicMock()
        mock_session.execute.side_effect = [
            MagicMock(fetchall=lambda: file_data),
            None,
            None,
        ]
        mock_session_factory.return_value.__enter__.return_value = mock_session
        controller = InvalidateOperationsDBFilesController.create_controller(
            project_id="test-project",
            state_code=StateCode.US_XX,
            ingest_instance=DirectIngestInstance.PRIMARY,
            normalized_filenames_filter=["file1.csv"],
            dry_run=False,
            skip_prompts=True,
        )

        controller.run()

        expected_file_query = """
SELECT file_tag, file_id, normalized_file_name, gcs_file_id
FROM direct_ingest_raw_gcs_file_metadata
WHERE region_code = 'US_XX' 
    AND raw_data_instance = 'PRIMARY' 
    AND is_invalidated IS NOT True
    AND normalized_file_name IN ('file1.csv')
"""

        expected_bq_query = """
UPDATE direct_ingest_raw_big_query_file_metadata
SET is_invalidated = True
WHERE file_id in (1)
"""
        expected_gcs_query = """
UPDATE direct_ingest_raw_gcs_file_metadata
SET is_invalidated = True
WHERE gcs_file_id in (6)
"""

        # Assertions to check if both queries are in the captured execute calls
        actual_queries = [
            call[0][0].text for call in mock_session.execute.call_args_list
        ]
        assert any(
            expected_file_query in query for query in actual_queries
        ), "File query not found in execute calls."
        assert any(
            expected_bq_query in query for query in actual_queries
        ), "BQ query not found in execute calls."
        assert any(
            expected_gcs_query in query for query in actual_queries
        ), "GCS query not found in execute calls."
