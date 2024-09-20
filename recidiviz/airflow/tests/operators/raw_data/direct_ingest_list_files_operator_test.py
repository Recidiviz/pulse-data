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
"""Tests for DirectIngestListNormalizedFileOperator and subclasses"""
from unittest import TestCase
from unittest.mock import MagicMock, create_autospec, patch

from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.raw_data import direct_ingest_list_files_operator
from recidiviz.airflow.dags.operators.raw_data.direct_ingest_list_files_operator import (
    DirectIngestListNormalizedFileOperator,
    DirectIngestListNormalizedProcessedFilesOperator,
    DirectIngestListNormalizedUnprocessedFilesOperator,
)

OPERATOR_FILE = direct_ingest_list_files_operator.__name__


class DirectIngestListNormalizedFileOperatorTest(TestCase):
    """Tests for DirectIngestListNormalizedFileOperator and subclasses"""

    def setUp(self) -> None:
        self.gcs_operator_patch = patch(
            f"{OPERATOR_FILE}.GCSListObjectsOperator.execute",
        )
        self.gcs_operator_mock = self.gcs_operator_patch.start()
        self.ingest_operator = DirectIngestListNormalizedFileOperator(
            task_id="test-list", bucket="testing"
        )

    def tearDown(self) -> None:
        self.gcs_operator_patch.stop()

    def test_no_paths(self) -> None:
        self.gcs_operator_mock.execute.return_value = []
        mock_context = create_autospec(Context)
        assert self.ingest_operator.execute(mock_context) == []

    def test_invalid_paths(self) -> None:
        self.gcs_operator_mock.return_value = ["unprocessed_abc"]
        mock_context = create_autospec(Context)
        assert self.ingest_operator.execute(mock_context) == []

    def test_valid_paths(self) -> None:
        valid_paths = [
            "unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
            "processed_2024-01-25T16:35:33:617135_raw_test_file_tag_with_suffix-1.csv",
            "subdirs_get_processed_too/unprocessed_2024-01-25T16:35:33:617135_raw_test_file_tag.csv",
        ]
        invalid_paths = [
            "unprocessed_2024-01-25T16:35:33:617135_invalid_test_file_tag.csv",
            "unprocessed_2024-01-25T16_35_33_617135_raw_test_file_tag.csv",
            "unprocessed_2024-01-25T16:35:33:617135_raw_invalid_test_file_tag_with_suffix-1-1-1-1.csv",
        ]
        self.gcs_operator_mock.return_value = [*valid_paths, *invalid_paths]
        mock_context = create_autospec(Context)
        valid_out_paths = [f"testing/{p}" for p in valid_paths]
        assert self.ingest_operator.execute(mock_context) == valid_out_paths

    @patch(f"{OPERATOR_FILE}.DirectIngestListNormalizedFileOperator.__init__")
    def test_unprocessed_prefix(self, super_class_mock: MagicMock) -> None:
        args = {"task_id": "test-list", "bucket": "testing", "default_args": {}}
        _ = DirectIngestListNormalizedUnprocessedFilesOperator(**args)
        super_class_mock.assert_called_once_with(**args, prefix="unprocessed")

    @patch(f"{OPERATOR_FILE}.DirectIngestListNormalizedFileOperator.__init__")
    def test_processed_prefix(self, super_class_mock: MagicMock) -> None:
        args = {"task_id": "test-list", "bucket": "testing", "default_args": {}}
        _ = DirectIngestListNormalizedProcessedFilesOperator(**args)
        super_class_mock.assert_called_once_with(**args, prefix="processed")
