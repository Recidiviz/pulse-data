# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for OperateOnRawStorageDirectoriesController."""
import unittest
from unittest import mock

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.tools.ingest.operations.helpers.operate_on_raw_storage_directories_controller import (
    IngestFilesOperationType,
    OperateOnRawStorageDirectoriesController,
)
from recidiviz.utils.future_executor import ThreadPoolExecutorResult

_SUBDIR = "gs://test-bucket/us_xx/raw/2026/09/19"


class OperateOnRawStorageDirectoriesControllerTest(unittest.TestCase):
    """Tests for OperateOnRawStorageDirectoriesController."""

    def _build_copy_controller(self) -> OperateOnRawStorageDirectoriesController:
        return OperateOnRawStorageDirectoriesController.create_controller(
            region_code="us_xx",
            operation_type=IngestFilesOperationType.COPY,
            source_region_storage_dir_path=GcsfsDirectoryPath.from_absolute_path(
                "gs://test-bucket/us_xx/"
            ),
            destination_region_storage_dir_path=GcsfsDirectoryPath.from_absolute_path(
                "gs://other-bucket/us_xx/"
            ),
            dry_run=False,
            skip_confirmation=True,
        )

    @mock.patch(
        "recidiviz.tools.ingest.operations.helpers."
        "operate_on_raw_storage_directories_controller.map_fn_with_progress_bar_results"
    )
    def test_execute_file_operations_raises_when_any_operation_fails(
        self, mock_map_fn: mock.MagicMock
    ) -> None:
        """A failed copy must surface as an exception rather than being dropped while
        the controller reports success."""
        # pylint: disable=protected-access
        failed_operation = ("gs://test-bucket/us_xx/raw/2026/09/19/*", "gs://other")
        mock_map_fn.return_value = ThreadPoolExecutorResult(
            successes=[],
            exceptions=[(failed_operation, RuntimeError("copy blew up"))],
        )
        controller = self._build_copy_controller()

        with self.assertRaisesRegex(
            ExceptionGroup,
            r"^Failed to copy files for \[1\] of \[1\] operations \(1 sub-exception\)$",
        ):
            controller._execute_file_operations([_SUBDIR])

    @mock.patch(
        "recidiviz.tools.ingest.operations.helpers."
        "operate_on_raw_storage_directories_controller.map_fn_with_progress_bar_results"
    )
    def test_execute_file_operations_records_successes_when_none_fail(
        self, mock_map_fn: mock.MagicMock
    ) -> None:
        # pylint: disable=protected-access
        succeeded_operation = ("gs://test-bucket/us_xx/raw/2026/09/19/*", "gs://other")
        mock_map_fn.return_value = ThreadPoolExecutorResult(
            successes=[(succeeded_operation, None)],
            exceptions=[],
        )
        controller = self._build_copy_controller()

        controller._execute_file_operations([_SUBDIR])

        self.assertEqual([succeeded_operation], controller.operations_list)
