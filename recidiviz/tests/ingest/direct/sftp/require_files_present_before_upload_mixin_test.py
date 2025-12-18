# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the require files present before upload mixin."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.sftp.require_files_present_before_upload_mixin import (
    RequireFilesBeforeUploadMixin,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestRequireFilesBeforeUploadMixin(unittest.TestCase):
    """Unit tests for RequireFilesBeforeUploadMixin"""

    def setUp(self) -> None:
        self.region_raw_file_config = get_region_raw_file_config(
            region_code=StateCode.US_XX.value, region_module=fake_regions
        )
        self.referenced_patcher = patch(
            "recidiviz.ingest.direct.sftp.require_files_present_before_upload_mixin.get_all_referenced_file_tags",
            return_value={
                "tagPipeSeparatedNonUTF8",
                "tagPipeSeparatedWindows",
                "tagRowExtraColumns",
            },
        )
        self.referenced_mock = self.referenced_patcher.start()

        self.chunking_metadata_patcher = patch(
            "recidiviz.ingest.direct.sftp.require_files_present_before_upload_mixin.StateRawFileChunkingMetadataFactory.build"
        )
        self.chunking_metadata_mock = self.chunking_metadata_patcher.start()
        mock_chunking_metadata = unittest.mock.MagicMock()
        self.chunking_metadata_mock.return_value = mock_chunking_metadata

        # By default, expect 1 file per tag
        def default_chunk_count(_file_tag: str) -> int:
            return 1

        mock_chunking_metadata.get_current_expected_file_count.side_effect = (
            default_chunk_count
        )

    def tearDown(self) -> None:
        self.referenced_patcher.stop()
        self.chunking_metadata_patcher.stop()

    def test_verify_daily_files_present_all_present(self) -> None:
        ingest_ready_normalized_file_paths = [
            "2025-02-14T16:29:30:000000/tagPipeSeparatedNonUTF8.csv",
            "2025-02-14T16:29:30:000000/tagPipeSeparatedWindows.csv",
            # We should group files with the same date together
            "2025-02-14T12:29:30:000000/tagRowExtraColumns.csv",
            # If there are multiple files for the same tag on the same date,
            # we can't really be sure if they are sending us a whole new
            # batch or just re-sending a file, so we should allow upload
            # if there is at least one full set for that date
            "2025-02-14T18:29:30:000000/tagRowExtraColumns.csv",
            # We should allow files with different update cadences
            "2025-02-14T16:29:30:000000/tagOneAllNullRow.csv",
            # We should allow files that have no raw file config
            "2025-02-14T16:29:30:000000/tagNoConfig.csv",
            # We should allow subsets from older dates
            "2025-02-13T16:29:30:000000/tagPipeSeparatedNonUTF8.csv",
        ]

        result = RequireFilesBeforeUploadMixin.are_all_referenced_daily_files_present(
            region_raw_file_config=self.region_raw_file_config,
            ingest_ready_normalized_file_paths=ingest_ready_normalized_file_paths,
        )
        self.assertTrue(result)

    def test_verify_daily_files_present_missing_file(self) -> None:
        ingest_ready_normalized_file_paths = [
            "2025-02-14T16:29:30:000000/tagPipeSeparatedNonUTF8.csv",
            # Missing tagPipeSeparatedWindows.csv
            "2025-02-14T12:29:30:000000/tagRowExtraColumns.csv",
            # Should fail even if we have a full set from an older date
            "2025-02-13T16:29:30:000000/tagPipeSeparatedNonUTF8.csv",
            "2025-02-13T16:29:30:000000/tagPipeSeparatedWindows.csv",
            "2025-02-13T12:29:30:000000/tagRowExtraColumns.csv",
        ]

        result = RequireFilesBeforeUploadMixin.are_all_referenced_daily_files_present(
            region_raw_file_config=self.region_raw_file_config,
            ingest_ready_normalized_file_paths=ingest_ready_normalized_file_paths,
        )
        self.assertFalse(result)

    def test_verify_daily_files_present_no_daily_files_configured(self) -> None:
        region_raw_file_config = get_region_raw_file_config(
            region_code=StateCode.US_LL.value, region_module=fake_regions
        )

        ingest_ready_normalized_file_paths = [
            "2025-02-14T16:29:30:000000/tagPipeSeparatedNonUTF8.csv",
            "2025-02-14T16:29:30:000000/tagPipeSeparatedWindows.csv",
        ]

        with self.assertRaisesRegex(
            ValueError, "No `update_cadence: DAILY` file tags configured"
        ):
            RequireFilesBeforeUploadMixin.are_all_referenced_daily_files_present(
                region_raw_file_config=region_raw_file_config,
                ingest_ready_normalized_file_paths=ingest_ready_normalized_file_paths,
            )

    def test_verify_daily_files_present_empty_ingest_ready_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "ingest_ready_normalized_file_paths cannot be empty"
        ):
            RequireFilesBeforeUploadMixin.are_all_referenced_daily_files_present(
                region_raw_file_config=self.region_raw_file_config,
                ingest_ready_normalized_file_paths=[],
            )

    def test_verify_daily_files_present_no_referenced_daily_files(self) -> None:
        self.referenced_mock.return_value = set()

        ingest_ready_normalized_file_paths = [
            "2025-02-14T16:29:30:000000/tagPipeSeparatedNonUTF8.csv",
            "2025-02-14T16:29:30:000000/tagPipeSeparatedWindows.csv",
            "2025-02-14T12:29:30:000000/tagRowExtraColumns.csv",
        ]

        result = RequireFilesBeforeUploadMixin.are_all_referenced_daily_files_present(
            region_raw_file_config=self.region_raw_file_config,
            ingest_ready_normalized_file_paths=ingest_ready_normalized_file_paths,
        )
        self.assertTrue(result)

    @patch(
        "recidiviz.ingest.direct.sftp.require_files_present_before_upload_mixin.StateRawFileChunkingMetadataFactory.build"
    )
    def test_verify_daily_files_present_chunked_file_missing_chunks(
        self, mock_chunking_factory: MagicMock
    ) -> None:
        mock_chunking_metadata = MagicMock()
        mock_chunking_factory.return_value = mock_chunking_metadata
        mock_chunking_metadata.get_current_expected_file_count.side_effect = (
            lambda file_tag: (3 if file_tag == "tagPipeSeparatedWindows" else 1)
        )

        ingest_ready_normalized_file_paths = [
            "2025-02-14T16:29:30:000000/tagPipeSeparatedNonUTF8.csv",
            # Only 1 chunk present but 3 expected for this file
            "2025-02-14T16:29:30:000000/tagPipeSeparatedWindows-1.csv",
            # should ignore chunks from older dates
            "2025-02-13T16:29:30:000000/tagPipeSeparatedWindows-2.csv",
            "2025-02-13T16:29:30:000000/tagPipeSeparatedWindows-3.csv",
            "2025-02-14T12:29:30:000000/tagRowExtraColumns.csv",
        ]

        result = RequireFilesBeforeUploadMixin.are_all_referenced_daily_files_present(
            region_raw_file_config=self.region_raw_file_config,
            ingest_ready_normalized_file_paths=ingest_ready_normalized_file_paths,
        )
        self.assertFalse(result)
