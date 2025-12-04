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
"""Unit tests for raw file chunking metadata classes"""

import datetime
from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata import (
    SequentiallyChunkedFileMetadata,
    SingleFileMetadata,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawGCSFileMetadata,
)


class TestSingleFileMetadata(TestCase):
    """Unit tests for SingleFileMetadata"""

    def test_coalesce_single_file_success(self) -> None:
        gcs_file = RawGCSFileMetadata(
            gcs_file_id=1,
            file_id=None,
            path=GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2025-01-15T12:30:45:000000_raw_test_file.csv"
            ),
        )

        metadata = SingleFileMetadata()
        bq_files, skipped = metadata.coalesce_files(
            file_tag="test_file", gcs_files=[gcs_file]
        )

        assert len(bq_files) == 1
        assert len(skipped) == 0

        assert bq_files[0] == RawBigQueryFileMetadata(
            gcs_files=[gcs_file],
            file_tag="test_file",
            update_datetime=datetime.datetime(
                2025, 1, 15, 12, 30, 45, tzinfo=datetime.UTC
            ),
            file_id=None,
        )

    def test_coalesce_multiple_files_error(self) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                gcs_file_id=i,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    f"test_bucket/unprocessed_2025-01-15T12:30:{str(i).zfill(2)}:000000_raw_test_file.csv"
                ),
            )
            for i in range(3)
        ]

        metadata = SingleFileMetadata()
        bq_files, skipped = metadata.coalesce_files(
            file_tag="test_file", gcs_files=gcs_files
        )

        assert len(bq_files) == 0
        assert len(skipped) == 1
        assert (
            skipped[0].skipped_message
            == "Skipping grouping for [test_file]; expected exactly 1 file, but found [3]"
        )
        assert skipped[0].file_tag == "test_file"
        assert len(skipped[0].file_paths) == 3


class TestSequentiallyChunkedFileMetadata(TestCase):
    """Unit tests for SequentiallyChunkedFileMetadata"""

    def test_empty_files_error(self) -> None:
        metadata = SequentiallyChunkedFileMetadata(known_chunk_count=None)
        with self.assertRaisesRegex(
            ValueError, r"Must provide at least one file to coalesce; found none."
        ):
            metadata.coalesce_files(file_tag="test", gcs_files=[])

    def test_file_counts_differ_with_known_count(self) -> None:
        metadata = SequentiallyChunkedFileMetadata(known_chunk_count=10)
        gcs_files = [
            RawGCSFileMetadata(
                gcs_file_id=1,
                file_id=1,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_c-1.csv"
                ),
            )
        ]

        bq_files, skipped_errors = metadata.coalesce_files(
            file_tag="tag_c", gcs_files=gcs_files
        )

        assert len(bq_files) == 0
        assert len(skipped_errors) == 1
        assert (
            skipped_errors[0].skipped_message
            == "Skipping grouping for [tag_c]; Expected [10] chunks, but found [1]"
        )

    def test_not_sequential(self) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                gcs_file_id=1,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_c-1.csv"
                ),
            ),
            RawGCSFileMetadata(
                gcs_file_id=2,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:01:000000_raw_tag_c-1.csv"
                ),
            ),
            RawGCSFileMetadata(
                gcs_file_id=3,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:02:000000_raw_tag_c-3.csv"
                ),
            ),
        ]

        # Test with known chunk count
        metadata = SequentiallyChunkedFileMetadata(known_chunk_count=3)
        bq_files, skipped_errors = metadata.coalesce_files(
            file_tag="tag_c", gcs_files=gcs_files
        )

        assert len(bq_files) == 0
        assert len(skipped_errors) == 1
        assert (
            "missing expected sequential suffixes" in skipped_errors[0].skipped_message
        )

        # Test with dynamic chunk count (no known_chunk_count)
        metadata = SequentiallyChunkedFileMetadata(known_chunk_count=None)
        bq_files, skipped_errors = metadata.coalesce_files(
            file_tag="tag_c", gcs_files=gcs_files
        )

        assert len(bq_files) == 0
        assert len(skipped_errors) == 1
        assert (
            "missing expected sequential suffixes" in skipped_errors[0].skipped_message
        )

    def test_sequential_1_indexed(self) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                gcs_file_id=1,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_c-0001.csv"
                ),
            ),
            RawGCSFileMetadata(
                gcs_file_id=2,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:01:000000_raw_tag_c-0002.csv"
                ),
            ),
            RawGCSFileMetadata(
                gcs_file_id=3,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:02:000000_raw_tag_c-0003.csv"
                ),
            ),
        ]

        # Test with dynamic chunk count
        metadata = SequentiallyChunkedFileMetadata(
            known_chunk_count=None, zero_indexed=False
        )
        bq_files, skipped_errors = metadata.coalesce_files(
            file_tag="tag_c", gcs_files=gcs_files
        )

        assert len(bq_files) == 1
        assert len(skipped_errors) == 0
        assert bq_files[0] == RawBigQueryFileMetadata(
            file_id=None,
            gcs_files=gcs_files,
            file_tag="tag_c",
            update_datetime=datetime.datetime(2024, 1, 2, 0, 0, 2, tzinfo=datetime.UTC),
        )

        # Test with known chunk count
        metadata = SequentiallyChunkedFileMetadata(
            known_chunk_count=3, zero_indexed=False
        )
        bq_files, skipped_errors = metadata.coalesce_files(
            file_tag="tag_c", gcs_files=gcs_files
        )

        assert len(bq_files) == 1
        assert len(skipped_errors) == 0

    def test_sequential_0_indexed(self) -> None:
        gcs_files = [
            RawGCSFileMetadata(
                gcs_file_id=1,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_c-0000.csv"
                ),
            ),
            RawGCSFileMetadata(
                gcs_file_id=2,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:01:000000_raw_tag_c-0001.csv"
                ),
            ),
            RawGCSFileMetadata(
                gcs_file_id=3,
                file_id=None,
                path=GcsfsFilePath.from_absolute_path(
                    "test_bucket/unprocessed_2024-01-02T00:00:02:000000_raw_tag_c-0002.csv"
                ),
            ),
        ]

        # Test that 1-indexed requirement fails
        metadata = SequentiallyChunkedFileMetadata(
            known_chunk_count=None, zero_indexed=False
        )
        bq_files, skipped_errors = metadata.coalesce_files(
            file_tag="tag_c", gcs_files=gcs_files
        )

        assert len(bq_files) == 0
        assert len(skipped_errors) == 1

        # Test that 0-indexed succeeds
        metadata = SequentiallyChunkedFileMetadata(
            known_chunk_count=None, zero_indexed=True
        )
        bq_files, skipped_errors = metadata.coalesce_files(
            file_tag="tag_c", gcs_files=gcs_files
        )

        assert len(bq_files) == 1
        assert len(skipped_errors) == 0
        assert bq_files[0] == RawBigQueryFileMetadata(
            file_id=None,
            gcs_files=gcs_files,
            file_tag="tag_c",
            update_datetime=datetime.datetime(2024, 1, 2, 0, 0, 2, tzinfo=datetime.UTC),
        )
