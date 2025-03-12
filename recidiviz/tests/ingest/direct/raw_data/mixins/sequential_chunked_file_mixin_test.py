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
"""Unit tests for SequentialChunkedFileMixin"""
import datetime
from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.raw_data.mixins.sequential_chunked_file_mixin import (
    SequentialChunkedFileMixin,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawGCSFileMetadata,
)


class TestSequentialChunkedFileMixin(TestCase):
    """Unit tests for SequentialChunkedFileMixin"""

    def test_bad_inputs(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"n must a positive integer; found \[0\]"
        ):
            SequentialChunkedFileMixin.group_n_files_with_sequential_suffixes(
                n=0, gcs_files=[], file_tag=""
            )

        with self.assertRaisesRegex(
            ValueError, r"Must provide at least one file to group; found none."
        ):
            SequentialChunkedFileMixin.group_files_with_sequential_suffixes(
                gcs_files=[], file_tag=""
            )

    def test_file_counts_differ(self) -> None:
        (
            bq_files,
            skipped_errors,
        ) = SequentialChunkedFileMixin.group_n_files_with_sequential_suffixes(
            n=10,
            gcs_files=[
                RawGCSFileMetadata(
                    gcs_file_id=1,
                    file_id=1,
                    path=GcsfsFilePath.from_absolute_path(
                        "test_bucket/unprocessed_2024-01-02T00:00:00:000000_raw_tag_c.csv"
                    ),
                )
            ],
            file_tag="tag_c",
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
        (
            bq_files,
            skipped_errors,
        ) = SequentialChunkedFileMixin.group_n_files_with_sequential_suffixes(
            n=3,
            gcs_files=gcs_files,
            file_tag="tag_c",
        )

        assert len(bq_files) == 0
        assert len(skipped_errors) == 1

        assert (
            skipped_errors[0].skipped_message
            == "Skipping grouping for [tag_c]; missing expected sequential suffixes [{2}] and/or found extra suffixes [set()]"
        )

        (
            bq_files,
            skipped_errors,
        ) = SequentialChunkedFileMixin.group_files_with_sequential_suffixes(
            gcs_files=gcs_files,
            file_tag="tag_c",
        )

        assert len(bq_files) == 0
        assert len(skipped_errors) == 1

        assert (
            skipped_errors[0].skipped_message
            == "Skipping grouping for [tag_c]; missing expected sequential suffixes [{2}] and/or found extra suffixes [set()]"
        )

    def test_sequential(self) -> None:
        (
            bq_files,
            skipped_errors,
        ) = SequentialChunkedFileMixin.group_files_with_sequential_suffixes(
            gcs_files=[
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
            ],
            file_tag="tag_c",
        )

        assert len(bq_files) == 1
        assert len(skipped_errors) == 0

        assert bq_files[0] == RawBigQueryFileMetadata(
            file_id=None,
            gcs_files=[
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
            ],
            file_tag="tag_c",
            update_datetime=datetime.datetime(2024, 1, 2, 0, 0, 2, tzinfo=datetime.UTC),
        )
