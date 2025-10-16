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
"""Unit tests for NonChunkedFileMixin"""

import datetime
from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.raw_data.mixins.non_chunked_file_mixin import (
    NonChunkedFileMixin,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawGCSFileMetadata,
)


class TestNonChunkedFileMixin(TestCase):
    """Unit tests for NonChunkedFileMixin"""

    def test_group_single_file_success(self) -> None:
        gcs_file = RawGCSFileMetadata(
            gcs_file_id=1,
            file_id=None,
            path=GcsfsFilePath.from_absolute_path(
                "test_bucket/unprocessed_2025-01-15T12:30:45:000000_raw_test_file.csv"
            ),
        )

        bq_files, skipped = NonChunkedFileMixin.group_single_non_chunked_file(
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

    def test_group_multiple_files_error(self) -> None:
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

        bq_files, skipped = NonChunkedFileMixin.group_single_non_chunked_file(
            file_tag="test_file", gcs_files=gcs_files
        )

        assert len(bq_files) == 0
        assert len(skipped) == 1
        assert (
            "Expected exactly 1 GCS file, but found [3]" in skipped[0].skipped_message
        )
        assert skipped[0].file_tag == "test_file"
        assert len(skipped[0].file_paths) == 3
