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
"""Shared testing utilities for sftp operators"""

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.base_raw_data_import_delegate import (
    BaseRawDataImportDelegate,
    skipped_error_for_unrecognized_file_tag_for_chunked_files,
)
from recidiviz.ingest.direct.raw_data.mixins.sequential_chunked_file_mixin import (
    SequentialChunkedFileMixin,
)
from recidiviz.ingest.direct.raw_data.raw_data_import_delegate_factory import (
    RawDataImportDelegateFactory,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)

TEST_PROJECT_ID = "recidiviz-testing"


class FakeRawDataImportDelegateFactory(RawDataImportDelegateFactory):
    @classmethod
    def build(cls, *, region_code: str) -> BaseRawDataImportDelegate:
        region_code = region_code.upper()
        if region_code == StateCode.US_XX.value:
            return FakeUsXxRawDataImportDelegate()
        if region_code == StateCode.US_LL.value:
            return FakeUsLlRawDataImportDelegate()
        if region_code == StateCode.US_YY.value:
            return FakeUsLlRawDataImportDelegate()
        raise ValueError(f"Unexpected region code provided: {region_code}")


class FakeUsXxRawDataImportDelegate(
    BaseRawDataImportDelegate, SequentialChunkedFileMixin
):
    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:

        if file_tag == "tagChunkedFile":
            mx = SequentialChunkedFileMixin.group_n_files_with_sequential_suffixes(
                n=3, file_tag=file_tag, gcs_files=gcs_files
            )
            print(f"!booya -- {file_tag} -- {mx}")
            return SequentialChunkedFileMixin.group_n_files_with_sequential_suffixes(
                n=3, file_tag=file_tag, gcs_files=gcs_files
            )

        if file_tag == "tagChunkedFileTwo":
            mx = SequentialChunkedFileMixin.group_n_files_with_sequential_suffixes(
                n=4, file_tag=file_tag, gcs_files=gcs_files
            )
            print(f"!booya22 -- {file_tag} -- {mx}")
            return SequentialChunkedFileMixin.group_n_files_with_sequential_suffixes(
                n=4, file_tag=file_tag, gcs_files=gcs_files
            )

        print(f"!booya skipped {file_tag}")

        return skipped_error_for_unrecognized_file_tag_for_chunked_files(
            file_tag=file_tag, gcs_files=gcs_files
        )


class FakeUsLlRawDataImportDelegate(BaseRawDataImportDelegate):
    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        return skipped_error_for_unrecognized_file_tag_for_chunked_files(
            file_tag=file_tag, gcs_files=gcs_files
        )


class FakeUsYyRawDataImportDelegate(BaseRawDataImportDelegate):
    def coalesce_chunked_files(
        self, file_tag: str, gcs_files: list[RawGCSFileMetadata]
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        return skipped_error_for_unrecognized_file_tag_for_chunked_files(
            file_tag=file_tag, gcs_files=gcs_files
        )
