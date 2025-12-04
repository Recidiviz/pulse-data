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
"""Shared testing utilities for raw data"""

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_data_import_chunked_file_handler import (
    RawDataImportChunkedFileHandler,
)
from recidiviz.ingest.direct.raw_data.raw_data_import_chunked_file_handler_factory import (
    RawDataImportChunkedFileHandlerFactory,
)
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata import (
    SequentiallyChunkedFileMetadata,
)
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    RawFileChunkingMetadataHistory,
)

US_XX_CHUNKING_METADATA_BY_FILE_TAG = {
    "tagChunkedFile": RawFileChunkingMetadataHistory(
        file_tag="tagChunkedFile",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                known_chunk_count=3,
            ),
        ],
    ),
    "tagChunkedFileTwo": RawFileChunkingMetadataHistory(
        file_tag="tagChunkedFileTwo",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                known_chunk_count=4,
            ),
        ],
    ),
}


class FakeRawDataImportChunkedFileHandlerFactory(
    RawDataImportChunkedFileHandlerFactory
):
    """Test factory that creates RawDataImportChunkedFileHandler instances for testing."""

    @classmethod
    def build(cls, *, region_code: str) -> RawDataImportChunkedFileHandler:
        region_code = region_code.upper()
        if region_code == StateCode.US_XX.value:
            return RawDataImportChunkedFileHandler(
                chunking_metadata_by_file_tag=US_XX_CHUNKING_METADATA_BY_FILE_TAG
            )
        if region_code in {StateCode.US_LL.value, StateCode.US_YY.value}:
            return RawDataImportChunkedFileHandler()
        raise ValueError(f"Unexpected region code provided: {region_code}")
