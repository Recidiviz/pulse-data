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
"""Raw file chunking metadata for US_OZ."""
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata import (
    SequentiallyChunkedFileMetadata,
)
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    RawFileChunkingMetadataHistory,
)

US_OZ_CHUNKING_METADATA_BY_FILE_TAG: dict[str, RawFileChunkingMetadataHistory] = {
    "lds_person": RawFileChunkingMetadataHistory(
        file_tag="lds_person",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                known_chunk_count=2,
            ),
        ],
    ),
}
