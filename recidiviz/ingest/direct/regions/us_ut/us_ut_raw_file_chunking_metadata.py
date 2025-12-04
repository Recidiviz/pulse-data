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
"""Raw file chunking metadata for US_UT."""

import datetime

from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata import (
    SequentiallyChunkedFileMetadata,
    SingleFileMetadata,
)
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    RawFileChunkingMetadataHistory,
)

US_UT_CHUNKING_METADATA_BY_FILE_TAG: dict[str, RawFileChunkingMetadataHistory] = {
    "sprvsn_cntc": RawFileChunkingMetadataHistory(
        file_tag="sprvsn_cntc",
        chunking_metadata_history=[
            SingleFileMetadata(
                start_date=None,
                end_date_exclusive=datetime.date(2025, 2, 14),
            ),
            SequentiallyChunkedFileMetadata(
                known_chunk_count=None,
                start_date=datetime.date(2025, 2, 14),
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "tst_qstn_rspns": RawFileChunkingMetadataHistory(
        file_tag="tst_qstn_rspns",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
}
