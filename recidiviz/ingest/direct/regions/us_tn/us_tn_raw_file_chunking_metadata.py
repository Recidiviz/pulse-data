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
"""Raw file chunking metadata for US_TN."""

import datetime

from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata import (
    SequentiallyChunkedFileMetadata,
)
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    RawFileChunkingMetadataHistory,
)

US_TN_CHUNKING_METADATA_BY_FILE_TAG: dict[str, RawFileChunkingMetadataHistory] = {
    "ContactNoteComment": RawFileChunkingMetadataHistory(
        file_tag="ContactNoteComment",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                known_chunk_count=19,
                start_date=None,
                end_date_exclusive=datetime.date(2025, 2, 15),
            ),
            SequentiallyChunkedFileMetadata(
                known_chunk_count=20,
                start_date=datetime.date(2025, 2, 15),
                end_date_exclusive=None,
            ),
        ],
    ),
    "AD_LOCATION": RawFileChunkingMetadataHistory(
        file_tag="AD_LOCATION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "BOP_PAROLE_STAFF_ACTION": RawFileChunkingMetadataHistory(
        file_tag="BOP_PAROLE_STAFF_ACTION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "CCR_CRIMINAL_HISTORY": RawFileChunkingMetadataHistory(
        file_tag="CCR_CRIMINAL_HISTORY",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "CD_HEARING_REPORT": RawFileChunkingMetadataHistory(
        file_tag="CD_HEARING_REPORT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "CD_HEARING_SANCTION": RawFileChunkingMetadataHistory(
        file_tag="CD_HEARING_SANCTION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "CD_STAFF_REVIEW": RawFileChunkingMetadataHistory(
        file_tag="CD_STAFF_REVIEW",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "CL_CAF_SCORING": RawFileChunkingMetadataHistory(
        file_tag="CL_CAF_SCORING",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "CL_CLASSIFICATION": RawFileChunkingMetadataHistory(
        file_tag="CL_CLASSIFICATION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "EV_EVENT": RawFileChunkingMetadataHistory(
        file_tag="EV_EVENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "EV_INCIDENT_REPORT": RawFileChunkingMetadataHistory(
        file_tag="EV_INCIDENT_REPORT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "EV_INVOLVED_INMATE": RawFileChunkingMetadataHistory(
        file_tag="EV_INVOLVED_INMATE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "EV_INVOLVED_NON_INMATE": RawFileChunkingMetadataHistory(
        file_tag="EV_INVOLVED_NON_INMATE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "IN_CASE_NOTE_TYPES": RawFileChunkingMetadataHistory(
        file_tag="IN_CASE_NOTE_TYPES",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "PERSON": RawFileChunkingMetadataHistory(
        file_tag="PERSON",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "PM_BED_ASSIGNMENT": RawFileChunkingMetadataHistory(
        file_tag="PM_BED_ASSIGNMENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "PM_EXTERNAL_MOVEMENT": RawFileChunkingMetadataHistory(
        file_tag="PM_EXTERNAL_MOVEMENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "SC_CONVERTED_CREDIT": RawFileChunkingMetadataHistory(
        file_tag="SC_CONVERTED_CREDIT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "SC_CREDIT_LAW_WAIVER": RawFileChunkingMetadataHistory(
        file_tag="SC_CREDIT_LAW_WAIVER",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "SC_SENTENCE": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "SC_SENTENCEACTION": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCEACTION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "SC_SENTENCE_COMMENT": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCE_COMMENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "SC_SENTENCINGNOTE": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCINGNOTE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "VIC_VICTIM": RawFileChunkingMetadataHistory(
        file_tag="VIC_VICTIM",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
    "VIC_VICTIM_PERSON": RawFileChunkingMetadataHistory(
        file_tag="VIC_VICTIM_PERSON",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # MiCase sizes each nightly delivery by volume, so the chunk count varies
                # between runs (the initial load ranged from 50 to 999 chunks across tags).
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=None,
                zero_indexed=True,
            ),
        ],
    ),
}
