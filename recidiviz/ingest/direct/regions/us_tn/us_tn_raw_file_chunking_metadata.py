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
    SingleFileMetadata,
)
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    RawFileChunkingMetadataHistory,
)

# MiCase delivered every one of its file tags' historical backfill as numbered
# chunks on this date (except IN_CASE_NOTE_TYPES, a day later), then switched to
# delivering daily incremental files as a single unchunked file with no suffix.
_MICASE_BACKFILL_END_DATE_EXCLUSIVE = datetime.date(2026, 8, 7)

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
                # One-time historical backfill, delivered as 150 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "BOP_PAROLE_STAFF_ACTION": RawFileChunkingMetadataHistory(
        file_tag="BOP_PAROLE_STAFF_ACTION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "CCR_CRIMINAL_HISTORY": RawFileChunkingMetadataHistory(
        file_tag="CCR_CRIMINAL_HISTORY",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "CD_HEARING_REPORT": RawFileChunkingMetadataHistory(
        file_tag="CD_HEARING_REPORT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "CD_HEARING_SANCTION": RawFileChunkingMetadataHistory(
        file_tag="CD_HEARING_SANCTION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "CD_STAFF_REVIEW": RawFileChunkingMetadataHistory(
        file_tag="CD_STAFF_REVIEW",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "CL_CAF_SCORING": RawFileChunkingMetadataHistory(
        file_tag="CL_CAF_SCORING",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 150 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "CL_CLASSIFICATION": RawFileChunkingMetadataHistory(
        file_tag="CL_CLASSIFICATION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "EV_EVENT": RawFileChunkingMetadataHistory(
        file_tag="EV_EVENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "EV_INCIDENT_REPORT": RawFileChunkingMetadataHistory(
        file_tag="EV_INCIDENT_REPORT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "EV_INVOLVED_INMATE": RawFileChunkingMetadataHistory(
        file_tag="EV_INVOLVED_INMATE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "EV_INVOLVED_NON_INMATE": RawFileChunkingMetadataHistory(
        file_tag="EV_INVOLVED_NON_INMATE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "IN_CASE_NOTE_TYPES": RawFileChunkingMetadataHistory(
        file_tag="IN_CASE_NOTE_TYPES",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered a day later than the
                # other MiCase tags, as 999 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=datetime.date(2026, 8, 8),
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=datetime.date(2026, 8, 8),
                end_date_exclusive=None,
            ),
        ],
    ),
    "PERSON": RawFileChunkingMetadataHistory(
        file_tag="PERSON",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "PM_BED_ASSIGNMENT": RawFileChunkingMetadataHistory(
        file_tag="PM_BED_ASSIGNMENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "PM_EXTERNAL_MOVEMENT": RawFileChunkingMetadataHistory(
        file_tag="PM_EXTERNAL_MOVEMENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "SC_CONVERTED_CREDIT": RawFileChunkingMetadataHistory(
        file_tag="SC_CONVERTED_CREDIT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "SC_CREDIT_LAW_WAIVER": RawFileChunkingMetadataHistory(
        file_tag="SC_CREDIT_LAW_WAIVER",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "SC_SENTENCE": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "SC_SENTENCEACTION": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCEACTION",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 400 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "SC_SENTENCE_COMMENT": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCE_COMMENT",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "SC_SENTENCINGNOTE": RawFileChunkingMetadataHistory(
        file_tag="SC_SENTENCINGNOTE",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "VIC_VICTIM": RawFileChunkingMetadataHistory(
        file_tag="VIC_VICTIM",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
    "VIC_VICTIM_PERSON": RawFileChunkingMetadataHistory(
        file_tag="VIC_VICTIM_PERSON",
        chunking_metadata_history=[
            SequentiallyChunkedFileMetadata(
                # One-time historical backfill, delivered as 50 numbered chunks.
                known_chunk_count=None,
                start_date=None,
                end_date_exclusive=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                zero_indexed=True,
            ),
            SingleFileMetadata(
                start_date=_MICASE_BACKFILL_END_DATE_EXCLUSIVE,
                end_date_exclusive=None,
            ),
        ],
    ),
}
