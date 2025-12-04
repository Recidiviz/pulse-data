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
"""Unit tests for RawFileChunkingMetadataHistory"""

import datetime
from unittest import TestCase

from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata import (
    SequentiallyChunkedFileMetadata,
    SingleFileMetadata,
)
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata_history import (
    RawFileChunkingMetadataHistory,
)


class TestRawFileChunkingMetadataHistory(TestCase):
    """Unit tests for RawFileChunkingMetadataHistory"""

    def test_missing_current_or_first_entry_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "must have a current entry with end_date_exclusive=None",
        ):
            RawFileChunkingMetadataHistory(
                file_tag="test_file",
                chunking_metadata_history=[
                    SingleFileMetadata(
                        end_date_exclusive=datetime.date(2025, 2, 13),
                    ),
                    SequentiallyChunkedFileMetadata(
                        known_chunk_count=5,
                        zero_indexed=False,
                        end_date_exclusive=datetime.date(2025, 3, 1),
                    ),
                ],
            )

        with self.assertRaisesRegex(
            ValueError,
            "must have a first entry with start_date=None",
        ):
            RawFileChunkingMetadataHistory(
                file_tag="test_file",
                chunking_metadata_history=[
                    SequentiallyChunkedFileMetadata(
                        known_chunk_count=5,
                        zero_indexed=False,
                        start_date=datetime.date(2025, 2, 13),
                        end_date_exclusive=datetime.date(2025, 3, 1),
                    ),
                    SequentiallyChunkedFileMetadata(
                        known_chunk_count=None,
                        zero_indexed=False,
                        start_date=datetime.date(2025, 3, 1),
                        end_date_exclusive=None,
                    ),
                ],
            )

    def test_empty_history_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Chunking metadata history for \[test_file\] must have at least one entry",
        ):
            RawFileChunkingMetadataHistory(
                file_tag="test_file",
                chunking_metadata_history=[],
            )

    def test_get_metadata_for_date(self) -> None:
        # Provide unsorted history
        history = RawFileChunkingMetadataHistory(
            file_tag="test_file",
            chunking_metadata_history=[
                SequentiallyChunkedFileMetadata(
                    known_chunk_count=None,
                    start_date=datetime.date(2025, 3, 1),
                    end_date_exclusive=None,
                ),
                SingleFileMetadata(
                    start_date=datetime.date(2025, 2, 13),
                    end_date_exclusive=datetime.date(2025, 3, 1),
                ),
                SingleFileMetadata(
                    end_date_exclusive=datetime.date(2025, 2, 13),
                ),
            ],
        )

        # Verify history is sorted correctly
        assert history.chunking_metadata_history == [
            SingleFileMetadata(
                end_date_exclusive=datetime.date(2025, 2, 13),
            ),
            SingleFileMetadata(
                start_date=datetime.date(2025, 2, 13),
                end_date_exclusive=datetime.date(2025, 3, 1),
            ),
            SequentiallyChunkedFileMetadata(
                known_chunk_count=None,
                start_date=datetime.date(2025, 3, 1),
                end_date_exclusive=None,
            ),
        ]

        # Date is before first end_date_exclusive in list
        first_metadata_entry = history.get_metadata_for_date(datetime.date(2025, 2, 1))
        assert first_metadata_entry == history.chunking_metadata_history[0]

        # Date is between first and second end_date_exclusive in list
        second_metadata_entry = history.get_metadata_for_date(
            datetime.date(2025, 2, 20)
        )
        assert second_metadata_entry == history.chunking_metadata_history[1]

        # Date is after second end_date_exclusive in list
        third_metadata_entry = history.get_metadata_for_date(datetime.date(2025, 4, 1))
        assert third_metadata_entry == history.chunking_metadata_history[2]

    def test_non_consecutive_spans_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "has non-consecutive spans: metadata 0 has end_date_exclusive=2025-02-13 but metadata 1 has start_date=2025-02-15",
        ):
            RawFileChunkingMetadataHistory(
                file_tag="test_file",
                chunking_metadata_history=[
                    SingleFileMetadata(
                        end_date_exclusive=datetime.date(2025, 2, 13),
                    ),
                    SingleFileMetadata(
                        start_date=datetime.date(2025, 2, 15),
                        end_date_exclusive=datetime.date(2025, 3, 1),
                    ),
                    SequentiallyChunkedFileMetadata(
                        known_chunk_count=None,
                        start_date=datetime.date(2025, 3, 1),
                        end_date_exclusive=None,
                    ),
                ],
            )
