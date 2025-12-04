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
"""Tracks the history of raw file chunking behavior across date ranges."""
import datetime

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_chunking_metadata import (
    RawFileChunkingMetadata,
)


@attr.define
class RawFileChunkingMetadataHistory:
    """Tracks the history of chunking behavior for a file tag across one or more date ranges."""

    file_tag: str = attr.ib(validator=attr_validators.is_str)
    chunking_metadata_history: list[RawFileChunkingMetadata] = attr.ib(
        validator=attr_validators.is_list
    )

    def __attrs_post_init__(self) -> None:
        if len(self.chunking_metadata_history) == 0:
            raise ValueError(
                f"Chunking metadata history for [{self.file_tag}] must have at least one entry."
            )
        # Ensure history is sorted from oldest to newest based on start_date
        self.chunking_metadata_history.sort(
            key=lambda metadata: (
                metadata.start_date
                if metadata.start_date is not None
                else datetime.date.min
            )
        )
        if self.chunking_metadata_history[0].start_date is not None:
            # Enforce that history has an open start date so people don't accidentally build incorrect
            # histories for files that started chunking later on. Ex if a file became chunked on 2025-03-01,
            # the first entry in the history should be a single file chunking metadata with start_date=None and end_date_exclusive=2025-03-01,
            # and the second entry should be a chunking metadata with start_date=2025-03-01 and end_date_exclusive=None.
            raise ValueError(
                f"Chunking metadata history for [{self.file_tag}] must have a first entry "
                "with start_date=None"
            )
        if self.chunking_metadata_history[-1].end_date_exclusive is not None:
            raise ValueError(
                f"Chunking metadata history for [{self.file_tag}] must have a current entry "
                "with end_date_exclusive=None"
            )

        # Enforce consecutive spans
        for i in range(len(self.chunking_metadata_history) - 1):
            current_metadata = self.chunking_metadata_history[i]
            next_metadata = self.chunking_metadata_history[i + 1]

            if current_metadata.end_date_exclusive != next_metadata.start_date:
                raise ValueError(
                    f"Chunking metadata history for [{self.file_tag}] has non-consecutive "
                    f"spans: metadata {i} has end_date_exclusive={current_metadata.end_date_exclusive} "
                    f"but metadata {i + 1} has start_date={next_metadata.start_date}"
                )

    def get_metadata_for_date(
        self, utc_upload_date: datetime.date
    ) -> RawFileChunkingMetadata:
        """Gets the applicable chunking metadata for a given date."""
        for metadata in self.chunking_metadata_history:
            start_ok = (
                metadata.start_date is None or utc_upload_date >= metadata.start_date
            )
            end_ok = (
                metadata.end_date_exclusive is None
                or utc_upload_date < metadata.end_date_exclusive
            )
            if start_ok and end_ok:
                return metadata

        raise ValueError(
            f"No chunking metadata found for file tag [{self.file_tag}] "
            f"and date [{utc_upload_date}]"
        )

    @property
    def current_expected_file_count(self) -> int | None:
        """Gets the expected number of files that make up a single, logical file
        for the current period.

        Returns:
            The expected file count, or None if the current metadata
            allows any number of files.
        """
        current_metadata = self.chunking_metadata_history[-1]
        return current_metadata.expected_file_count


@attr.define
class StateRawFileChunkingMetadata:
    """Encapsulates metadata for any chunked files in a given state.

    Attributes:
        state_code: The state code for which this metadata applies.
        chunking_metadata_by_file_tag: Dictionary mapping file tags to their
            chunking metadata history. If None, no files are chunked.
    """

    state_code: StateCode
    chunking_metadata_by_file_tag: dict[
        str, RawFileChunkingMetadataHistory
    ] | None = attr.ib(default=None, validator=attr_validators.is_opt_dict)

    def get_current_expected_file_count(self, file_tag: str) -> int | None:
        """Gets the expected number of files that make up a single, logical file
        for the current period for a file tag.

        Returns:
            The expected file count, or None if the current metadata allows any
            number of files. If no chunking metadata exists for the file tag,
            defaults to 1 (indicating a single file).
        """
        if (
            self.chunking_metadata_by_file_tag is None
            or file_tag not in self.chunking_metadata_by_file_tag
        ):
            return 1  # Default to single file if no chunking metadata exists for the file tag

        metadata_history = self.chunking_metadata_by_file_tag[file_tag]
        return metadata_history.current_expected_file_count
