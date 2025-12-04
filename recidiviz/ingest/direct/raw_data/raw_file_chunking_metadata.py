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
"""Classes for defining raw file chunking behavior and their associated metadata."""
import abc
import datetime
import logging

import attr

from recidiviz.common import attr_validators
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)
from recidiviz.utils.types import assert_type


@attr.define
class RawFileChunkingMetadata(abc.ABC):
    """Abstract base class defining how a file is chunked."""

    start_date: datetime.date | None = attr.ib(validator=attr_validators.is_opt_date)
    end_date_exclusive: datetime.date | None = attr.ib(
        validator=attr_validators.is_opt_date
    )

    @abc.abstractmethod
    def coalesce_files(
        self,
        *,
        file_tag: str,
        gcs_files: list[RawGCSFileMetadata],
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Coalesces multiple GCS files into a single BigQuery metadata entry.

        Args:
            file_tag: The tag of the file being processed.
            gcs_files: The list of GCS files to coalesce.

        Returns:
            A tuple of:
                - A list of RawBigQueryFileMetadata entries representing the
                  coalesced files.
                - A list of RawDataFilesSkippedError entries indicating any errors
                  encountered during coalescing.
        """

    @property
    @abc.abstractmethod
    def expected_file_count(self) -> int | None:
        """Returns the expected number of files that make up a single, logical file tag
        for this chunking strategy.

        Returns:
            The expected number of files, or None if any number of files is acceptable.
        """


@attr.define
class SequentiallyChunkedFileMetadata(RawFileChunkingMetadata):
    """Metadata for files that are chunked with sequential numeric suffixes.

    Attributes:
        known_chunk_count: If specified, the exact number of chunks expected for
            this file type. If None, any number of chunks is acceptable.
        start_date: The start date (inclusive) for which this chunking metadata is
            applicable. If None, indicates this is the initial metadata with no start.
        end_date_exclusive: The end date (exclusive) for which this chunking metadata
            is applicable. If None, it is currently applicable.
        zero_indexed: Whether the sequential suffixes are zero-indexed (starting
            at 0) or one-indexed (starting at 1).
    """

    known_chunk_count: int | None = attr.ib(validator=attr_validators.is_opt_int)
    start_date: datetime.date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    end_date_exclusive: datetime.date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    zero_indexed: bool = attr.ib(default=False, validator=attr_validators.is_bool)

    def coalesce_files(
        self,
        *,
        file_tag: str,
        gcs_files: list[RawGCSFileMetadata],
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Coalesces sequentially-suffixed files, validating chunk count if known."""
        if len(gcs_files) == 0:
            raise ValueError("Must provide at least one file to coalesce; found none.")

        # If a known chunk count is specified, validate we have exactly that many
        if self.known_chunk_count is not None:
            if len(gcs_files) != self.known_chunk_count:
                return [], [
                    RawDataFilesSkippedError.from_gcs_files_and_message(
                        skipped_message=(
                            f"Skipping grouping for [{file_tag}]; Expected "
                            f"[{self.known_chunk_count}] chunks, but found "
                            f"[{len(gcs_files)}]"
                        ),
                        gcs_files=gcs_files,
                        file_tag=file_tag,
                    )
                ]

        # Validate that file suffixes are sequential integers
        actual_suffixes: set[int] = set()
        for gcs_file in gcs_files:
            try:
                suffix_int = int(assert_type(gcs_file.parts.filename_suffix, str))
                actual_suffixes.add(suffix_int)
            except Exception as e:
                return [], [
                    RawDataFilesSkippedError.from_gcs_files_and_message(
                        skipped_message=(
                            f"Skipping grouping for [{file_tag}]; expected filename "
                            f"suffix for [{gcs_file.path.abs_path()}] to be "
                            f"integer-parseable but found [{gcs_file.parts.filename_suffix}] "
                            f"instead: \n{e}"
                        ),
                        gcs_files=gcs_files,
                        file_tag=file_tag,
                    )
                ]

        # Build expected suffix set
        starting_suffix = 0 if self.zero_indexed else 1
        expected_suffixes = set(
            range(starting_suffix, len(gcs_files) + starting_suffix)
        )

        if actual_suffixes != expected_suffixes:
            extra_suffixes = actual_suffixes - expected_suffixes
            missing_suffixes = expected_suffixes - actual_suffixes
            return [], [
                RawDataFilesSkippedError.from_gcs_files_and_message(
                    skipped_message=(
                        f"Skipping grouping for [{file_tag}]; missing expected "
                        f"sequential suffixes [{sorted(missing_suffixes)}] and/or found "
                        f"extra suffixes [{sorted(extra_suffixes)}]"
                    ),
                    gcs_files=gcs_files,
                    file_tag=file_tag,
                )
            ]

        logging.info(
            "Coalescing [%s] chunks for [%s] on [%s]",
            len(gcs_files),
            file_tag,
            gcs_files[0].parts.utc_upload_datetime.date(),
        )
        return [RawBigQueryFileMetadata.from_gcs_files(gcs_files=gcs_files)], []

    @property
    def expected_file_count(self) -> int | None:
        return self.known_chunk_count


@attr.define
class SingleFileMetadata(RawFileChunkingMetadata):
    """Metadata for non-chunked files.

    This is useful for handling files that were historically sent as single files
    but may have transitioned to chunked files at a later date.
    """

    start_date: datetime.date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )
    end_date_exclusive: datetime.date | None = attr.ib(
        default=None, validator=attr_validators.is_opt_date
    )

    def coalesce_files(
        self,
        *,
        file_tag: str,
        gcs_files: list[RawGCSFileMetadata],
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Validates that exactly one file is present and creates a single bq metadata from it."""
        if len(gcs_files) != 1:
            return [], [
                RawDataFilesSkippedError.from_gcs_files_and_message(
                    skipped_message=(
                        f"Skipping grouping for [{file_tag}]; expected exactly 1 file, "
                        f"but found [{len(gcs_files)}]"
                    ),
                    gcs_files=gcs_files,
                    file_tag=file_tag,
                )
            ]

        logging.info(
            "Coalescing single file for [%s] on [%s]",
            file_tag,
            gcs_files[0].parts.utc_upload_datetime.date(),
        )
        return [RawBigQueryFileMetadata.from_gcs_files(gcs_files=gcs_files)], []

    @property
    def expected_file_count(self) -> int | None:
        return 1
