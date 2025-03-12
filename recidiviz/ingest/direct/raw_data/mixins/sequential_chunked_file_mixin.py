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
"""Logic for validating and grouping chunked files that are sequentially numbered"""

import logging

from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)
from recidiviz.utils.types import assert_type


class SequentialChunkedFileMixin:
    """Mixin class for grouping chunked raw data files"""

    @classmethod
    def group_files_with_sequential_suffixes(
        cls,
        *,
        gcs_files: list[RawGCSFileMetadata],
        file_tag: str,
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Validates that |gcs_files| have numerically ascending file suffixes from 1
        to n.
        """
        if len(gcs_files) == 0:
            raise ValueError("Must provide at least one file to group; found none.")

        actual_suffixes: set[int] = set()
        for gcs_file in gcs_files:
            try:
                extension_int = int(assert_type(gcs_file.parts.filename_suffix, str))
                actual_suffixes.add(extension_int)
            except Exception as e:
                return [], [
                    RawDataFilesSkippedError.from_gcs_files_and_message(
                        skipped_message=(
                            f"Skipping grouping for [{file_tag}]; expected filename suffix "
                            f"for [{gcs_file.path.abs_path()}] to be integer-parseable but "
                            f"found [{gcs_file.parts.filename_suffix}] instead: \n {e}"
                        ),
                        gcs_files=gcs_files,
                        file_tag=file_tag,
                    )
                ]

        expected_suffixes = set(range(1, len(gcs_files) + 1))
        if actual_suffixes != expected_suffixes:
            extra_suffixes = actual_suffixes - expected_suffixes
            missing_suffixes = expected_suffixes - actual_suffixes
            return [], [
                RawDataFilesSkippedError.from_gcs_files_and_message(
                    skipped_message=(
                        f"Skipping grouping for [{file_tag}]; missing expected sequential "
                        f"suffixes [{missing_suffixes}] and/or found extra suffixes "
                        f"[{extra_suffixes}]"
                    ),
                    gcs_files=gcs_files,
                    file_tag=file_tag,
                )
            ]

        logging.info(
            "Found [%s] files on [%s] w/ ascending numerical suffixes, grouping...",
            len(gcs_files),
            gcs_files[0].parts.utc_upload_datetime.date(),
        )
        return [RawBigQueryFileMetadata.from_gcs_files(gcs_files=gcs_files)], []

    @classmethod
    def group_n_files_with_sequential_suffixes(
        cls,
        *,
        n: int,
        gcs_files: list[RawGCSFileMetadata],
        file_tag: str,
    ) -> tuple[list[RawBigQueryFileMetadata], list[RawDataFilesSkippedError]]:
        """Validates that |gcs_files| contains exactly |n| files that have numerically
        ascending file suffixes from 1 to n.
        """

        if n <= 0:
            raise ValueError(f"n must a positive integer; found [{n}]")

        if len(gcs_files) != n:
            return [], [
                RawDataFilesSkippedError.from_gcs_files_and_message(
                    skipped_message=(
                        f"Skipping grouping for [{file_tag}]; Expected [{n}] chunks, "
                        f"but found [{len(gcs_files)}]"
                    ),
                    gcs_files=gcs_files,
                    file_tag=file_tag,
                )
            ]

        logging.info(
            "Found [%s]/[%s] files on [%s]",
            len(gcs_files),
            n,
            gcs_files[0].parts.utc_upload_datetime.date(),
        )
        return cls.group_files_with_sequential_suffixes(
            gcs_files=gcs_files, file_tag=file_tag
        )
