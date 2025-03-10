# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Implements a controller used to move raw files matching the provided filters from the ingest bucket to the
deprecated storage directory for a given state."""
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

import attr

from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import is_datetime_in_opt_range
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_deprecated_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.gsutil_shell_helpers import gsutil_ls, gsutil_mv
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.log_helpers import make_log_output_path

MAX_THREADS = 12


@attr.define
class MoveIngestBucketRawFilesToDeprecatedController:
    """Class responsible for moving raw files matching the provided filters from the ingest bucket to the
    deprecated storage directory for a given state."""

    source_ingest_bucket: GcsfsBucketPath
    destination_region_deprecated_storage_dir_path: GcsfsDirectoryPath
    start_datetime_inclusive: Optional[datetime.datetime]
    end_datetime_exclusive: Optional[datetime.datetime]
    file_tag_filters: Optional[List[str]]
    log_output_path: str
    dry_run: bool

    def __attrs_post_init__(self) -> None:
        if (
            self.start_datetime_inclusive is not None
            and self.end_datetime_exclusive is not None
            and self.start_datetime_inclusive == self.end_datetime_exclusive
        ):
            raise ValueError(
                f"start_datetime_inclusive and end_datetime_exclusive are both set to [{self.end_datetime_exclusive}] which will exclude all files; please provide two distinct values"
            )

        if (
            self.start_datetime_inclusive is not None
            and self.end_datetime_exclusive is not None
            and self.start_datetime_inclusive > self.end_datetime_exclusive
        ):
            raise ValueError(
                f"start_datetime_inclusive [{self.start_datetime_inclusive}] is after than end_datetime_exclusive [{self.end_datetime_exclusive}]; please specify a start value that is before the provided end time"
            )

    @classmethod
    def create_controller(
        cls,
        *,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        project_id: str,
        start_datetime_inclusive: Optional[datetime.datetime] = None,
        end_datetime_exclusive: Optional[datetime.datetime] = None,
        file_tag_filters: Optional[List[str]] = None,
        dry_run: bool,
    ) -> "MoveIngestBucketRawFilesToDeprecatedController":
        """Creates a controller responsible for moving raw files from the
        ingest bucket to the deprecated storage directory for a given state.
        """
        return cls(
            source_ingest_bucket=gcsfs_direct_ingest_bucket_for_state(
                region_code=state_code.value,
                ingest_instance=ingest_instance,
                project_id=project_id,
            ),
            destination_region_deprecated_storage_dir_path=(
                gcsfs_direct_ingest_deprecated_storage_directory_path_for_state(
                    region_code=state_code.value,
                    ingest_instance=ingest_instance,
                    deprecated_on_date=datetime.date.today(),
                    project_id=project_id,
                )
            ),
            start_datetime_inclusive=start_datetime_inclusive,
            end_datetime_exclusive=end_datetime_exclusive,
            file_tag_filters=file_tag_filters,
            dry_run=dry_run,
            log_output_path=make_log_output_path(
                operation_name="move_ingest_bucket_raw_files",
                region_code=state_code.value,
                date_string=f"start_bound_{start_datetime_inclusive}_end_bound_{end_datetime_exclusive}",
                dry_run=dry_run,
            ),
        )

    @property
    def destination_region_storage_uri(self) -> str:
        return self.destination_region_deprecated_storage_dir_path.uri()

    def _write_log_file(
        self, files_moved: List[GcsfsFilePath], failed_files: List[GcsfsFilePath]
    ) -> None:
        with open(self.log_output_path, "w", encoding="utf-8") as f:
            if self.dry_run:
                f.write("[DRY RUN] Would move the following files:\n")
            else:
                f.write("Moved the following files:\n")

            f.writelines(
                f"    {f.uri()} -> {self.destination_region_storage_uri}\n"
                for f in files_moved
            )
            if failed_files:
                f.write("Failed to move the following files:\n")
                f.writelines(f"    {f.uri()}\n" for f in failed_files)

    def _move_file(self, raw_file: GcsfsFilePath) -> None:
        if not self.dry_run:
            gsutil_mv(
                from_path=raw_file.uri(),
                to_path=self.destination_region_storage_uri,
                allow_empty=False,
            )

    def _file_matches_filter(self, raw_file: GcsfsFilePath) -> bool:
        try:
            file_parts = filename_parts_from_path(raw_file)
        except ValueError:
            # Skip files that don't match the expected raw file format
            return False

        if self.file_tag_filters and file_parts.file_tag not in self.file_tag_filters:
            return False

        return is_datetime_in_opt_range(
            file_parts.utc_upload_datetime,
            start_datetime_inclusive=self.start_datetime_inclusive,
            end_datetime_exclusive=self.end_datetime_exclusive,
        )

    def _find_ingest_bucket_raw_files(
        self,
    ) -> List[GcsfsFilePath]:
        files_in_bucket = gsutil_ls(
            self.source_ingest_bucket.uri(), allow_empty=True, files_only=True
        )
        gcsfs_paths = [GcsfsFilePath.from_absolute_path(f) for f in files_in_bucket]

        return [
            raw_file for raw_file in gcsfs_paths if self._file_matches_filter(raw_file)
        ]

    def run(self) -> Tuple[List[GcsfsFilePath], List[GcsfsFilePath]]:
        """
        Moves raw files matching the class's filters from the ingest bucket to the deprecated storage directory
        for a given state. Returns a tuple of files that were successfully moved and files that failed to move.
        """
        prompt_for_confirmation(
            f"Will move files from "
            f"[{self.source_ingest_bucket.abs_path()}] to "
            f"[{self.destination_region_deprecated_storage_dir_path.abs_path()}] - continue?",
            dry_run=self.dry_run,
        )

        gcsfs_files_to_operate_on = self._find_ingest_bucket_raw_files()
        if not gcsfs_files_to_operate_on:
            logging.info("No files found to move.")
            return [], []

        prompt_for_confirmation(
            f"Found [{len(gcsfs_files_to_operate_on)}] files to move - continue?",
            dry_run=self.dry_run,
        )

        failed_gcsfs_files = []
        successful_gcsfs_files = []
        with ThreadPoolExecutor(
            max_workers=min(MAX_THREADS, len(gcsfs_files_to_operate_on))
        ) as executor:
            future_to_raw_file = {
                executor.submit(self._move_file, raw_file): raw_file
                for raw_file in gcsfs_files_to_operate_on
            }

            for future in as_completed(future_to_raw_file):
                raw_file = future_to_raw_file[future]
                try:
                    future.result()
                    successful_gcsfs_files.append(raw_file)
                except Exception as e:
                    print(f"Error processing file {raw_file}: {e}")
                    failed_gcsfs_files.append(raw_file)

        self._write_log_file(successful_gcsfs_files, failed_gcsfs_files)

        if self.dry_run:
            logging.info(
                "[DRY RUN] See results in [%s].\n"
                "Rerun with [--dry-run False] to execute move.",
                self.log_output_path,
            )
        else:
            logging.info(
                "Move complete! See results in [%s].\n",
                self.log_output_path,
            )

        return successful_gcsfs_files, failed_gcsfs_files
