# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements a controller used to copy or move files used in ingest across buckets."""
import datetime
import itertools
import logging
import os
import threading
from collections import defaultdict
from enum import Enum
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Tuple

from progress.bar import Bar

from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsDirectoryPath,
    normalize_relative_path,
)
from recidiviz.tools.gsutil_shell_helpers import (
    gsutil_cp,
    gsutil_get_storage_subdirs_containing_raw_files,
    gsutil_mv,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation


class IngestFilesOperationType(Enum):
    COPY = "COPY"
    MOVE = "MOVE"

    def gerund(self) -> str:
        return self.value.rstrip("E") + "ING"


class OperateOnStorageRawFilesController:
    """Class with functionality to copy or move raw files consumed by ingest from
    storage buckets across different paths."""

    def __init__(
        self,
        *,
        region_code: str,
        operation_type: IngestFilesOperationType,
        source_region_storage_dir_path: GcsfsDirectoryPath,
        destination_region_storage_dir_path: GcsfsDirectoryPath,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        file_tag_filters: List[str],
        dry_run: bool,
    ):
        """Creates a controller responsible for copying or moving ingest files from
        storage buckets across different paths.

        Args:
        region_code - region code where this is run (only used in the output log)
        operation_type - Whether to perform a copy or to fully move the files.
        source_region_storage_dir_path - root path where the files to be copied/moved
            live. Critically, this must be an ingest storage buckets, e.g.
            `gs://recidiviz-staging-direct-ingest-state-storage/us_xx/`
        destination_region_storage_dir_path - root path where the files should be
            copied/moved to. Critically, this must be an ingest storage buckets, e.g.
            `gs://recidiviz-staging-direct-ingest-state-storage/us_xx/`, or must have
            the same internal structure as the ingest storage buckets, e.g. the
            deprecated/ folder in the ingest buckets.
        start_date_bound - optional start date in the format 1901-02-28
        end_date_bound - optional end date in the format 1901-02-28
        dry_run - whether or not to run in dry-run mode
        """
        self.operation_type = operation_type
        self.source_region_storage_dir_path = source_region_storage_dir_path
        self.destination_region_storage_dir_path = destination_region_storage_dir_path
        self.dry_run = dry_run
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.file_tag_filters = file_tag_filters

        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f"{self.operation_type.value.lower()}_storage_raw_files_result_"
            f"{region_code}_start_bound_{self.start_date_bound}_end_bound_"
            f"{self.end_date_bound}_dry_run_{dry_run}_"
            f"{datetime.datetime.now().isoformat()}.txt",
        )
        self.mutex = threading.Lock()
        # List of (from_path, to_path) tuples for each copy or move operation
        self.operations_list: List[Tuple[str, str]] = []
        self.file_operation_progress: Optional[Bar] = None

    def run(self) -> None:
        """Main function that will execute the copy/move."""
        prompt_for_confirmation(
            f"Will {self.operation_type.value.lower()} files from "
            f"[{self.source_region_storage_dir_path.abs_path()}] to "
            f"[{self.destination_region_storage_dir_path.abs_path()}] - continue?",
            dry_run=self.dry_run,
        )

        logging.info(
            "Collecting subdirectories to %s...", self.operation_type.value.lower()
        )
        subdirs_to_operate_on = self._get_subdirs_to_operate_on()

        prompt_for_confirmation(
            f"Found [{len(subdirs_to_operate_on)}] subdirectories to "
            f"{self.operation_type.value.lower()} - continue?",
            dry_run=self.dry_run,
        )

        # To avoid potential conflict with copies or moves that involve nested
        # subdirectories, we move all lower-level subdirectories first, before their
        # parent directories.
        bucket_by_depth = defaultdict(list)
        for subdir in subdirs_to_operate_on:
            bucket_by_depth[len(subdir.split("/"))].append(subdir)

        for i, (_, subdirs_at_depth) in enumerate(
            sorted(bucket_by_depth.items(), reverse=True)
        ):
            logging.info(
                "Processing subdirectory batch [%s/%s]", i + 1, len(bucket_by_depth)
            )
            self._execute_file_operations(subdirs_at_depth)

        self._write_copies_to_log_file()

        if self.dry_run:
            logging.info(
                "[DRY RUN] See results in [%s].\n"
                "Rerun with [--dry-run False] to execute %s.",
                self.log_output_path,
                self.operation_type.value.lower(),
            )
        else:
            logging.info(
                "%s complete! See results in [%s].\n",
                self.operation_type.value.capitalize(),
                self.log_output_path,
            )

    def _get_subdirs_to_operate_on(self) -> List[str]:
        return gsutil_get_storage_subdirs_containing_raw_files(
            storage_bucket_path=self.source_region_storage_dir_path.abs_path(),
            upper_bound_date=self.end_date_bound,
            lower_bound_date=self.start_date_bound,
        )

    def _write_copies_to_log_file(self) -> None:
        self.operations_list.sort()
        with open(self.log_output_path, "w", encoding="utf-8") as f:
            if self.dry_run:
                prefix = "[DRY RUN] Would"
            else:
                prefix = "Did"

            f.writelines(
                f"{prefix} {self.operation_type.value.lower()} {original_path} -> {new_path}\n"
                for original_path, new_path in self.operations_list
            )

    def _do_file_operation(self, operation_paths: Tuple[str, str]) -> None:
        from_path = operation_paths[0]
        to_path = operation_paths[1]
        # If we're filtering for certain tags, it's expected that we might find some
        # directories with no results.
        allow_empty = bool(self.file_tag_filters)
        if not self.dry_run:
            if self.operation_type == IngestFilesOperationType.COPY:
                gsutil_cp(from_path=from_path, to_path=to_path, allow_empty=allow_empty)
            elif self.operation_type == IngestFilesOperationType.MOVE:
                gsutil_mv(from_path=from_path, to_path=to_path, allow_empty=allow_empty)
            else:
                raise ValueError(f"Unexpected operation type [{self.operation_type}] ")
        with self.mutex:
            self.operations_list.append((from_path, to_path))
            if self.file_operation_progress:
                self.file_operation_progress.next()

    def _get_operations_for_subdir(self, subdir_path_str: str) -> List[Tuple[str, str]]:
        """Returns a list of (from_path, to_path) tuples representing operations
        that should be done against files in this subdirectory.
        """
        subdir_path = GcsfsDirectoryPath.from_absolute_path(subdir_path_str)

        from_path = subdir_path.uri() + "*"
        relative_to_source = os.path.relpath(
            subdir_path.uri(), self.source_region_storage_dir_path.uri()
        )

        to_path = GcsfsDirectoryPath.from_dir_and_subdir(
            self.destination_region_storage_dir_path,
            normalize_relative_path(relative_to_source),
        ).uri()

        if not self.file_tag_filters:
            return [(from_path, to_path)]

        operations = []
        for file_tag in self.file_tag_filters:
            # In all other files, the split file is followed by the extension.
            tag_filter = f"*_{file_tag}[.]*"

            operations.append(
                (
                    from_path.rstrip("*") + tag_filter,
                    to_path,
                )
            )
        return operations

    def _execute_file_operations(self, subdirs_to_operate_on: List[str]) -> None:
        thread_pool = ThreadPool(processes=12)
        operations_lists = thread_pool.map(
            self._get_operations_for_subdir, subdirs_to_operate_on
        )

        all_operations = list(itertools.chain(*operations_lists))
        self.file_operation_progress = Bar(
            f"{self.operation_type.gerund().capitalize()} files from subdirectories...",
            max=len(all_operations),
        )
        thread_pool.map(self._do_file_operation, all_operations)
        self.file_operation_progress.finish()
