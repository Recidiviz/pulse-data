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
import itertools
import logging
import os
import threading
from collections import defaultdict
from enum import Enum
from multiprocessing.pool import ThreadPool
from typing import Callable, List, Optional, Tuple

import attr
from progress.bar import Bar

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common import attr_validators
from recidiviz.tools.gsutil_shell_helpers import (
    GSUTIL_DEFAULT_TIMEOUT_SEC,
    gsutil_cp,
    gsutil_get_storage_subdirs_containing_raw_files,
    gsutil_mv,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results
from recidiviz.utils.log_helpers import make_log_output_path


class IngestFilesOperationType(Enum):
    COPY = "COPY"
    MOVE = "MOVE"

    def present_participle(self) -> str:
        return self.value.rstrip("E") + "ING"

    def past_participle(self) -> str:
        return self.value.replace("Y", "I").rstrip("E") + "ED"


# TODO(#37517) make start_date_bound -> state_datetime_inclusive and make it datetime | None instead of string
# TODO(#37517) in _do_file_operation filter by datetime too
@attr.define
class OperateOnRawStorageDirectoriesController:
    """Class with functionality to copy or move raw directories consumed by ingest from
    storage buckets across different paths, with options of applying filters on update_date
    and file tag names.

    Attributes:
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

        mutex - lock to ensure thread safety
        operations_list - list of tuples representing the from_path and to_path of the operations
        file_operation_progress - progress bar to show progress of file operations
    """

    operation_type: IngestFilesOperationType = attr.field(
        validator=attr.validators.instance_of(IngestFilesOperationType)
    )
    source_region_storage_dir_path: GcsfsDirectoryPath = attr.field(
        validator=attr.validators.instance_of(GcsfsDirectoryPath)
    )
    destination_region_storage_dir_path: GcsfsDirectoryPath = attr.field(
        validator=attr.validators.instance_of(GcsfsDirectoryPath)
    )
    start_date_bound: Optional[str] = attr.field(validator=attr_validators.is_opt_str)
    end_date_bound: Optional[str] = attr.field(validator=attr_validators.is_opt_str)
    file_tag_filters: Optional[List[str]] = attr.field(
        validator=attr_validators.is_opt_list
    )
    file_tag_regex: Optional[str] = attr.field(validator=attr_validators.is_opt_str)
    log_output_path: str = attr.field(validator=attr_validators.is_str)
    dry_run: bool = attr.field(validator=attr_validators.is_bool)

    mutex: threading.Lock = attr.ib(factory=threading.Lock, init=False)
    operations_list: List[Tuple[str, str]] = attr.ib(factory=list, init=False)
    file_operation_progress: Optional[Bar] = attr.ib(default=None, init=False)

    def __attrs_post_init__(self) -> None:
        if self.file_tag_filters and self.file_tag_regex:
            raise ValueError("Cannot have both file_tag_filter and file_tag_regex")

    @classmethod
    def create_controller(
        cls,
        *,
        region_code: str,
        operation_type: IngestFilesOperationType,
        source_region_storage_dir_path: GcsfsDirectoryPath,
        destination_region_storage_dir_path: GcsfsDirectoryPath,
        dry_run: bool,
        start_date_bound: Optional[str] = None,
        end_date_bound: Optional[str] = None,
        file_tags: Optional[List[str]] = None,
        file_tag_regex: Optional[str] = None,
    ) -> "OperateOnRawStorageDirectoriesController":
        """Creates a controller responsible for copying or moving ingest files from
        storage buckets across different paths."""
        file_tag_filters = (
            [f"*_{file_tag}[.]*" for file_tag in file_tags] if file_tags else None
        )
        file_tag_regex = f"*{file_tag_regex.strip('*')}*" if file_tag_regex else None

        log_output_path = make_log_output_path(
            operation_name=f"{operation_type.value.lower()}_storage_raw_files",
            region_code=region_code,
            date_string=f"start_bound_{start_date_bound}_end_bound_{end_date_bound}",
            dry_run=dry_run,
        )
        return cls(
            operation_type=operation_type,
            source_region_storage_dir_path=source_region_storage_dir_path,
            destination_region_storage_dir_path=destination_region_storage_dir_path,
            start_date_bound=start_date_bound,
            end_date_bound=end_date_bound,
            file_tag_filters=file_tag_filters,
            file_tag_regex=file_tag_regex,
            log_output_path=log_output_path,
            dry_run=dry_run,
        )

    @property
    def file_operation_fn(self) -> Callable:
        if self.operation_type == IngestFilesOperationType.COPY:
            return gsutil_cp
        if self.operation_type == IngestFilesOperationType.MOVE:
            return gsutil_mv
        raise ValueError(f"Unsupported operation type: {self.operation_type}")

    def run(self) -> None:
        """Main function that will execute the copy/move."""
        logging.basicConfig(level=logging.INFO)
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
        if not subdirs_to_operate_on:
            logging.info(
                "No subdirectories found to %s.", self.operation_type.value.lower()
            )
            return

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
        # TODO(#45991) deprecate in favor of get_storage_directories_containing_raw_files
        return gsutil_get_storage_subdirs_containing_raw_files(
            storage_bucket_path=self.source_region_storage_dir_path,
            upper_bound_date=self.end_date_bound,
            lower_bound_date=self.start_date_bound,
            file_filters=(
                [self.file_tag_regex] if self.file_tag_regex else self.file_tag_filters
            ),
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
        # If we're filtering for certain tags, it's expected that we might find some
        # directories with no results.
        allow_empty = bool(self.file_tag_filters)
        if not self.dry_run:
            from_path, to_path = operation_paths
            logging.debug(
                "%s FROM %s TO %s",
                self.operation_type.present_participle().capitalize(),
                from_path,
                to_path,
            )
            self.file_operation_fn(
                from_path=from_path, to_path=to_path, allow_empty=allow_empty
            )

    def _get_operations_for_subdir(self, subdir_path_str: str) -> List[Tuple[str, str]]:
        """Returns a list of (from_path, to_path) tuples representing operations
        that should be done against files in this subdirectory.
        """
        subdir_path = GcsfsDirectoryPath.from_absolute_path(subdir_path_str)

        from_path = subdir_path.uri()
        relative_to_source = os.path.relpath(
            subdir_path.uri(), self.source_region_storage_dir_path.uri()
        )

        to_path = GcsfsDirectoryPath.from_dir_and_subdir(
            self.destination_region_storage_dir_path,
            relative_to_source,
        ).uri()

        if self.file_tag_regex:
            return [(from_path + self.file_tag_regex, to_path)]

        if self.file_tag_filters:
            return [
                (
                    from_path + file_tag_filter,
                    to_path,
                )
                for file_tag_filter in self.file_tag_filters
            ]

        return [(from_path + "*", to_path)]

    def _execute_file_operations(self, subdirs_to_operate_on: List[str]) -> None:
        thread_pool = ThreadPool(processes=12)
        operations_lists = thread_pool.map(
            self._get_operations_for_subdir, subdirs_to_operate_on
        )

        all_operations = list(itertools.chain(*operations_lists))
        logging.debug("ALL OPERATIONS: %s", all_operations)
        dry_run_str = "[DRY_RUN] " if self.dry_run else ""
        progress_bar_message = f"{dry_run_str}{self.operation_type.present_participle().capitalize()} files from subdirectories..."
        result = map_fn_with_progress_bar_results(
            work_items=all_operations,
            work_fn=self._do_file_operation,
            progress_bar_message=progress_bar_message,
            # Add a timeout that is generously longer than the timeout for the
            # GSUTIL copy call
            single_work_item_timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC + 10,
            # 4 hour timeout
            overall_timeout_sec=60 * 60 * 4,
        )

        self.operations_list.extend([operation for operation, _ in result.successes])
