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
"""Implements a controller used to copy files used in ingest across buckets."""
import datetime
import logging
import os
import threading
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Tuple

from progress.bar import Bar

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
)
from recidiviz.tools.gsutil_shell_helpers import (
    gsutil_cp,
    gsutil_get_storage_subdirs_containing_file_types,
)


class CopyStorageIngestFilesController:
    """Class with functionality to copy ingest files from storage buckets
    across different paths."""

    def __init__(
        self,
        *,
        region_code: str,
        source_region_storage_dir_path: GcsfsDirectoryPath,
        destination_region_storage_dir_path: GcsfsDirectoryPath,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        dry_run: bool,
    ):
        """Creates a controller responsible for copying ingest files from storage buckets
        across different paths.

        Args:
        region_code - region code where this is run (only used in the output log)
        source_region_storage_dir_path - root path where the files to be moved live. Critically,
            this must be an ingest storage buckets, e.g. `gs://recidiviz-staging-direct-ingest-state-storage/us_xx/`
        destination_region_storage_dir_path - root path where the files should be moved to
            Critically, this must be an ingest storage buckets, e.g.
            `gs://recidiviz-staging-direct-ingest-state-storage/us_xx/`
        start_date_bound - optional start date in the format 1901-02-28
        end_date_bound - optional end date in the format 1901-02-28
        dry_run - whether or not to run in dry-run mode
        """
        self.source_region_storage_dir_path = source_region_storage_dir_path
        self.destination_region_storage_dir_path = destination_region_storage_dir_path
        self.dry_run = dry_run
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound

        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f"copy_prod_to_staging_result_{region_code}_start_bound_{self.start_date_bound}_end_bound_"
            f"{self.end_date_bound}_dry_run_{dry_run}_{datetime.datetime.now().isoformat()}.txt",
        )
        self.mutex = threading.Lock()
        self.copy_list: List[Tuple[str, str]] = []
        self.copy_progress: Optional[Bar] = None

    def run(self) -> None:
        """Main function that will execute the copy."""
        if self.dry_run:
            logging.info(
                "[DRY RUN] Copying files from [%s] to [%s]",
                self.source_region_storage_dir_path.abs_path(),
                self.destination_region_storage_dir_path.abs_path(),
            )
        else:
            i = input(
                f"Copying files from [{self.source_region_storage_dir_path.abs_path()}] to "
                f"[{self.destination_region_storage_dir_path.abs_path()}] - continue? [y/n]: "
            )

            if i.upper() != "Y":
                return

        subdirs_to_copy = self._get_subdirs_to_copy()

        if self.dry_run:
            logging.info(
                "[DRY RUN] Found [%d] subdirectories to copy", len(subdirs_to_copy)
            )

        else:
            i = input(
                f"Found [{len(subdirs_to_copy)}] subdirectories to copy - "
                f"continue? [y/n]: "
            )

            if i.upper() != "Y":
                return

        self._execute_copy(subdirs_to_copy)
        self._write_copies_to_log_file()

        if self.dry_run:
            logging.info(
                "DRY RUN: See results in [%s].\n"
                "Rerun with [--dry-run False] to execute copy.",
                self.log_output_path,
            )
        else:
            logging.info("Copy complete! See results in [%s].\n", self.log_output_path)

    def _get_subdirs_to_copy(self) -> List[str]:
        return gsutil_get_storage_subdirs_containing_file_types(
            storage_bucket_path=self.source_region_storage_dir_path.abs_path(),
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
            upper_bound_date=self.end_date_bound,
            lower_bound_date=self.start_date_bound,
        )

    def _write_copies_to_log_file(self) -> None:
        self.copy_list.sort()
        with open(self.log_output_path, "w") as f:
            if self.dry_run:
                template = "DRY RUN: Would copy {} -> {}\n"
            else:
                template = "Copied {} -> {}\n"

            f.writelines(
                template.format(original_path, new_path)
                for original_path, new_path in self.copy_list
            )

    def _copy_files_for_date(self, subdir_path_str: str) -> None:
        dir_path = GcsfsDirectoryPath.from_absolute_path(subdir_path_str.rstrip("/"))

        from_path = f"gs://{self.source_region_storage_dir_path.bucket_name}/{dir_path.relative_path}*"
        to_path = f"gs://{self.destination_region_storage_dir_path.bucket_name}/{dir_path.relative_path}"

        if not self.dry_run:
            gsutil_cp(from_path=from_path, to_path=to_path)
        with self.mutex:
            self.copy_list.append((from_path, to_path))
            if self.copy_progress:
                self.copy_progress.next()

    def _execute_copy(self, subdirs_to_copy: List[str]) -> None:
        self.copy_progress = Bar(
            "Copying files from subdirectories...", max=len(subdirs_to_copy)
        )

        thread_pool = ThreadPool(processes=12)
        thread_pool.map(self._copy_files_for_date, subdirs_to_copy)
        self.copy_progress.finish()
