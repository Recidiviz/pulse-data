# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""
Script for copying all files in production storage for a region to state storage for a region. Should be used when we
want to rerun ingest for a state in staging.

When run in dry-run mode (the default), will only log copies, but will not execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.copy_state_files_from_prod_to_staging_storage --file-type raw \
    --region us_nd --start-date-bound 2019-08-12 --end-date-bound 2019-08-17 --dry-run True
"""
import argparse
import datetime
import logging
import os
import threading
from multiprocessing.pool import ThreadPool
from typing import List, Tuple, Optional

from progress.bar import Bar

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    gcsfs_direct_ingest_storage_directory_path_for_region,
    GcsfsDirectIngestFileType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.tools.gsutil_shell_helpers import (
    gsutil_cp,
    gsutil_get_storage_subdirs_containing_file_types,
)
from recidiviz.utils.params import str_to_bool

# pylint: disable=not-callable


class CopyFilesFromProdToStagingController:
    """Class with functionality to copy files between prod and staging storage."""

    def __init__(
        self,
        region_code: str,
        file_type: GcsfsDirectIngestFileType,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        dry_run: bool,
    ):
        self.file_type = file_type
        self.prod_region_storage_dir_path = (
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code,
                SystemLevel.STATE,
                project_id="recidiviz-123",
            )
        )
        self.staging_region_storage_dir_path = (
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code,
                SystemLevel.STATE,
                project_id="recidiviz-staging",
            )
        )
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
                self.prod_region_storage_dir_path.abs_path(),
                self.staging_region_storage_dir_path.abs_path(),
            )
        else:
            i = input(
                f"Copying files from [{self.prod_region_storage_dir_path.abs_path()}] to "
                f"[{self.staging_region_storage_dir_path.abs_path()}] - continue? [y/n]: "
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
            storage_bucket_path=self.prod_region_storage_dir_path.abs_path(),
            file_type=self.file_type,
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

        from_path = f"gs://{self.prod_region_storage_dir_path.bucket_name}/{dir_path.relative_path}*"
        to_path = f"gs://{self.staging_region_storage_dir_path.bucket_name}/{dir_path.relative_path}"

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


def main() -> None:
    """Executes the main flow of the script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--region", required=True, help="E.g. 'us_nd'")

    parser.add_argument(
        "--file-type",
        required=True,
        choices=[file_type.value for file_type in GcsfsDirectIngestFileType],
        help="Defines whether we should move raw files or generated ingest_view files",
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs copy in dry-run mode, only prints the file copies it would do.",
    )

    parser.add_argument(
        "--start-date-bound",
        help="The lower bound date to start from, inclusive. For partial copying of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--end-date-bound",
        help="The upper bound date to end at, inclusive. For partial copying of ingested files. "
        "E.g. 2019-09-23.",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    CopyFilesFromProdToStagingController(
        region_code=args.region,
        file_type=GcsfsDirectIngestFileType(args.file_type),
        start_date_bound=args.start_date_bound,
        end_date_bound=args.end_date_bound,
        dry_run=args.dry_run,
    ).run()


if __name__ == "__main__":
    main()
