# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
Script for moving all unspecified files in state storage to the proper location for SQL pre-processing and
format them appropriately in the raw folder.

When run in dry-run mode (the default), will log the move of each file, but will not execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.move_storage_files_from_unspecified_to_raw --region us_nd --start-date-bound 2019-08-12 \
--end-date-bound 2019-08-17 --project-id recidiviz-123 --dry-run True
"""
import argparse
import datetime
import logging
import os
import threading
from multiprocessing.pool import ThreadPool
from typing import List, Tuple, Optional

from datetime import date

from progress.bar import Bar

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    to_normalized_processed_file_path_from_normalized_path,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    gcsfs_direct_ingest_storage_directory_path_for_region,
    GcsfsDirectIngestFileType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tools.gsutil_shell_helpers import (
    gsutil_get_storage_subdirs_containing_file_types,
    gsutil_mv,
    gsutil_ls,
)
from recidiviz.utils.params import str_to_bool


# pylint: disable=not-callable


class MoveFilesFromUnspecifiedToRawController:
    """Class with functionality to move files from unspecified to raw with proper format."""

    def __init__(
        self,
        region_code: str,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        dry_run: bool,
        project_id: str,
    ):
        self.region_code = region_code
        self.file_type = GcsfsDirectIngestFileType.UNSPECIFIED
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.dry_run = dry_run
        self.project_id = project_id
        self.region_storage_dir_path = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code, SystemLevel.STATE, project_id=self.project_id
            )
        )
        self.region_storage_raw_dir_path = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code,
                SystemLevel.STATE,
                GcsfsDirectIngestFileType.RAW_DATA,
                project_id=self.project_id,
            )
        )
        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f"move_storage_files_from_unspecified_to_raw_start_bound_{self.region_code}_region_{self.start_date_bound}"
            f"_end_bound_{self.end_date_bound}_dry_run_{dry_run}_{datetime.datetime.now().isoformat()}.txt",
        )
        self.mutex = threading.Lock()
        self.move_list: List[Tuple[str, str]] = []
        self.move_progress: Optional[Bar] = None

    def run(self) -> None:
        """Main function that will execute the move."""
        if self.dry_run:
            logging.info(
                "[DRY RUN] Moving files from [%s] to " "[%s]",
                self.region_storage_dir_path.abs_path(),
                self.region_storage_raw_dir_path.abs_path(),
            )

        else:
            i = input(
                f"Moving files from [{self.region_storage_dir_path.abs_path()}] to "
                f"[{self.region_storage_raw_dir_path.abs_path()}] - continue? [y/n]: "
            )

            if i.upper() != "Y":
                return

        subdirs_to_move = self._get_subdirs_to_move()

        if self.dry_run:
            logging.info(
                "[DRY RUN] Found [%d] subdirectories to move", len(subdirs_to_move)
            )

        else:
            i = input(
                f"Found [{len(subdirs_to_move)}] subdirectories to move - "
                f"continue? [y/n]: "
            )

            if i.upper() != "Y":
                return

        self._execute_move(subdirs_to_move)
        self._write_move_to_log_file()

        if self.dry_run:
            logging.info(
                "DRY RUN: See results in [%s].\n"
                "Rerun with [--dry-run False] to execute move.",
                self.log_output_path,
            )
        else:
            logging.info("Move complete! See results in [%s].\n", self.log_output_path)

    def _get_subdirs_to_move(self) -> List[str]:
        return gsutil_get_storage_subdirs_containing_file_types(
            storage_bucket_path=self.region_storage_dir_path.abs_path(),
            file_type=self.file_type,
            upper_bound_date=self.end_date_bound,
            lower_bound_date=self.start_date_bound,
        )

    def _write_move_to_log_file(self) -> None:
        self.move_list.sort()
        with open(self.log_output_path, "w") as f:
            if self.dry_run:
                template = "DRY RUN: Would move {} -> {}\n"
            else:
                template = "Moved {} -> {}\n"

            f.writelines(
                template.format(original_path, new_path)
                for original_path, new_path in self.move_list
            )

    def _move_files_for_date(self, subdir_path_str: str) -> None:
        """Function that loops through each subdirectory and moves files in each subdirectory using the from path
        and to path specified."""

        from_dir_path = GcsfsDirectoryPath.from_absolute_path(
            subdir_path_str.rstrip("/")
        )

        previous_date_format = from_dir_path.relative_path.rstrip("/").split("/")[-1]
        new_date_format = date.fromisoformat(previous_date_format).strftime("%Y/%m/%d/")

        from_paths = gsutil_ls(f"{subdir_path_str}*.csv")
        for from_path in from_paths:
            file_name = GcsfsFilePath(
                bucket_name=self.region_storage_dir_path.bucket_name,
                blob_name=from_path,
            ).file_name

            to_file_path = os.path.join(
                "gs://",
                self.region_storage_dir_path.bucket_name,
                self.region_code,
                GcsfsDirectIngestFileType.RAW_DATA.value,
                new_date_format,
                file_name,
            )

            normalized_to_file_path = (
                to_normalized_processed_file_path_from_normalized_path(
                    to_file_path, file_type_override=GcsfsDirectIngestFileType.RAW_DATA
                )
            )

            to_path = normalized_to_file_path

            if not self.dry_run:
                gsutil_mv(from_path=from_path, to_path=to_path)
            with self.mutex:
                self.move_list.append((from_path, to_path))

        if self.move_progress:
            self.move_progress.next()

    def _execute_move(self, subdirs_to_move: List[str]) -> None:
        self.move_progress = Bar(
            "Moving files from subdirectories...", max=len(subdirs_to_move)
        )

        thread_pool = ThreadPool(processes=12)
        thread_pool.map(self._move_files_for_date, subdirs_to_move)
        self.move_progress.finish()


def main() -> None:
    """Executes the main flow of the script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--region", required=True, help="E.g. 'us_nd'")

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs move in dry-run mode, only prints the file moves it would do.",
    )

    parser.add_argument(
        "--start-date-bound",
        help="The lower bound date to start from, inclusive. For partial moving of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--end-date-bound",
        help="The upper bound date to end at, inclusive. For partial moving of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--project-id", help="The id for this particular project, E.g. 'recidiviz-123'"
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    MoveFilesFromUnspecifiedToRawController(
        region_code=args.region,
        start_date_bound=args.start_date_bound,
        end_date_bound=args.end_date_bound,
        project_id=args.project_id,
        dry_run=args.dry_run,
    ).run()


if __name__ == "__main__":
    main()
