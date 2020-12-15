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
Script for moving all files with an unspecified file type from a state's ingest bucket to the 'raw' subfolder
in the state's storage bucket in order to prepare for SQL-preprocessing launch. Adds the 'raw' file type to the
normalized file name of each file.

Example path transformation:
    gs://recidiviz-123-direct-ingest-state-us-nd/processed_2020-07-14T12:49:22:638466_elite_orderstable.csv ->
    gs://recidiviz-123-direct-ingest-state-storage/us_nd/raw/2020/07/14/processed_2020-07-14T12:49:22:638466_raw_elite_
    orderstable.csv

When run in dry-run mode (the default), will log the move of each file, but will not execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.move_prod_ingest_files_to_raw --region us_nd --dry-run True
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
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    to_normalized_processed_file_path_from_normalized_path, \
    to_normalized_unprocessed_file_path_from_normalized_path, DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_storage_directory_path_for_region, GcsfsDirectIngestFileType, \
    gcsfs_direct_ingest_directory_path_for_region, filename_parts_from_path
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tools.gsutil_shell_helpers import gsutil_mv, gsutil_ls
from recidiviz.utils.params import str_to_bool


# pylint: disable=not-callable


class MoveFilesFromProdIngestToRawController:
    """Class with functionality to move files from a state's prod ingest bucket to raw file storage."""

    def __init__(self,
                 region_code: str,
                 dry_run: bool, ):
        self.region_code = region_code
        self.file_type = GcsfsDirectIngestFileType.UNSPECIFIED
        self.dry_run = dry_run
        self.project_id = 'recidiviz-123'
        self.region_ingest_bucket_dir_path = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_directory_path_for_region(
                region_code, SystemLevel.STATE, project_id=self.project_id))
        self.region_storage_raw_dir_path = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code, SystemLevel.STATE, GcsfsDirectIngestFileType.RAW_DATA,
                project_id=self.project_id))
        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f'move_prod_ingest_files_to_raw_start_bound_{self.region_code}_region_dry_run_{dry_run}_'
            f'{datetime.datetime.now().isoformat()}.txt')
        self.mutex = threading.Lock()
        self.move_list: List[Tuple[str, str]] = []
        self.move_progress: Optional[Bar] = None

    def run(self) -> None:
        """Main function that will execute the move."""
        if self.dry_run:
            logging.info("[DRY RUN] Moving files from [%s] to [%s]", self.region_ingest_bucket_dir_path.abs_path(),
                         self.region_storage_raw_dir_path.abs_path())
        else:
            i = input(f"Moving files from [{self.region_ingest_bucket_dir_path.abs_path()}] to "
                      f"[{self.region_storage_raw_dir_path.abs_path()}] - continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        files_to_move = self._get_files_to_move()

        if self.dry_run:
            logging.info("[DRY RUN] Found [%d] files to move", len(files_to_move))

        else:
            i = input(
                f"Found [{len(files_to_move)}] files to move - continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        self._execute_move(files_to_move)
        self._write_move_to_log_file()

        if self.dry_run:
            logging.info("DRY RUN: See results in [%s].\n Rerun with [--dry-run False] to execute move.",
                         self.log_output_path)
        else:
            logging.info("Move complete! See results in [%s].\n", self.log_output_path)

    def _get_files_to_move(self) -> List[str]:
        return gsutil_ls('gs://'f'{self.region_ingest_bucket_dir_path.bucket_name}/*.csv')

    def _write_move_to_log_file(self) -> None:
        self.move_list.sort()
        with open(self.log_output_path, 'w') as f:
            if self.dry_run:
                template = "DRY RUN: Would move {} -> {}\n"
            else:
                template = "Moved {} -> {}\n"

            f.writelines(template.format(original_path, new_path) for original_path, new_path in self.move_list)

    def _move_files(self, from_uri: str) -> None:
        curr_gcsfs_file_path = GcsfsFilePath.from_absolute_path(from_uri)
        previous_date_format = filename_parts_from_path(curr_gcsfs_file_path).date_str
        new_date_format = date.fromisoformat(previous_date_format).strftime("%Y/%m/%d/")

        path_with_new_file_name = GcsfsFilePath.from_absolute_path(
            to_normalized_unprocessed_file_path_from_normalized_path(from_uri,
                                                                     GcsfsDirectIngestFileType.RAW_DATA))

        if DirectIngestGCSFileSystem.is_processed_file(curr_gcsfs_file_path):
            path_with_new_file_name = GcsfsFilePath.from_absolute_path(
                to_normalized_processed_file_path_from_normalized_path(from_uri,
                                                                       GcsfsDirectIngestFileType.RAW_DATA))

        raw_dir_with_date = GcsfsDirectoryPath.from_dir_and_subdir(self.region_storage_raw_dir_path, new_date_format)

        to_uri = GcsfsFilePath.from_directory_and_file_name(raw_dir_with_date, path_with_new_file_name.file_name).uri()

        if not self.dry_run:
            gsutil_mv(from_path=from_uri, to_path=to_uri)
        with self.mutex:
            self.move_list.append((from_uri, to_uri))
            if self.move_progress:
                self.move_progress.next()

    def _execute_move(self, files_to_move: List[str]) -> None:
        self.move_progress = Bar("Moving files from prod ingest bucket...", max=len(files_to_move))

        thread_pool = ThreadPool(processes=12)
        thread_pool.map(self._move_files, files_to_move)
        self.move_progress.finish()


def main() -> None:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--region', required=True, help='E.g. \'us_nd\'')

    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs move in dry-run mode, only prints the file moves it would do.')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    MoveFilesFromProdIngestToRawController(
        region_code=args.region,
        dry_run=args.dry_run).run()


if __name__ == '__main__':
    main()
