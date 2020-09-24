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
Script for moving files in state storage to the deprecated folder in state storage.

Example path transformation:
gs://recidiviz-123-direct-ingest-state-storage/us_nd/raw/2019/08/12/
unprocessed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv ->
gs://recidiviz-123-direct-ingest-state-storage/us_nd/deprecated/deprecated_on_2020-07-22/raw/2019/08/12/
unprocessed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv

When run in dry-run mode (the default), will log the move of each file, but will not execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.move_storage_files_to_deprecated --file-type raw --region us_nd --start-date-bound \
2019-08-12 --end-date-bound 2019-08-17 --project-id recidiviz-staging --file-filter "docstars" --dry-run True


"""
import argparse
import datetime
import logging
import os
import re
import threading
from multiprocessing.pool import ThreadPool
from typing import List, Tuple, Optional

from datetime import date

from progress.bar import Bar

from recidiviz.common.ingest_metadata import SystemLevel

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_storage_directory_path_for_region, GcsfsDirectIngestFileType, filename_parts_from_path
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tools.gsutil_shell_helpers import gsutil_mv, gsutil_ls, \
    gsutil_get_storage_subdirs_containing_file_types
from recidiviz.tools.utils import INGESTED_FILE_REGEX
from recidiviz.utils.params import str_to_bool


# pylint: disable=not-callable


class MoveFilesToDeprecatedController:
    """Class with functionality to move files to deprecated folder with proper formatting."""

    def __init__(self,
                 file_type: GcsfsDirectIngestFileType,
                 region_code: str,
                 start_date_bound: Optional[str],
                 end_date_bound: Optional[str],
                 dry_run: bool,
                 project_id: str,
                 file_filter: Optional[str]):
        self.file_type = file_type
        self.region_code = region_code
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.dry_run = dry_run
        self.file_filter = file_filter
        self.project_id = project_id
        self.region_storage_dir_path_for_file_type = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code, SystemLevel.STATE, self.file_type,
                project_id=self.project_id))
        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f'move_storage_files_to_deprecated_start_bound_{self.region_code}_region_{self.start_date_bound}'
            f'_end_bound_{self.end_date_bound}_dry_run_{dry_run}_{datetime.datetime.now().isoformat()}.txt')
        self.mutex = threading.Lock()
        self.move_list: List[Tuple[str, str]] = []
        self.move_progress: Optional[Bar] = None

    def run(self) -> None:
        """Main function that will execute the move to deprecated."""

        # TODO(#3666): Update this script to make updates to our Operations db and BigQuery (if necessary).
        #  For now we print these messages to check if appropriate data has been deleted from operations db.
        if self.dry_run:
            if self.file_type == GcsfsDirectIngestFileType.RAW_DATA:
                logging.info("[DRY RUN] All associated rows from our postgres table `direct_ingest_raw_file_metadata` "
                             "and BigQuery dataset `%s_raw_data` must be deleted before moving these "
                             "files to a deprecated location. Make sure you have done this before moving these files.",
                             self.region_code)

            elif self.file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
                logging.info("[DRY RUN] All associated rows from our postgres table `direct_ingest_ingest_file_"
                             "metadata` must be deleted before moving these files to a deprecated location. "
                             "Make sure you have done this before moving these files.")

        else:
            if self.file_type == GcsfsDirectIngestFileType.RAW_DATA:
                i = input("All associated rows from our postgres table `direct_ingest_raw_file_metadata` "
                          f"and BigQuery dataset `{self.region_code}_raw_data` must be deleted before moving these "
                          "files to a deprecated location.\n Have you already done so? [y/n]: ")

                if i.upper() != 'Y':
                    return

            elif self.file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
                i = input("All associated rows from our postgres table `direct_ingest_ingest_file_metadata` "
                          "must be deleted before moving these files to a deprecated location.\n"
                          "Have you already done so? [y/n]: ")

                if i.upper() != 'Y':
                    return

        destination_dir_path = os.path.join(self.region_storage_dir_path_for_file_type.abs_path(), "deprecated",
                                            f"deprecated_on_{date.today()}", f"{str(self.file_type.value)}/")

        if self.dry_run:
            logging.info("[DRY RUN] Moving files from [%s] to [%s]",
                         self.region_storage_dir_path_for_file_type.abs_path(), destination_dir_path)

        else:

            i = input(f"Moving files from [{self.region_storage_dir_path_for_file_type.abs_path()}] to "
                      f"[{destination_dir_path}] - continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        files_to_move = self._get_files_to_move()

        if self.dry_run:
            logging.info("[DRY RUN] Found [%d] files to move",
                         len(files_to_move))

        else:
            i = input(
                f"Found [{len(files_to_move)}] files to move - "
                f"continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        self._execute_move(files_to_move)
        self._write_move_to_log_file()

        if self.dry_run:
            logging.info("DRY RUN: See results in [%s].\n"
                         "Rerun with [--dry-run False] to execute move.",
                         self.log_output_path)
        else:
            logging.info(
                "Move complete! See results in [%s].\n",
                self.log_output_path)

    def _get_files_to_move(self) -> List[str]:
        """Function that gets the files to move to deprecated based on the file_filter and end/start dates specified"""
        subdirs = gsutil_get_storage_subdirs_containing_file_types(
            storage_bucket_path=GcsfsDirectoryPath.from_bucket_and_blob_name(
                self.region_storage_dir_path_for_file_type.bucket_name,
                self.region_code).abs_path(),
            file_type=self.file_type,
            lower_bound_date=self.start_date_bound,
            upper_bound_date=self.end_date_bound)
        result = []
        for subdir_path in subdirs:
            from_paths = gsutil_ls(f'{subdir_path}*.csv')
            for from_path in from_paths:
                _, file_name = os.path.split(from_path)
                if re.match(INGESTED_FILE_REGEX, file_name):
                    if not self.file_filter or re.search(self.file_filter, file_name):
                        result.append(from_path)
        return result

    def _write_move_to_log_file(self):
        self.move_list.sort()
        with open(self.log_output_path, 'w') as f:
            if self.dry_run:
                template = "DRY RUN: Would move {} -> {}\n"
            else:
                template = "Moved {} -> {}\n"

            f.writelines(template.format(original_path, new_path) for original_path, new_path in self.move_list)

    def _move_files_for_date(self, from_uri: str):
        """Function that loops through each list of files to move and moves them to the deprecated folder
        in accordance with the date they were received and the date they were deprecated."""
        curr_gcsfs_file_path = GcsfsFilePath.from_absolute_path(from_uri)
        previous_date_format = filename_parts_from_path(curr_gcsfs_file_path).date_str
        new_date_format = date.fromisoformat(previous_date_format).strftime("%Y/%m/%d/")
        to_uri = os.path.join('gs://', self.region_storage_dir_path_for_file_type.bucket_name,
                              self.region_code,
                              'deprecated',
                              f'deprecated_on_{date.today()}',
                              str(self.file_type.value),
                              new_date_format,
                              curr_gcsfs_file_path.file_name)
        if not self.dry_run:
            gsutil_mv(from_path=from_uri, to_path=to_uri)
        with self.mutex:
            self.move_list.append((from_uri, to_uri))
            if self.move_progress:
                self.move_progress.next()

    def _execute_move(self, files_to_move: List[str]) -> None:
        self.move_progress = Bar("Moving files to deprecated...", max=len(files_to_move))

        thread_pool = ThreadPool(processes=12)
        thread_pool.map(self._move_files_for_date, files_to_move)
        self.move_progress.finish()


def main():
    """Runs the move_state_files_to_deprecated script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--file-type', required=True,
                        choices=[file_type.value for file_type in GcsfsDirectIngestFileType],
                        help='Defines whether we should move raw files or generated ingest_view files')

    parser.add_argument('--region', required=True, help='E.g. \'us_nd\'')

    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs move in dry-run mode, only prints the file moves it would do.')

    parser.add_argument('--start-date-bound',
                        help='The lower bound date to start from, inclusive. For partial moving of ingested files. '
                             'E.g. 2019-09-23.')

    parser.add_argument('--end-date-bound',
                        help='The upper bound date to end at, inclusive. For partial moving of ingested files. '
                             'E.g. 2019-09-23.')

    parser.add_argument('--project-id',
                        help='The id for this particular project, E.g. \'recidiviz-123\'')

    parser.add_argument('--file-filter', default=None,
                        help='Regex name filter - when set, will only move files that match this regex.')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    MoveFilesToDeprecatedController(
        file_type=GcsfsDirectIngestFileType(args.file_type),
        region_code=args.region,
        start_date_bound=args.start_date_bound,
        end_date_bound=args.end_date_bound,
        project_id=args.project_id,
        dry_run=args.dry_run,
        file_filter=args.file_filter).run()


if __name__ == '__main__':
    main()
