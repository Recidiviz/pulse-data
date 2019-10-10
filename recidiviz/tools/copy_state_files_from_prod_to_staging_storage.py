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
Script for copying all files in production storage for a region to state
storage for a region. Should be used when we want to rerun ingest for a state
in staging.

When run in dry-run mode (the default), will only log copies, but will not
execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.copy_state_files_from_prod_to_staging_storage \
    --region us_nd --dry-run True
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
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_storage_directory_path_for_region
from recidiviz.tools.gsutil_shell_helpers import gsutil_ls, gsutil_cp
from recidiviz.utils.params import str_to_bool


class CopyFilesFromProdToStagingController:
    """Class with functionality to copy files between prod and staging storage.
    """

    def __init__(self,
                 region_code: str,
                 dry_run: bool):
        self.prod_storage_bucket = \
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code,
                SystemLevel.STATE,
                project_id='recidiviz-123')
        self.staging_storage_bucket = \
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code,
                SystemLevel.STATE,
                project_id='recidiviz-staging')
        self.dry_run = dry_run

        self.log_output_path = \
            os.path.join(os.path.dirname(__file__),
                         f'copy_prod_to_staging_result_{region_code}_'
                         f'dry_run_{dry_run}'
                         f'_{datetime.datetime.now().isoformat()}.txt')
        self.mutex = threading.Lock()
        self.copy_list: List[Tuple[str, str]] = []
        self.copy_progress: Optional[Bar] = None

    def run(self) -> None:
        """Main function that will execute the copy."""
        if self.dry_run:
            logging.info("[DRY RUN] Copying files from [%s] to [%s]",
                         self.prod_storage_bucket,
                         self.staging_storage_bucket)
        else:
            i = input(f"Copying files from [{self.prod_storage_bucket}] to "
                      f"[{self.staging_storage_bucket}] - continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        subdirs_to_copy = self._get_subdirs_to_copy()

        if self.dry_run:
            logging.info("[DRY RUN] Found [%d] subdirectories to copy",
                         len(subdirs_to_copy))
        else:
            i = input(
                f"Found [{len(subdirs_to_copy)}] subdirectories to copy - "
                f"continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        self._execute_copy(subdirs_to_copy)
        self._write_copies_to_log_file()

        if self.dry_run:
            logging.info("DRY RUN: See results in [%s].\n"
                         "Rerun with [--dry-run False] to execute copy.",
                         self.log_output_path)
        else:
            logging.info(
                "Copy complete! See results in [%s].\n",
                self.log_output_path)

    def _get_subdirs_to_copy(self) -> List[str]:
        subdirs = gsutil_ls(f'gs://{self.prod_storage_bucket}')

        subdirs_to_copy = []
        for subdir in subdirs:
            if not subdir.endswith('/'):
                logging.info("Path [%s] is in unexpected format, skipping",
                             subdir)
                continue

            subdir_name = os.path.basename(os.path.normpath(subdir))
            try:
                datetime.date.fromisoformat(subdir_name)
            except ValueError:
                logging.info("Path [%s] is in unexpected format, skipping",
                             subdir)
                continue

            subdirs_to_copy.append(subdir_name)
        return subdirs_to_copy

    def _write_copies_to_log_file(self):
        self.copy_list.sort()
        with open(self.log_output_path, 'w') as f:
            if self.dry_run:
                template = "DRY RUN: Would copy {} -> {}\n"
            else:
                template = "Copied {} -> {}\n"

            f.writelines(template.format(original_path, new_path)
                         for original_path, new_path in self.copy_list)

    def _copy_files_for_date(self, date_str: str):

        from_path = f'gs://{self.prod_storage_bucket}/{date_str}/*'
        to_path = f'gs://{self.staging_storage_bucket}/{date_str}/'

        if not self.dry_run:
            gsutil_cp(from_path=from_path, to_path=to_path)
        with self.mutex:
            self.copy_list.append((from_path, to_path))
            if self.copy_progress:
                self.copy_progress.next()

    def _execute_copy(self,
                      subdirs_to_copy: List[str]) -> None:
        self.copy_progress = \
            Bar(f"Copying files from subdirectories...",
                max=len(subdirs_to_copy))

        thread_pool = ThreadPool(processes=12)
        thread_pool.map(self._copy_files_for_date, subdirs_to_copy)
        self.copy_progress.finish()


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--region', required=True,
                        help='E.g. \'us_nd\'')
    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs copy in dry-run mode, only prints the file '
                             'copies it would do.')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    CopyFilesFromProdToStagingController(region_code=args.region,
                                         dry_run=args.dry_run).run()


if __name__ == '__main__':
    main()
