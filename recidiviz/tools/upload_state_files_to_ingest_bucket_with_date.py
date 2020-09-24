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
Script for uploading a file/files manually to a region's ingest bucket so that the paths will be normalized with the
date that should be associated with that file/files. Should be used for any new historical files or files we're asked to
upload manually due to an upload script failure.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.upload_state_files_to_ingest_bucket_with_date \
    ~/Downloads/MyHistoricalDump/ --date 2019-08-12 \
    --project-id recidiviz-staging --region us_nd --dry-run True
"""
import argparse
import datetime
import logging
import os
import threading
from multiprocessing.pool import ThreadPool
from typing import Optional, List, Tuple

from progress.bar import Bar

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    to_normalized_unprocessed_file_path
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsDirectIngestFileType, gcsfs_direct_ingest_directory_path_for_region
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tools.gsutil_shell_helpers import gsutil_cp
from recidiviz.utils.params import str_to_bool

# pylint: disable=not-callable


class UploadStateFilesToIngestBucketController:
    """Class with functionality to upload a file or files to a region's ingest bucket."""

    def __init__(self,
                 paths: str,
                 project_id: str,
                 region: str,
                 date: str,
                 dry_run: bool):

        self.paths = paths
        self.project_id = project_id
        self.region = region.lower()
        self.datetime = datetime.datetime.fromisoformat(date)
        self.dry_run = dry_run

        self.ingest_bucket = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_directory_path_for_region(region, SystemLevel.STATE, project_id=self.project_id))

        self.mutex = threading.Lock()
        self.move_progress: Optional[Bar] = None
        self.copies_list: List[Tuple[str, str]] = []
        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f'upload_to_ingest_result_{region}_{self.project_id}_date_{self.datetime.date().isoformat()}'
            f'_dry_run_{self.dry_run}_{datetime.datetime.now().isoformat()}.txt')

    def _copy_to_ingest_bucket(self, path: str, normalized_file_name: str) -> None:
        full_file_upload_path_uri = GcsfsFilePath.from_directory_and_file_name(self.ingest_bucket,
                                                                               normalized_file_name).uri()

        if not self.dry_run:
            gsutil_cp(path, full_file_upload_path_uri)

        with self.mutex:
            self.copies_list.append((path, full_file_upload_path_uri))
            if self.move_progress:
                self.move_progress.next()

    def _upload_file(self, path: str) -> None:
        normalized_file_name = os.path.basename(
            to_normalized_unprocessed_file_path(path,
                                                file_type=GcsfsDirectIngestFileType.RAW_DATA,
                                                dt=self.datetime))
        self._copy_to_ingest_bucket(path, normalized_file_name)

    def _get_paths_to_upload(self) -> List[str]:
        path_candidates = []
        for path in self.paths:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    path_candidates.append(os.path.join(path, filename))
            elif os.path.isfile(path):
                path_candidates.append(path)

            else:
                raise ValueError(
                    f'Could not tell if path [{path}] is a file or directory.')

        result = []
        for path in path_candidates:
            _, ext = os.path.splitext(path)
            if not ext or ext not in ('.csv', '.txt'):
                logging.info("Skipping file [%s] - invalid extension", path)
                continue
            result.append(path)

        return result

    def upload_files(self, paths_to_upload: List[str], thread_pool: ThreadPool):
        msg_prefix = 'DRY_RUN: ' if self.dry_run else ''
        self.move_progress = Bar(f"{msg_prefix}Moving files...", max=len(paths_to_upload))
        thread_pool.map(self._upload_file, paths_to_upload)

        if not self.move_progress:
            raise ValueError('Progress bar should not be None')
        self.move_progress.finish()

    def do_upload(self) -> None:
        """Perform upload to ingest bucket."""
        if self.dry_run:
            logging.info("Running in DRY RUN mode for region [%s]", self.region)
        else:
            i = input(f"This will upload raw files to the [{self.region}] ingest bucket [{self.ingest_bucket.uri()}] "
                      f"with datetime [{self.datetime}]. Type {self.project_id} to continue: ")

            if i != self.project_id:
                return

        paths_to_upload = self._get_paths_to_upload()

        if self.dry_run:
            logging.info("DRY RUN: Found [%s] paths to upload", len(paths_to_upload))
        else:
            i = input(f"Found [{len(paths_to_upload)}] files to move - " f"continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        thread_pool = ThreadPool(processes=12)

        self.upload_files(paths_to_upload, thread_pool)

        thread_pool.close()
        thread_pool.join()

        self.write_copies_to_log_file()

        if self.dry_run:
            logging.info("DRY RUN: See results in [%s].\nRerun with [--dry-run False] to execute move.",
                         self.log_output_path)
        else:
            logging.info("Upload complete! See results in [%s].", self.log_output_path)

    def write_copies_to_log_file(self):
        self.copies_list.sort()
        with open(self.log_output_path, 'w') as f:
            if self.dry_run:
                template = "DRY RUN: Would copy {} -> {}\n"
            else:
                template = "Copied {} -> {}\n"

            f.writelines(template.format(original_path, new_path) for original_path, new_path in self.copies_list)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('paths', metavar='PATH', nargs='+',
                        help='Path to files to move, either single file path or directory path.')

    parser.add_argument('--project-id', required=True,
                        help='Which project the file(s) should be uploaded to (e.g. recidiviz-123).')

    parser.add_argument('--region', required=True,
                        help='E.g. \'us_nd\'')

    parser.add_argument('--date', required=True,
                        help='The date to be associated with this file.')

    parser.add_argument('--dry-run',
                        type=str_to_bool,
                        default=True,
                        help='Whether or not to run this script in dry run (log only) mode.')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    UploadStateFilesToIngestBucketController(paths=args.paths,
                                             project_id=args.project_id,
                                             region=args.region,
                                             date=args.date,
                                             dry_run=args.dry_run).do_upload()


if __name__ == '__main__':
    main()
