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
Script for uploading a file/files manually to storage. Should be used for any
new historical files or files we're asked to upload manually due to an upload
script failure.

Can be used in coordination with the move_state_files_from_storage script to
both upload and start ingest for these files.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.check_state_file_into_storage \
    ~/Downloads/MyHistoricalDump/ --date 2019-08-12 \
    --project-id recidiviz-staging --region us_nd
"""
import argparse
import datetime
import logging
import os

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    to_normalized_unprocessed_file_path
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_storage_directory_path_for_region, GcsfsDirectIngestFileType
from recidiviz.tools.gsutil_shell_helpers import gsutil_cp


# TODO(3020): This script will need to be changed once we have SQL preprocessing flow to import raw data into BQ.
#  Our flow for files we don't want to ingest yet should be just dropped into the ingest bucket as raw files, imported
#  to BQ without actually having an ingest view built on top of that data. We will want to change this script to allow
#  us to upload to the *ingest* bucket with a certain date timestamp for backfills etc.
class CheckStateFileIntoStorageController:
    """Class with functionality to upload a file or files to storage."""

    def __init__(self,
                 paths: str,
                 project_id: str,
                 region: str,
                 date: str):

        self.paths = paths
        self.project_id = project_id
        self.region = region.lower()
        self.datetime = datetime.datetime.fromisoformat(date)

        self.storage_bucket = \
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region,
                SystemLevel.STATE,
                project_id=self.project_id)

    def _copy_to_storage(self, path: str, normalized_file_name: str) -> None:
        storage_dir_path = os.path.join('gs://',
                                        self.storage_bucket,
                                        GcsfsDirectIngestFileType.RAW_DATA.value,
                                        str(self.datetime.year),
                                        str(self.datetime.month),
                                        str(self.datetime.day))

        logging.info("Copying [%s] to [%s]", path, storage_dir_path)

        full_file_storage_path = f'{storage_dir_path}/{normalized_file_name}'
        gsutil_cp(path, full_file_storage_path)

    def _do_check_in_for_file(self, path: str) -> None:
        normalized_file_name = os.path.basename(
            to_normalized_unprocessed_file_path(path,
                                                file_type=GcsfsDirectIngestFileType.RAW_DATA,
                                                dt=self.datetime))
        self._copy_to_storage(path, normalized_file_name)

    def do_check_in(self) -> None:
        path_candidates = []
        for path in self.paths:
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    path_candidates.append(os.path.join(path, filename))
                    self._do_check_in_for_file(os.path.join(path, filename))
            elif os.path.isfile(path):
                path_candidates.append(path)

            else:
                raise ValueError(
                    f'Could not tell if path [{path}] is a file or directory.')

        for path in path_candidates:
            _, ext = os.path.splitext(path)
            if not ext or ext != '.csv':
                logging.info("Skipping file [%s] - invalid extension", path)
                continue
            self._do_check_in_for_file(path)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('paths', metavar='PATH', nargs='+',
                        help='Path to files to move, either single file path '
                             'or directory path.')

    parser.add_argument('--project-id', required=True,
                        help='Which project the file(s) should be checked into '
                             '(e.g. recidiviz-123).')

    parser.add_argument('--region', required=True,
                        help='E.g. \'us_nd\'')

    parser.add_argument('--date', required=True,
                        help='The date to be associated with this file.')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    CheckStateFileIntoStorageController(paths=args.paths,
                                        project_id=args.project_id,
                                        region=args.region,
                                        date=args.date).do_check_in()


if __name__ == '__main__':
    main()
