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
Script for moving files from storage back into an ingest bucket to be
re-ingested. Should be run in the pipenv shell.

Steps:
1. Pauses ingest queues so we don't ingest partially split files.
2. Finds all subfolders in storage for dates we want to re-ingest, based on
    date_bound.
3. Finds all files in those subfolders.
4. Moves all found files to the ingest bucket.
5. Writes moves to a logfile.
6. Prints instructions for next steps, including how to unpause queues, if
    necessary.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.move_state_files_from_storage \
    --project-id recidiviz-staging --region us_nd \
    --date-bound 2019-07-12 --dry-run True
    [--file_filter "docstars_offendercases|elite_offender"]
"""

import argparse
import datetime
import json
import logging
import os
import re
import subprocess
import threading
from multiprocessing.pool import ThreadPool
from typing import Optional, List, Tuple

from progress.bar import Bar

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_storage_directory_path_for_region, \
    gcsfs_direct_ingest_directory_path_for_region
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    DIRECT_INGEST_SCHEDULER_QUEUE_V2, DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2
from recidiviz.tools.gsutil_shell_helpers import gsutil_ls, gsutil_mv
from recidiviz.utils.params import str_to_bool


class MoveFilesFromStorageController:
    """
    Class that executes file moves from a direct ingest Google Cloud Storage
    bucket to the appropriate ingest bucket.
    """

    FILE_TO_MOVE_RE = \
        re.compile(r'^(processed_|unprocessed_|un)?(\d{4}-\d{2}-\d{2}T.*)')

    QUEUES_TO_PAUSE = {DIRECT_INGEST_SCHEDULER_QUEUE_V2,
                       DIRECT_INGEST_STATE_PROCESS_JOB_QUEUE_V2}

    PAUSE_QUEUE_URL = \
        'https://cloudtasks.googleapis.com/v2/projects/{}/locations/us-east1/' \
        'queues/{}:pause'

    PURGE_QUEUE_URL = \
        'https://cloudtasks.googleapis.com/v2/projects/{}/locations/us-east1/' \
        'queues/{}:purge'

    CURL_POST_REQUEST_TEMPLATE = \
        'curl -X POST -H ' \
        '"Authorization: Bearer $(gcloud auth print-access-token)" {}'

    def __init__(self,
                 project_id: str,
                 region: str,
                 date_bound: Optional[str],
                 dry_run: bool,
                 file_filter: Optional[str]):

        self.project_id = project_id
        self.region = region
        self.date_bound = date_bound
        self.dry_run = dry_run
        self.file_filter = file_filter

        self.storage_bucket = \
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region,
                SystemLevel.STATE,
                project_id=self.project_id)
        self.ingest_bucket = \
            gcsfs_direct_ingest_directory_path_for_region(
                region,
                SystemLevel.STATE,
                project_id=self.project_id)

        self.mutex = threading.Lock()
        self.collect_progress: Optional[Bar] = None
        self.move_progress: Optional[Bar] = None
        self.moves_list: List[Tuple[str, str]] = []
        self.log_output_path = \
            os.path.join(os.path.dirname(__file__),
                         f'move_result_{region}_{self.project_id}_'
                         f'bound_{self.date_bound}_dry_run_{self.dry_run}_'
                         f'{datetime.datetime.now().isoformat()}.txt')

    def run_move(self):
        """Main method of script - executes move, or runs a dry run of a move.
        """
        if self.dry_run:
            logging.info("Running in DRY RUN mode for region [%s]",
                         self.region)
        else:
            i = input(f"This will move [{self.region}] files in "
                      f"[{self.project_id}] that were uploaded on or after "
                      f"[{self.date_bound}]. Type {self.project_id} to "
                      f"continue: ")

            if i != self.project_id:
                return

        if self.dry_run:
            logging.info("DRY RUN: Would pause [%s] in project [%s]",
                         self.QUEUES_TO_PAUSE, self.project_id)
        else:
            i = input(f"Pausing queues {self.QUEUES_TO_PAUSE} in project "
                      f"[{self.project_id}] - continue? [y/n]: ")

            if i.upper() != 'Y':
                return

            self.pause_and_purge_queues()

        date_subdir_paths = self.get_date_subdir_paths()

        if self.dry_run:
            logging.info("DRY RUN: Found [%s] dates to move",
                         len(date_subdir_paths))
        else:
            i = input(f"Found [{len(date_subdir_paths)}] dates to move - "
                      f"continue? [y/n]: ")

            if i.upper() != 'Y':
                return

        thread_pool = ThreadPool(processes=12)
        files_to_move = self.collect_files_to_move(date_subdir_paths,
                                                   thread_pool)

        self.move_files(files_to_move, thread_pool)

        thread_pool.close()
        thread_pool.join()

        self.write_moves_to_log_file()

        if self.dry_run:
            logging.info("DRY RUN: See results in [%s].\n"
                         "Rerun with [--dry-run False] to execute move.",
                         self.log_output_path)
        else:
            logging.info(
                "Move complete! See results in [%s].\n"
                "\nNext steps:"
                "\n1. (If doing a full re-ingest) Drop Google Cloud database "
                "for [%s]"
                "\n2. Resume queues here:",
                self.log_output_path, self.project_id)

            for queue_name in self.QUEUES_TO_PAUSE:
                logging.info("\t%s", self.queue_console_url(queue_name))

    def get_date_subdir_paths(self) -> List[str]:
        possible_paths = gsutil_ls(f'gs://{self.storage_bucket}')

        result = []
        for path in possible_paths:
            last_part = \
                os.path.basename(
                    os.path.normpath(path))
            if self.is_date_str(last_part) and \
                    (self.date_bound is None or last_part >= self.date_bound):
                result.append(path)
        return result

    def collect_files_to_move(self,
                              date_subdir_paths: List[str],
                              thread_pool: ThreadPool) -> List[str]:
        """Searches the given list of directory paths for files directly in
        those directories that should be moved to the ingest directory and
        returns a list of string paths to those files.
        """
        msg_prefix = 'DRY_RUN: ' if self.dry_run else ''
        self.collect_progress = \
            Bar(f"{msg_prefix}Gathering paths to move...",
                max=len(date_subdir_paths))
        collect_files_res = thread_pool.map(self.get_files_to_move_from_path,
                                            date_subdir_paths)

        if not self.collect_progress:
            raise ValueError('Progress bar should not be None')
        self.collect_progress.finish()

        return [f for sublist in collect_files_res for f in sublist]

    def move_files(self, files_to_move: List[str], thread_pool: ThreadPool):
        """Moves files at the given paths to the ingest directory, changing the
        prefix to 'unprocessed' as necessary.

        For the given list of file paths:

        files_to_move = [
            'storage_bucket/path/to/processed_2019-09-24T09:01:20:039807_'
            'elite_offendersentenceterms.csv'
        ]

        Will run:
        gsutil mv
            gs://storage_bucket/path/to/processed_2019-09-24T09:01:20:039807_\
            elite_offendersentenceterms.csv \
            unprocessed_2019-09-24T09:01:20:039807_\
            elite_offendersentenceterms.csv

        Note: Move order is not guaranteed - file moves are parallelized.
        """
        msg_prefix = 'DRY_RUN: ' if self.dry_run else ''
        self.move_progress = \
            Bar(f"{msg_prefix}Moving files...",
                max=len(files_to_move))
        thread_pool.map(self.move_file, files_to_move)

        if not self.move_progress:
            raise ValueError('Progress bar should not be None')
        self.move_progress.finish()

    def queue_console_url(self, queue_name: str):
        """Returns the url to the GAE console page for a queue with a given
        name.
        """
        return f'https://console.cloud.google.com/cloudtasks/queue/' \
            f'{queue_name}?project={self.project_id}'

    def do_post_request(self, url: str):
        """Executes a googleapis.com curl POST request with the given url. """
        res = subprocess.Popen(self.CURL_POST_REQUEST_TEMPLATE.format(url),
                               shell=True,
                               stdout=subprocess.PIPE)
        stdout, _stderr = res.communicate()
        response = json.loads(stdout)
        if 'error' in response:
            raise ValueError(response['error'])

    def pause_queue(self, queue_name: str):
        """Posts a request to pause the queue with the given name."""
        logging.info("Pausing [%s] in [%s]", queue_name, self.project_id)
        self.do_post_request(
            self.PAUSE_QUEUE_URL.format(self.project_id, queue_name))

    def purge_queue(self, queue_name: str):
        """Posts a request to purge the queue with the given name."""
        logging.info("Purging [%s] in [%s]", queue_name, self.project_id)
        self.do_post_request(
            self.PURGE_QUEUE_URL.format(self.project_id, queue_name))

    def pause_and_purge_queues(self):
        """Pauses and purges Direct Ingest queues for the specified project."""
        for queue_name in self.QUEUES_TO_PAUSE:
            self.pause_queue(queue_name)
            self.purge_queue(queue_name)

    @staticmethod
    def is_date_str(potential_date_str: str) -> bool:
        """Returns True if the string is an ISO-formatted date,
        (e.g. '2019-09-25'), False otherwise.
        """
        try:
            datetime.datetime.strptime(potential_date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def get_files_to_move_from_path(self, gs_dir_path: str) -> List[str]:
        """Returns files directly in the given directory that should be moved
        back into the ingest directory.
        """
        file_paths = gsutil_ls(gs_dir_path)

        result = []
        for file_path in file_paths:
            _, file_name = os.path.split(file_path)
            if re.match(self.FILE_TO_MOVE_RE, file_name):
                if not self.file_filter or \
                        re.search(self.file_filter, file_name):
                    result.append(file_path)
        with self.mutex:
            if self.collect_progress:
                self.collect_progress.next()
        return result

    def move_file(self, original_file_path: str):
        """Moves a file at the given path into the ingest directory, updating
        the name to always have an prefix of 'unprocessed'. Logs the file move,
        which will later be written to a log file.

        If in dry_run mode, merely logs the move, but does not execute it.
        """
        new_file_path = \
            self.build_moved_file_path(original_file_path)

        if not self.dry_run:
            gsutil_mv(original_file_path, new_file_path)

        with self.mutex:
            self.moves_list.append((original_file_path, new_file_path))
            if self.move_progress:
                self.move_progress.next()

    def build_moved_file_path(self,
                              original_file_path: str) -> str:
        """Builds the desired path for the given file in the ingest bucket,
        changing the prefix to 'unprocessed' as is necessary.
        """
        _, file_name = os.path.split(original_file_path)
        match_result = re.match(self.FILE_TO_MOVE_RE, file_name)

        if not match_result:
            raise ValueError(f"Invalid file name {file_name}")

        return os.path.join('gs://', self.ingest_bucket,
                            f'unprocessed_{match_result.group(2)}')

    def write_moves_to_log_file(self):
        self.moves_list.sort()
        with open(self.log_output_path, 'w') as f:
            if self.dry_run:
                template = "DRY RUN: Would move {} -> {}\n"
            else:
                template = "Moved {} -> {}\n"

            f.writelines(template.format(original_path, new_path)
                         for original_path, new_path in self.moves_list)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--project-id', required=True,
                        help='Which project\'s files should be moved '
                             '(e.g. recidiviz-123).')

    parser.add_argument('--region', required=True,
                        help='E.g. \'us_nd\'')

    parser.add_argument('--date-bound',
                        help='The lower bound date to start from, inclusive. '
                             'For partial replays of ingested files. '
                             'E.g. 2019-09-23.')

    parser.add_argument('--dry-run', default=True, type=str_to_bool,
                        help='Runs move in dry-run mode, only prints the file '
                             'moves it would do.')

    parser.add_argument('--file-filter', default=None,
                        help='Regex name filter - when set, will only move '
                             'files that match this regex.')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    MoveFilesFromStorageController(
        project_id=args.project_id,
        region=args.region,
        date_bound=args.date_bound,
        dry_run=args.dry_run,
        file_filter=args.file_filter
    ).run_move()


if __name__ == '__main__':
    main()
