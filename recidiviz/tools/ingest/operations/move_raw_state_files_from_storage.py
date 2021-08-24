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
Script for moving files from storage back into an ingest bucket to be re-ingested. Should be run in the pipenv shell.

Steps:
1. Pauses ingest queues so we don't ingest partially split files.
2. Finds all subfolders in storage for dates we want to re-ingest, based on start-date-bound, end-date-bound, and
    file-type-to-move.
3. Finds all files in those subfolders.
4. Moves all found files to the ingest bucket, updating the file type to destination-file-type.
5. Writes moves to a logfile.
6. Prints instructions for next steps, including how to unpause queues, if necessary.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.operations.move_raw_state_files_from_storage \
    --project-id recidiviz-staging --region us_nd \
    --start-date-bound 2019-08-12  --end-date-bound 2019-08-13 --dry-run True \
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
from typing import List, Optional, Tuple

from progress.bar import Bar

from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_path_from_normalized_path,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    gcsfs_direct_ingest_bucket_for_region,
    gcsfs_direct_ingest_storage_directory_path_for_region,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    get_direct_ingest_queues_for_state,
)
from recidiviz.tools.gsutil_shell_helpers import (
    gsutil_get_storage_subdirs_containing_file_types,
    gsutil_ls,
    gsutil_mv,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

# pylint: disable=not-callable


class MoveFilesFromStorageController:
    """Class that executes file moves from a direct ingest Google Cloud Storage bucket to the appropriate ingest
    bucket.
    """

    FILE_TO_MOVE_RE = re.compile(
        r"^(processed_|unprocessed_|un)?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}:\d{6}(raw|ingest_view)?.*)"
    )

    PAUSE_QUEUE_URL = "https://cloudtasks.googleapis.com/v2/projects/{}/locations/us-east1/queues/{}:pause"

    PURGE_QUEUE_URL = "https://cloudtasks.googleapis.com/v2/projects/{}/locations/us-east1/queues/{}:purge"

    CURL_POST_REQUEST_TEMPLATE = (
        'curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" {}'
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        dry_run: bool,
        file_filter: Optional[str],
    ):

        self.project_id = project_id
        self.region = region
        self.state_code = StateCode(region.upper())
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.dry_run = dry_run
        self.file_filter = file_filter

        self.storage_bucket = gcsfs_direct_ingest_storage_directory_path_for_region(
            region_code=region,
            system_level=SystemLevel.STATE,
            # Raw files are only ever stored in the PRIMARY storage bucket
            ingest_instance=DirectIngestInstance.PRIMARY,
            project_id=self.project_id,
        )
        self.ingest_bucket = gcsfs_direct_ingest_bucket_for_region(
            region_code=region,
            system_level=SystemLevel.STATE,
            # Raw files are only ever processed in the PRIMARY ingest bucket
            ingest_instance=DirectIngestInstance.PRIMARY,
            project_id=self.project_id,
        )

        self.mutex = threading.Lock()
        self.collect_progress: Optional[Bar] = None
        self.move_progress: Optional[Bar] = None
        self.moves_list: List[Tuple[str, str]] = []
        self.log_output_path = os.path.join(
            os.path.dirname(__file__),
            f"move_result_{region}_{self.project_id}_start_bound_{self.start_date_bound}_end_bound_"
            f"{self.end_date_bound}_dry_run_{self.dry_run}_{datetime.datetime.now().isoformat()}.txt",
        )

    def run_move(self) -> None:
        """Main method of script - executes move, or runs a dry run of a move."""
        if self.dry_run:
            logging.info("Running in DRY RUN mode for region [%s]", self.region)

        prompt_for_confirmation(
            f"This will move [{self.region}] files in [{self.project_id}] that were uploaded starting on date"
            f"[{self.start_date_bound}] and ending on date [{self.end_date_bound}].",
            accepted_response_override=self.project_id,
            dry_run=self.dry_run,
        )

        prompt_for_confirmation(
            f"Pausing queues {self._queues_to_pause()} in project [{self.project_id}] "
            f"- continue?",
            dry_run=self.dry_run,
        )

        if not self.dry_run:
            self.pause_and_purge_queues()

        logging.info("Finding files to move...")
        date_subdir_paths = self.get_date_subdir_paths()

        prompt_for_confirmation(
            f"Found [{len(date_subdir_paths)}] dates to move - continue?",
            dry_run=self.dry_run,
        )

        thread_pool = ThreadPool(processes=12)
        files_to_move = self.collect_files_to_move(date_subdir_paths, thread_pool)

        self.move_files(files_to_move, thread_pool)

        thread_pool.close()
        thread_pool.join()

        self.write_moves_to_log_file()

        if self.dry_run:
            logging.info(
                "[DRY RUN] See results in [%s].\n"
                "Rerun with [--dry-run False] to execute move.",
                self.log_output_path,
            )
        else:
            logging.info(
                "Move complete! See results in [%s].\n"
                "\nNext steps:"
                "\n1. (If doing a full re-ingest) Drop Google Cloud database for [%s]"
                "\n2. Resume queues here:",
                self.log_output_path,
                self.project_id,
            )

            for queue_name in self._queues_to_pause():
                logging.info("\t%s", self.queue_console_url(queue_name))

    def get_date_subdir_paths(self) -> List[str]:
        return gsutil_get_storage_subdirs_containing_file_types(
            storage_bucket_path=self.storage_bucket.abs_path(),
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
            upper_bound_date=self.end_date_bound,
            lower_bound_date=self.start_date_bound,
        )

    def collect_files_to_move(
        self, date_subdir_paths: List[str], thread_pool: ThreadPool
    ) -> List[str]:
        """Searches the given list of directory paths for files directly in those directories that should be moved to
        the ingest directory and returns a list of string paths to those files.
        """
        msg_prefix = "[DRY RUN] " if self.dry_run else ""
        self.collect_progress = Bar(
            f"{msg_prefix}Gathering paths to move...", max=len(date_subdir_paths)
        )
        collect_files_res = thread_pool.map(
            self.get_files_to_move_from_path, date_subdir_paths
        )

        if not self.collect_progress:
            raise ValueError("Progress bar should not be None")
        self.collect_progress.finish()

        return [f for sublist in collect_files_res for f in sublist]

    def move_files(self, files_to_move: List[str], thread_pool: ThreadPool) -> None:
        """Moves files at the given paths to the ingest directory, changing the prefix to 'unprocessed' as necessary.

        For the given list of file paths:

        files_to_move = [
            'storage_bucket/path/to/processed_2019-09-24T09:01:20:039807_elite_offendersentenceterms.csv'
        ]

        Will run:
        gsutil mv
            gs://storage_bucket/path/to/processed_2019-09-24T09:01:20:039807_elite_offendersentenceterms.csv \
            unprocessed_2019-09-24T09:01:20:039807_elite_offendersentenceterms.csv

        Note: Move order is not guaranteed - file moves are parallelized.
        """
        msg_prefix = "[DRY RUN] " if self.dry_run else ""
        self.move_progress = Bar(f"{msg_prefix}Moving files...", max=len(files_to_move))
        thread_pool.map(self.move_file, files_to_move)

        if not self.move_progress:
            raise ValueError("Progress bar should not be None")
        self.move_progress.finish()

    def queue_console_url(self, queue_name: str) -> str:
        """Returns the url to the GCP console page for a queue with a given name."""
        return f"https://console.cloud.google.com/cloudtasks/queue/{queue_name}?project={self.project_id}"

    def do_post_request(self, url: str) -> None:
        """Executes a googleapis.com curl POST request with the given url."""
        res = subprocess.run(
            self.CURL_POST_REQUEST_TEMPLATE.format(url),
            shell=True,
            stdout=subprocess.PIPE,
            check=True,
        )
        response = json.loads(res.stdout)
        if "error" in response:
            raise ValueError(response["error"])

    def pause_queue(self, queue_name: str) -> None:
        """Posts a request to pause the queue with the given name."""
        logging.info("Pausing [%s] in [%s]", queue_name, self.project_id)
        self.do_post_request(self.PAUSE_QUEUE_URL.format(self.project_id, queue_name))

    def purge_queue(self, queue_name: str) -> None:
        """Posts a request to purge the queue with the given name."""
        logging.info("Purging [%s] in [%s]", queue_name, self.project_id)
        self.do_post_request(self.PURGE_QUEUE_URL.format(self.project_id, queue_name))

    def pause_and_purge_queues(self) -> None:
        """Pauses and purges Direct Ingest queues for the specified project."""
        for queue_name in self._queues_to_pause():
            self.pause_queue(queue_name)
            self.purge_queue(queue_name)

    def get_files_to_move_from_path(self, gs_dir_path: str) -> List[str]:
        """Returns files directly in the given directory that should be moved back into the ingest directory."""
        file_paths = gsutil_ls(gs_dir_path)

        result = []
        for file_path in file_paths:
            _, file_name = os.path.split(file_path)
            if re.match(self.FILE_TO_MOVE_RE, file_name):
                if not self.file_filter or re.search(self.file_filter, file_name):
                    result.append(file_path)
        with self.mutex:
            if self.collect_progress:
                self.collect_progress.next()
        return result

    def move_file(self, original_file_path: str) -> None:
        """Moves a file at the given path into the ingest directory, updating the name to always have an prefix of
        'unprocessed'. Logs the file move, which will later be written to a log file.

        If in dry_run mode, merely logs the move, but does not execute it.
        """
        new_file_path = self.build_moved_file_path(original_file_path)

        if not self.dry_run:
            gsutil_mv(original_file_path, new_file_path)

        with self.mutex:
            self.moves_list.append((original_file_path, new_file_path))
            if self.move_progress:
                self.move_progress.next()

    def build_moved_file_path(self, original_file_path: str) -> str:
        """Builds the desired path for the given file in the ingest bucket, changing the prefix to 'unprocessed' as is
        necessary.
        """

        path_as_unprocessed = to_normalized_unprocessed_file_path_from_normalized_path(
            original_file_path
        )

        _, file_name = os.path.split(path_as_unprocessed)

        if not re.match(self.FILE_TO_MOVE_RE, file_name):
            raise ValueError(f"Invalid file name {file_name}")

        return os.path.join("gs://", self.ingest_bucket.abs_path(), file_name)

    def write_moves_to_log_file(self) -> None:
        self.moves_list.sort()
        with open(self.log_output_path, "w", encoding="utf-8") as f:
            if self.dry_run:
                template = "[DRY RUN] Would move {} -> {}\n"
            else:
                template = "Moved {} -> {}\n"

            f.writelines(
                template.format(original_path, new_path)
                for original_path, new_path in self.moves_list
            )

    def _queues_to_pause(self) -> List[str]:
        return get_direct_ingest_queues_for_state(self.state_code)


def main() -> None:
    """Runs the move_state_files_to_storage script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="Which project's files should be moved (e.g. recidiviz-123).",
    )

    parser.add_argument("--region", required=True, help="E.g. 'us_nd'")

    parser.add_argument(
        "--start-date-bound",
        help="The lower bound date to start from, inclusive. For partial replays of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--end-date-bound",
        help="The upper bound date to end at, inclusive. For partial replays of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs move in dry-run mode, only prints the file moves it would do.",
    )

    parser.add_argument(
        "--file-filter",
        default=None,
        help="Regex name filter - when set, will only move files that match this regex.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with local_project_id_override(args.project_id):
        MoveFilesFromStorageController(
            project_id=args.project_id,
            region=args.region,
            start_date_bound=args.start_date_bound,
            end_date_bound=args.end_date_bound,
            dry_run=args.dry_run,
            file_filter=args.file_filter,
        ).run_move()


if __name__ == "__main__":
    main()
