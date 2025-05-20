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
1. Finds all sub-folders in storage for dates we want to re-ingest, based on start-date-bound, end-date-bound, and
    file-type-to-move.
2. Finds all files in those sub-folders.
3. Acquires raw data resource lock for the ingest bucket
4. Moves all found files to the ingest bucket, updating the file type to destination-file-type.
5. Writes moves to a logfile.
6. release raw data resource lock for the ingest bucket

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.operations.move_raw_state_files_from_storage \
    --source-project-id recidiviz-staging --source-raw-data-instance PRIMARY \
    --destination-project-id recidiviz-staging --destination-raw-data-instance SECONDARY \
    --region us_tn --start-date-bound 2025-03-10 --dry-run True
"""

import argparse
import logging
import os
import re
from typing import List, Optional, Tuple

from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_path_from_normalized_path,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_resource_lock_manager import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestRawDataResourceLock,
)
from recidiviz.tools.gsutil_shell_helpers import (
    GSUTIL_DEFAULT_TIMEOUT_SEC,
    gsutil_get_storage_subdirs_containing_raw_files,
    gsutil_ls,
    gsutil_mv,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results
from recidiviz.utils.log_helpers import make_log_output_path
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
    RESOURCE_LOCKS_REQUIRED = (DirectIngestRawDataResourceLockResource.BUCKET,)

    def __init__(
        self,
        *,
        source_project_id: str,
        destination_project_id: str,
        source_raw_data_instance: DirectIngestInstance,
        destination_raw_data_instance: DirectIngestInstance,
        region: str,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        dry_run: bool,
        file_filter: Optional[str],
    ):
        self.source_project_id = source_project_id
        self.destination_project_id = destination_project_id
        self.region = region
        self.state_code = StateCode(region.upper())
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.dry_run = dry_run
        self.file_filter = file_filter

        self.source_raw_data_instance = source_raw_data_instance
        self.destination_raw_data_instance = destination_raw_data_instance

        self.source_storage_bucket = (
            gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code=region,
                ingest_instance=self.source_raw_data_instance,
                project_id=self.source_project_id,
            )
        )
        self.destination_ingest_bucket = gcsfs_direct_ingest_bucket_for_state(
            region_code=region,
            ingest_instance=self.destination_raw_data_instance,
            project_id=self.destination_project_id,
        )

        self.successful_moves_list: List[Tuple[str, str]] = []
        self.failed_moves_list: List[Tuple[str, str]] = []

        self.lock_manager = DirectIngestRawDataResourceLockManager(
            region_code=self.region,
            raw_data_source_instance=self.destination_raw_data_instance,
            with_proxy=True,
        )

        self.log_output_path = make_log_output_path(
            operation_name="move",
            region_code=region,
            date_string=f"start_bound_{start_date_bound}_end_bound_{end_date_bound}",
            dry_run=dry_run,
        )

    def run_move(self) -> None:
        """Main method of script - executes move, or runs a dry run of a move."""
        if self.dry_run:
            logging.info("Running in DRY RUN mode for region [%s]", self.region)

        prompt_for_confirmation(
            f"This will move [{self.region}] files from [{self.source_project_id}] storage that were uploaded starting "
            f"on date [{self.start_date_bound}] and ending on date [{self.end_date_bound}] to ingest bucket in "
            f"[{self.destination_project_id}].",
            dry_run=self.dry_run,
        )

        logging.info("Finding files to move...")
        date_subdir_paths = self.get_date_subdir_paths()

        prompt_for_confirmation(
            f"Found [{len(date_subdir_paths)}] dates to move - continue?",
            dry_run=self.dry_run,
        )

        resource_locks: list[DirectIngestRawDataResourceLock]

        if self.dry_run:
            logging.info("DRY RUN: would acquire locks...")
        else:
            logging.info("Acquiring locks...")
            resource_locks = self.lock_manager.acquire_lock_for_resources(
                resources=list(self.RESOURCE_LOCKS_REQUIRED),
                actor=DirectIngestRawDataLockActor.ADHOC,
                description="Holding locks during the ingest file upload step of the move_raw_state_files_from_storage script",
                ttl_seconds=3 * 60 * 60,  # 3 hours
            )

        try:
            files_to_move = self.collect_files_to_move(date_subdir_paths)
            self.move_files(files_to_move)
            self.write_moves_to_log_file()

            if self.dry_run:
                logging.info(
                    "[DRY RUN] See results in [%s].\n"
                    "Rerun with [--dry-run False] to execute move.",
                    self.log_output_path,
                )
            else:
                logging.info(
                    "Move complete! See results in [%s].\n",
                    self.log_output_path,
                )
        finally:
            if self.dry_run:
                logging.info("DRY RUN: would release locks...")
            else:
                logging.info("Releasing locks...")
                for lock in resource_locks:
                    self.lock_manager.release_lock_by_id(lock.lock_id)

    def get_date_subdir_paths(self) -> List[str]:
        return gsutil_get_storage_subdirs_containing_raw_files(
            storage_bucket_path=self.source_storage_bucket,
            upper_bound_date=self.end_date_bound,
            lower_bound_date=self.start_date_bound,
        )

    def collect_files_to_move(self, date_subdir_paths: List[str]) -> List[str]:
        """Searches the given list of directory paths for files directly in those directories that should be moved to
        the ingest directory and returns a list of string paths to those files.
        """
        msg_prefix = "[DRY RUN] " if self.dry_run else ""

        result = map_fn_with_progress_bar_results(
            work_items=date_subdir_paths,
            work_fn=self.get_files_to_move_from_path,
            progress_bar_message=f"{msg_prefix}Gathering paths to move...",
            # Add a timeout that is generously longer than the timeout for the
            # GSUTIL ls call
            single_work_item_timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC + 10,
            # 4 hour timeout
            overall_timeout_sec=60 * 60 * 4,
        )

        return [f for _subdir, sublist in result.successes for f in sublist]

    def move_files(self, files_to_move: List[str]) -> None:
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
        result = map_fn_with_progress_bar_results(
            work_items=[
                (original_file_path, self.build_moved_file_path(original_file_path))
                for original_file_path in files_to_move
            ],
            work_fn=self.move_file,
            progress_bar_message=f"{msg_prefix}Moving files...",
            # Add a timeout that is generously longer than the timeout for the
            # GSUTIL mv call
            single_work_item_timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC + 10,
            # 4 hour timeout
            overall_timeout_sec=60 * 60 * 4,
        )

        self.successful_moves_list.extend(paths for paths, _ in result.successes)
        self.failed_moves_list.extend(paths for paths, _ in result.exceptions)

    def get_files_to_move_from_path(self, gs_dir_path: str) -> List[str]:
        """Returns files directly in the given directory that should be moved back into the ingest directory."""
        file_paths = gsutil_ls(gs_dir_path)

        result = []
        for file_path in file_paths:
            _, file_name = os.path.split(file_path)
            if re.match(self.FILE_TO_MOVE_RE, file_name):
                if not self.file_filter or re.search(self.file_filter, file_name):
                    result.append(file_path)
        return result

    def move_file(self, original_and_new_file_paths: tuple[str, str]) -> None:
        """Moves a file at the given path into the ingest directory, updating the name
        to always have an prefix of 'unprocessed'. The result of the file move will
        later be written to a log file.

        If in dry_run mode, merely logs the move, but does not execute it.
        """
        original_file_path, new_file_path = original_and_new_file_paths
        if not self.dry_run:
            gsutil_mv(original_file_path, new_file_path)

    def build_moved_file_path(self, original_file_path: str) -> str:
        """Builds the desired path for the given file in the ingest bucket, changing the
        prefix to 'unprocessed' as is necessary.
        """

        path_as_unprocessed = to_normalized_unprocessed_file_path_from_normalized_path(
            original_file_path
        )

        _, file_name = os.path.split(path_as_unprocessed)

        if not re.match(self.FILE_TO_MOVE_RE, file_name):
            raise ValueError(f"Invalid file name {file_name}")

        return os.path.join(
            "gs://", self.destination_ingest_bucket.abs_path(), file_name
        )

    def write_moves_to_log_file(self) -> None:
        self.successful_moves_list.sort()
        self.failed_moves_list.sort()
        with open(self.log_output_path, "w", encoding="utf-8") as f:
            if self.dry_run:
                prefix = "[DRY RUN] Would move"
            else:
                prefix = "Moved"

            f.writelines(
                f"{prefix} {original_path} -> {new_path}\n"
                for original_path, new_path in self.successful_moves_list
            )
            f.writelines(
                f"⚠️FAILED TO MOVE {original_path} -> {new_path}\n"
                for original_path, new_path in self.failed_moves_list
            )


def main() -> None:
    """Runs the move_state_files_to_storage script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--source-project-id",
        choices=DATA_PLATFORM_GCP_PROJECTS,
        required=True,
        help="Which project's files should be moved from (e.g. recidiviz-123).",
    )

    parser.add_argument(
        "--destination-project-id",
        choices=DATA_PLATFORM_GCP_PROJECTS,
        required=True,
        help="Which project's files should be moved to (e.g. recidiviz-123).",
    )

    parser.add_argument(
        "--source-raw-data-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="Used to identify which instance ingest bucket the raw data should be moved from.",
        required=True,
    )

    parser.add_argument(
        "--destination-raw-data-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="Used to identify which instance ingest bucket the raw data should be moved to.",
        required=True,
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

    with local_project_id_override(args.destination_project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            MoveFilesFromStorageController(
                source_project_id=args.source_project_id,
                destination_project_id=args.destination_project_id,
                source_raw_data_instance=args.source_raw_data_instance,
                destination_raw_data_instance=args.destination_raw_data_instance,
                region=args.region,
                end_date_bound=args.end_date_bound,
                start_date_bound=args.start_date_bound,
                dry_run=args.dry_run,
                file_filter=args.file_filter,
            ).run_move()


if __name__ == "__main__":
    main()
