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
Script for moving files from storage back into an ingest bucket to be re-ingested.

Steps:
1. Finds all sub-folders in storage for dates we want to re-ingest, based on
   start-date-bound, end-date-bound and file tag filters.
2. Finds all files in those sub-folders.
3. Acquires raw data resource lock for the destination ingest bucket
4. Executes all operations on all found files to the ingest bucket, updating the file type to destination-file-type.
5. Writes operations to a logfile.
6. Release raw data resource lock for the ingest bucket

Example usage:

uv run python -m recidiviz.tools.ingest.operations.move_raw_state_files_from_storage \
    --source-project-id recidiviz-staging --source-raw-data-instance PRIMARY \
    --destination-project-id recidiviz-staging --destination-raw-data-instance SECONDARY \
    --region us_tn --start-date-bound 2025-03-10 --dry-run True


If you wanted to *copy* instead of *move* the files over, consider running the following command:

python -m recidiviz.tools.ingest.operations.move_raw_state_files_from_storage \
    --source-project-id recidiviz-staging --source-raw-data-instance PRIMARY \
    --destination-project-id recidiviz-staging --destination-raw-data-instance SECONDARY \
    --region us_tn --start-date-bound 2025-03-10 --operation-type COPY --dry-run True

you might want to do this in cases where you are doing a secondary re-import or partially 
re-ingesting a specific file in primary and want the files to remain in primary storage 
for the duration of the re-import.
"""
import argparse
import datetime
import logging
import re
from typing import List, Optional

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import parse_date
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
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
from recidiviz.tools.deploy.logging import redirect_logging_to_file
from recidiviz.tools.ingest.operations.helpers.cloud_storage_utils import (
    get_storage_directories_containing_raw_files,
)
from recidiviz.tools.ingest.operations.helpers.operate_on_raw_storage_directories_controller import (
    IngestFilesOperationType,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.future_executor import map_fn_with_progress_bar_results
from recidiviz.utils.log_helpers import make_log_output_path
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


class OperateOnRawStorageFilesController:
    """Class that executes file operations from direct ingest state storage to the
    appropriate state ingest bucket.
    """

    RESOURCE_LOCKS_REQUIRED = (DirectIngestRawDataResourceLockResource.BUCKET,)

    def __init__(
        self,
        *,
        source_project_id: str,
        destination_project_id: str,
        source_raw_data_instance: DirectIngestInstance,
        destination_raw_data_instance: DirectIngestInstance,
        region: str,
        start_date_bound: datetime.date | None,
        end_date_bound: datetime.date | None,
        dry_run: bool,
        operation_type: IngestFilesOperationType,
        file_filter: Optional[str],
        skip_confirmation: bool = False,
    ):
        self.source_project_id = source_project_id
        self.destination_project_id = destination_project_id
        self.region = region
        self.state_code = StateCode(region.upper())
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.dry_run = dry_run
        self.file_filter = file_filter
        self.operation_type = operation_type
        self.skip_confirmation = skip_confirmation

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

        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())

        self.successful_operations_list: List[tuple[GcsfsFilePath, GcsfsFilePath]] = []
        self.failed_operations_list: List[GcsfsFilePath] = []

        self.lock_manager = DirectIngestRawDataResourceLockManager(
            region_code=self.region,
            raw_data_source_instance=self.destination_raw_data_instance,
            with_proxy=True,
        )

        date_string = f"start_bound_{start_date_bound.isoformat() if start_date_bound else None}_end_bound_{end_date_bound.isoformat() if end_date_bound else None}"

        self.log_output_path = make_log_output_path(
            operation_name=operation_type.value.lower(),
            region_code=region,
            date_string=date_string,
            dry_run=dry_run,
        )

    def run(self) -> None:
        """Main method of script - executes operations, or runs a dry run of the
        operations
        """

        if self.dry_run:
            logging.info("Running in DRY RUN mode for region [%s]", self.region)

        prompt_for_confirmation(
            f"This will {self.operation_type.value.lower()} [{self.region}] files from "
            f"[{self.source_project_id}] storage that were uploaded starting on date "
            f"[{self.start_date_bound}] and ending on date [{self.end_date_bound}] to "
            f"ingest bucket in [{self.destination_project_id}].",
            dry_run=self.dry_run,
            skip_confirmation=self.skip_confirmation,
        )

        logging.info("Finding files to %s...", self.operation_type.value.lower())
        date_subdir_paths = get_storage_directories_containing_raw_files(
            fs=self.fs,
            storage_bucket_path=self.source_storage_bucket,
            upper_bound_date_inclusive=self.end_date_bound,
            lower_bound_date_inclusive=self.start_date_bound,
        )

        prompt_for_confirmation(
            f"Found [{len(date_subdir_paths)}] dates to {self.operation_type.value.lower()} - continue?",
            dry_run=self.dry_run,
            skip_confirmation=self.skip_confirmation,
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
                ttl_seconds=4 * 60 * 60,  # 4 hours
            )

        try:
            files_to_operate_on = self.collect_files_to_operate_on(date_subdir_paths)
            self.execute_operations(files_to_operate_on)
            self.write_operations_to_log_file()

            if self.dry_run:
                logging.info(
                    "[DRY RUN] See results in [%s].\n"
                    "Rerun with [--dry-run False] to execute %s.",
                    self.log_output_path,
                    self.operation_type.value.lower(),
                )
            else:
                logging.info(
                    "%s complete! See results in [%s].\n",
                    self.operation_type.value.title(),
                    self.log_output_path,
                )
        finally:
            if self.dry_run:
                logging.info("DRY RUN: would release locks...")
            else:
                logging.info("Releasing locks...")
                for lock in resource_locks:
                    self.lock_manager.release_lock_by_id(lock.lock_id)

    def collect_files_to_operate_on(
        self, date_subdir_paths: List[GcsfsDirectoryPath]
    ) -> List[GcsfsFilePath]:
        """Searches the given list of directory paths for files directly in those directories
        that should be moved /copied to the ingest directory and returns a list of string
        paths to those files.
        """
        msg_prefix = "[DRY RUN] " if self.dry_run else ""

        result = map_fn_with_progress_bar_results(
            work_items=date_subdir_paths,
            work_fn=self.get_files_to_operate_on_from_path,
            progress_bar_message=f"{msg_prefix} Gathering paths to {self.operation_type.value.lower()}...",
            single_work_item_timeout_sec=60 * 20,  # 20 minute timeout
            overall_timeout_sec=60 * 60 * 4,  # 4 hour timeout
        )

        if result.exceptions:
            raise ExceptionGroup(
                "Failed to search subdirectories", [e[1] for e in result.exceptions]
            )

        return [f for _subdir, sublist in result.successes for f in sublist]

    def execute_operations(self, files_to_operate_on: List[GcsfsFilePath]) -> None:
        """Executes file operations on |files_to_operate_on| from storage to the ingest
        bucket, changing the prefix to 'unprocessed' as necessary.

        For the given list of file paths:

        files_to_operate_on = [
            'storage_bucket/path/to/processed_2019-09-24T09:01:20:039807_elite_offendersentenceterms.csv'
        ]

        Will run:
        gsutil mv/cp
            gs://storage_bucket/path/to/processed_2019-09-24T09:01:20:039807_elite_offendersentenceterms.csv \
            unprocessed_2019-09-24T09:01:20:039807_elite_offendersentenceterms.csv
        """
        msg_prefix = "[DRY RUN] " if self.dry_run else ""
        with redirect_logging_to_file(self.log_output_path):
            result = map_fn_with_progress_bar_results(
                work_items=files_to_operate_on,
                work_fn=self.execute_operation,
                progress_bar_message=f"{msg_prefix} {self.operation_type.present_participle().title()} files...",
                single_work_item_timeout_sec=60 * 20,  # 20 minute timeout
                overall_timeout_sec=60 * 60 * 4,  # 4 hour timeout
            )

        self.successful_operations_list.extend(
            (input_path, output_path) for input_path, output_path in result.successes
        )
        self.failed_operations_list.extend(paths for paths, _ in result.exceptions)

    def get_files_to_operate_on_from_path(
        self, subdir_path: GcsfsDirectoryPath
    ) -> List[GcsfsFilePath]:
        """Returns files in the provided |gs_dir_path| that should be moved/copied back
        into the ingest bucket.
        """
        result = []
        for file_path in self.fs.ls(
            bucket_name=subdir_path.bucket_name, blob_prefix=subdir_path.relative_path
        ):
            if isinstance(file_path, GcsfsDirectoryPath):
                continue
            # continue if the
            # - path conforms to our naming conventions
            # - matches the provided file filter regex, if one exists
            if self.fs.is_normalized_file_path(file_path) and (
                not self.file_filter or re.search(self.file_filter, file_path.file_name)
            ):
                result.append(file_path)
        return result

    def execute_operation(self, src_path: GcsfsFilePath) -> GcsfsFilePath:
        """Executes the operation on a file at the given path into the ingest bucket,
        updating the name to always have an prefix of 'unprocessed'. The result of the
        file operation will later be written to a log file.

        If in dry_run mode, merely logs the operation, but does not execute it.
        """
        if not self.dry_run:
            match self.operation_type:
                case IngestFilesOperationType.COPY:
                    return self.fs.cp_storage_file_to_ingest_bucket(
                        src_path, self.destination_ingest_bucket
                    )
                case IngestFilesOperationType.MOVE:
                    return self.fs.mv_storage_file_to_ingest_bucket(
                        src_path, self.destination_ingest_bucket
                    )
        else:
            return self.fs.build_ingest_destination_path_for_storage_path(
                src_path, self.destination_ingest_bucket
            )

    def write_operations_to_log_file(self) -> None:
        self.successful_operations_list.sort()
        self.failed_operations_list.sort()
        with open(self.log_output_path, "a", encoding="utf-8") as f:
            if self.dry_run:
                prefix = f"[DRY RUN] Would {self.operation_type.value.lower()}"
            else:
                prefix = f"Successfully {self.operation_type.past_participle().lower()}"

            f.writelines(
                f"{prefix} {source_path.uri()} -> {dest_path.uri()}\n"
                for source_path, dest_path in self.successful_operations_list
            )
            f.writelines(
                f"⚠️⚠️⚠️FAILED TO {self.operation_type.value} {original_path.uri()} \n"
                for original_path in self.failed_operations_list
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

    parser.add_argument(
        "--operation-type",
        default=IngestFilesOperationType.MOVE,
        type=IngestFilesOperationType,
        help="Whether the files should be moved or copied. By default, we will move the files.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with local_project_id_override(args.destination_project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            OperateOnRawStorageFilesController(
                source_project_id=args.source_project_id,
                destination_project_id=args.destination_project_id,
                source_raw_data_instance=args.source_raw_data_instance,
                destination_raw_data_instance=args.destination_raw_data_instance,
                region=args.region,
                end_date_bound=(
                    parse_date(args.end_date_bound)
                    if args.end_date_bound is not None
                    else None
                ),
                start_date_bound=(
                    parse_date(args.start_date_bound)
                    if args.start_date_bound is not None
                    else None
                ),
                dry_run=args.dry_run,
                file_filter=args.file_filter,
                operation_type=args.operation_type,
            ).run()


if __name__ == "__main__":
    main()
