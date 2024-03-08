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

"""Functionality to perform direct ingest.
"""
import datetime
import logging
import os
from types import ModuleType
from typing import List, Optional

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockManager
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestCloudTaskQueueManagerImpl,
    build_handle_new_files_task_id,
    build_scheduler_task_id,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_deprecated_storage_directory_path_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
    gcsfs_direct_ingest_temporary_output_directory_path,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsRawDataBQImportArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.monitoring import trace
from recidiviz.persistence.database.schema.operations.dao import (
    stale_secondary_raw_data,
)
from recidiviz.utils import environment

_RAW_FILE_IMPORT_INGEST_PROCESS_RUNNING_LOCK_PREFIX = "INGEST_PROCESS_RUNNING_RAW_FILE_"


# TODO(#20930): Rename this class/file and related test classes/files to
#  IngestRawFileImportController.
class BaseDirectIngestController:
    """Parses and persists individual-level info from direct ingest partners."""

    def __init__(
        self,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        region_module_override: Optional[ModuleType],
    ) -> None:
        """Initialize the controller."""
        self.state_code = state_code
        self.region_module_override = region_module_override
        self.cloud_task_manager = DirectIngestCloudTaskQueueManagerImpl()
        self.ingest_instance = ingest_instance
        self.lock_manager = GCSPseudoLockManager()
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.instance_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code=self.region.region_code, ingest_instance=self.ingest_instance
        )
        self.csv_reader = GcsfsCsvReader(GcsfsFactory.build())

        self.ingest_instance_status_manager = DirectIngestInstanceStatusManager(
            region_code=self.region.region_code,
            ingest_instance=self.ingest_instance,
        )

        self.temp_output_directory_path = (
            gcsfs_direct_ingest_temporary_output_directory_path()
        )

        self.big_query_client = BigQueryClientImpl()

        self.raw_file_metadata_manager = DirectIngestRawFileMetadataManager(
            region_code=self.region.region_code,
            raw_data_instance=self.ingest_instance,
        )

        self.raw_file_import_manager = DirectIngestRawFileImportManager(
            region=self.region,
            fs=self.fs,
            temp_output_directory_path=self.temp_output_directory_path,
            big_query_client=self.big_query_client,
            csv_reader=self.csv_reader,
            instance=self.ingest_instance,
        )

    @property
    def raw_data_bucket_path(self) -> GcsfsBucketPath:
        return gcsfs_direct_ingest_bucket_for_state(
            region_code=self.region.region_code,
            ingest_instance=self.ingest_instance,
        )

    @property
    def raw_data_storage_directory_path(self) -> GcsfsDirectoryPath:
        return gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=self.region_code(),
            ingest_instance=self.ingest_instance,
        )

    def raw_data_deprecated_storage_directory_path(
        self, deprecated_on_date: datetime.date
    ) -> GcsfsDirectoryPath:
        return gcsfs_direct_ingest_deprecated_storage_directory_path_for_state(
            region_code=self.region_code(),
            ingest_instance=self.ingest_instance,
            deprecated_on_date=deprecated_on_date,
        )

    @property
    def region(self) -> DirectIngestRegion:
        return direct_ingest_regions.get_direct_ingest_region(
            self.region_code().lower(),
            region_module_override=self.region_module_override,
        )

    def region_code(self) -> str:
        return self.state_code.value.lower()

    def _ingest_lock_name_for_raw_file(self, raw_file_tag: str) -> str:
        return (
            _RAW_FILE_IMPORT_INGEST_PROCESS_RUNNING_LOCK_PREFIX
            + self.region_code().upper()
            + f"_{self.ingest_instance.name}"
            + f"_{raw_file_tag}"
        )

    def _ingest_is_not_running(self) -> bool:
        """Returns True if ingest is not running in this instance and all functions should
        early-return without doing any work.
        """
        current_status = self.ingest_instance_status_manager.get_current_status()

        return current_status in {
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
        }

    # ============== #
    # JOB SCHEDULING #
    # ============== #
    def kick_scheduler(self) -> None:
        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_scheduler_queue_task(
            region=self.region,
            ingest_instance=self.ingest_instance,
        )

    def _prune_redundant_tasks(self, current_task_id: str) -> None:
        """Prunes all tasks that match the type of the current task out of the scheduler
        queue, leaving the current task.
        """
        queue_info = self.cloud_task_manager.get_scheduler_queue_info(
            self.region, self.ingest_instance
        )
        scheduler_task_id_prefix = build_scheduler_task_id(
            self.region, self.ingest_instance, prefix_only=True
        )
        handle_new_files_task_id_prefix = build_handle_new_files_task_id(
            self.region, self.ingest_instance, prefix_only=True
        )
        if current_task_id.startswith(scheduler_task_id_prefix):
            task_id_prefix = scheduler_task_id_prefix
        elif current_task_id.startswith(handle_new_files_task_id_prefix):
            task_id_prefix = handle_new_files_task_id_prefix
        else:
            raise ValueError(f"Unexpected task_id: [{current_task_id}]")

        task_names = queue_info.task_names_for_task_id_prefix(task_id_prefix)
        pruned_task_count = 0
        for task_name in task_names:
            _, task_id = os.path.split(task_name)
            if task_id == current_task_id:
                continue
            self.cloud_task_manager.delete_scheduler_queue_task(
                self.region, self.ingest_instance, task_name
            )
            pruned_task_count += 1
        if pruned_task_count:
            logging.info(
                "Pruned [%s] duplicate tasks out of the queue.", pruned_task_count
            )

    def schedule_next_ingest_task(self, *, current_task_id: str) -> None:
        """Finds the next task(s) that need to be scheduled for ingest and queues
        them. Also prunes redundant tasks out of the scheduler queue, if they exist.
        """
        self._prune_redundant_tasks(current_task_id=current_task_id)
        self._schedule_next_ingest_task()

    def _schedule_next_ingest_task(self) -> None:
        """Internal helper for scheduling the next ingest task."""
        check_is_region_launched_in_env(self.region)

        if self._ingest_is_not_running():
            logging.info(
                "Ingest is not running in this instance - not proceeding and returning early.",
            )
            return

        if self._schedule_raw_data_import_tasks():
            self.ingest_instance_status_manager.change_status_to(
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
            )
            logging.info(
                "Found pre-ingest raw data import tasks to schedule - returning."
            )
            return

        # If there aren't any more tasks to schedule for the region, update to the appropriate next
        # status, based on the ingest instance.
        if self.ingest_instance == DirectIngestInstance.PRIMARY:
            logging.info(
                "Controller's instance is PRIMARY. No tasks to schedule and instance is now up to date."
            )
            self.ingest_instance_status_manager.change_status_to(
                DirectIngestStatus.RAW_DATA_UP_TO_DATE
            )
        elif stale_secondary_raw_data(self.region_code()):
            logging.info(
                "Controller's instance is SECONDARY. No tasks to schedule but secondary raw data is now "
                "stale."
            )
            self.ingest_instance_status_manager.change_status_to(
                DirectIngestStatus.STALE_RAW_DATA
            )
        else:
            logging.info(
                "Controller's instance is SECONDARY. No tasks to schedule and secondary is now ready to "
                "flash."
            )
            self.ingest_instance_status_manager.change_status_to(
                DirectIngestStatus.READY_TO_FLASH
            )

    def _schedule_raw_data_import_tasks(self) -> bool:
        """Schedules all pending raw data import tasks for launched ingest view tags, if
        they have not been scheduled. If tasks are scheduled or are still running,
        returns True. Otherwise, if it's safe to proceed with next steps of ingest,
        returns False.
        """
        # Fetch the queue information for where the raw data is being processed.
        queue_info = self.cloud_task_manager.get_raw_data_import_queue_info(
            region=self.region, ingest_instance=self.ingest_instance
        )

        raw_files_pending_import = (
            self.raw_file_metadata_manager.get_unprocessed_raw_files_eligible_for_import()
        )

        did_schedule = False
        unrecognized_file_tags = set()

        for raw_file_metadata in raw_files_pending_import:
            raw_data_file_path = GcsfsFilePath.from_directory_and_file_name(
                self.raw_data_bucket_path, raw_file_metadata.normalized_file_name
            )
            is_unrecognized_tag = (
                raw_file_metadata.file_tag
                not in self.raw_file_import_manager.region_raw_file_config.raw_file_tags
            )
            if is_unrecognized_tag:
                unrecognized_file_tags.add(raw_file_metadata.file_tag)
                continue

            task_args = GcsfsRawDataBQImportArgs(raw_data_file_path)
            if not queue_info.is_raw_data_import_task_already_queued(task_args):
                # Fetch the queue information for where the raw data is being processed.
                self.cloud_task_manager.create_direct_ingest_raw_data_import_task(
                    self.region, self.ingest_instance, task_args
                )
                did_schedule = True

        for file_tag in sorted(unrecognized_file_tags):
            logging.warning(
                "Unrecognized raw file tag [%s] for region [%s] - skipped import.",
                file_tag,
                self.region_code(),
            )
        return queue_info.has_raw_data_import_jobs_queued() or did_schedule

    def default_job_lock_timeout_in_seconds(self) -> int:
        """This method can be overridden by subclasses that need more (or less)
        time to process jobs to completion, but by default enforces a
        one hour timeout on locks.
        Jobs may take longer than the alotted time, but if they do so, they
        will de facto relinquish their hold on the acquired lock."""
        return 3600

    # ================= #
    # NEW FILE HANDLING #
    # ================= #
    def handle_file(self, path: GcsfsFilePath, start_ingest: bool) -> None:
        """Called when a single new file is added to an ingest bucket (may also
        be called as a result of a rename).
        May be called from any worker/queue.
        """
        if self.fs.is_processed_file(path):
            logging.info("File [%s] is already processed, returning.", path.abs_path())
            return

        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=self.region,
            ingest_instance=self.ingest_instance,
            can_start_ingest=start_ingest,
        )

    def _register_all_new_raw_file_paths_in_metadata(
        self, paths: List[GcsfsFilePath]
    ) -> None:
        for path in paths:
            if not self.raw_file_metadata_manager.has_raw_file_been_discovered(path):
                self.raw_file_metadata_manager.mark_raw_file_as_discovered(path)

    @trace.span
    def handle_new_files(self, *, current_task_id: str, can_start_ingest: bool) -> None:
        """Searches the ingest directory for new/unprocessed files. Normalizes
        new raw file names as necessary, then schedules the next ingest job if allowed.
        Should only be called from the scheduler queue.
        """
        if not can_start_ingest and self.region.is_ingest_launched_in_env():
            raise ValueError(
                "The can_start_ingest flag should only be used for regions where ingest is not yet launched in a "
                "particular environment. If we want to be able to selectively pause ingest processing for a state, we "
                "will first have to build a config that is respected by both the /ensure_all_raw_file_paths_normalized "
                "endpoint and any cloud functions that trigger ingest."
            )

        if self._ingest_is_not_running():
            logging.info(
                "Ingest is not running in this instance - not proceeding and returning early.",
            )
            return

        self._prune_redundant_tasks(current_task_id=current_task_id)

        unnormalized_paths = self.fs.get_unnormalized_file_paths(
            self.instance_bucket_path
        )

        for path in unnormalized_paths:
            logging.info("File [%s] is not yet seen, normalizing.", path.abs_path())
            self.fs.mv_raw_file_to_normalized_path(path)

        if unnormalized_paths:
            logging.info(
                "Normalized at least one path - returning, will handle "
                "normalized files separately."
            )
            # Normalizing file paths will cause the cloud function that calls
            # this function to be re-triggered.
            return

        if not can_start_ingest:
            logging.warning(
                "Ingest not configured to start post-file normalization - returning."
            )
            return

        check_is_region_launched_in_env(self.region)

        unprocessed_raw_paths = self.fs.get_unprocessed_raw_file_paths(
            self.raw_data_bucket_path
        )

        unprocessed_zip_files = [
            p for p in unprocessed_raw_paths if p.has_zip_extension
        ]
        for zip_path in unprocessed_zip_files:
            logging.info("File [%s] is a zip file, unzipping...", zip_path.abs_path())
            parts = filename_parts_from_path(zip_path)
            unzipped_files = self.fs.unzip(
                zip_path, destination_dir=self.raw_data_bucket_path
            )
            # Normalize all the internal file paths with the same date as the outer file
            for internal_path in unzipped_files:
                self.fs.mv_raw_file_to_normalized_path(
                    internal_path, dt=parts.utc_upload_datetime
                )
            self.fs.mv(
                src_path=zip_path,
                dst_path=self.raw_data_deprecated_storage_directory_path(
                    deprecated_on_date=datetime.date.today()
                ),
            )
        if unprocessed_zip_files:
            logging.info("Unzipped at least one path - returning")
            # Normalizing file paths will cause the cloud function that calls
            # this function to be re-triggered.
            return

        self._register_all_new_raw_file_paths_in_metadata(unprocessed_raw_paths)

        # Even if there are no unprocessed paths, we still want to look for the next
        # job because there may be new files to generate.
        self._schedule_next_ingest_task()

    def do_raw_data_import(self, data_import_args: GcsfsRawDataBQImportArgs) -> None:
        """Process a raw incoming file by importing it to BQ, tracking it in our metadata tables, and moving it to
        storage on completion.
        """
        check_is_region_launched_in_env(self.region)

        if self._ingest_is_not_running():
            logging.info(
                "Ingest is not running in this instance - not proceeding and returning early.",
            )
            return

        try:
            file_metadata = self.raw_file_metadata_manager.get_raw_file_metadata(
                data_import_args.raw_data_file_path
            )
        except ValueError:
            # If there is no operations DB row and the file doesn't exist, then
            # this file was likely deprecated fully.
            file_metadata = None

        if not self.fs.exists(data_import_args.raw_data_file_path):
            if file_metadata is not None and file_metadata.file_processed_time is None:
                raise ValueError(
                    f"Attempting to run raw data import for raw data path "
                    f"[{data_import_args.raw_data_file_path}] which does not exist "
                    f"but still has an unprocessed row in the operations DB. This "
                    f"likely happened because some raw data files were deprecated "
                    f"but the corresponding rows were not cleaned out of the "
                    f"operations DB."
                )

            logging.warning(
                "File path [%s] no longer exists - might have already been "
                "processed or deleted",
                data_import_args.raw_data_file_path,
            )
            self.kick_scheduler()
            return

        if file_metadata is None:
            raise ValueError(
                f"No metadata row for file [{data_import_args.raw_data_file_path}]."
            )

        if file_metadata.file_processed_time:
            logging.warning(
                "File [%s] is already marked as processed. Skipping file processing.",
                data_import_args.raw_data_file_path.file_name,
            )
            self.kick_scheduler()
            return

        # Grab the lock for this file tag so no other tasks for the same file tag can
        # run at the same time.
        with self.lock_manager.using_lock(
            self._ingest_lock_name_for_raw_file(data_import_args.file_id()),
            expiration_in_seconds=self.default_job_lock_timeout_in_seconds(),
        ):
            self.raw_file_import_manager.import_raw_file_to_big_query(
                data_import_args.raw_data_file_path, file_metadata
            )

            self.raw_file_metadata_manager.mark_raw_file_as_processed(
                path=data_import_args.raw_data_file_path
            )
            processed_path = self.fs.mv_path_to_processed_path(
                data_import_args.raw_data_file_path
            )

            self.fs.mv_raw_file_to_storage(
                processed_path, self.raw_data_storage_directory_path
            )
            should_schedule = True

        if should_schedule:
            self.kick_scheduler()


def check_is_region_launched_in_env(region: DirectIngestRegion) -> None:
    """Checks if direct ingest has been launched for the provided |region| in the current GCP env and throws if it has
    not."""
    if not region.is_ingest_launched_in_env():
        gcp_env = environment.get_gcp_environment()
        error_msg = f"Bad environment [{gcp_env}] for region [{region.region_code}]."
        logging.error(error_msg)
        raise DirectIngestError(
            msg=error_msg, error_type=DirectIngestErrorType.ENVIRONMENT_ERROR
        )
