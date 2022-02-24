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
import abc
import datetime
import logging
import os
from types import ModuleType
from typing import List, Optional

import pandas

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcs_file_system import GCSBlobDoesNotExistError
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockAlreadyExists
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    ReadOneGcsfsCsvReaderDelegate,
    SplittingGcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import (
    IngestMetadata,
    LegacyStateAndJailsIngestMetadata,
    SystemLevel,
)
from recidiviz.common.io.contents_handle import ContentsHandle
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    SPLIT_FILE_SUFFIX,
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_file_path,
)
from recidiviz.ingest.direct.controllers.direct_ingest_ingest_view_export_manager import (
    DirectIngestIngestViewExportManager,
)
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.ingest.direct.controllers.extract_and_merge_job_prioritizer import (
    ExtractAndMergeJobPrioritizer,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_job_prioritizer import (
    GcsfsDirectIngestJobPrioritizer,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    ExtractAndMergeArgs,
    GcsfsDirectIngestFileType,
    GcsfsIngestViewExportArgs,
    GcsfsRawDataBQImportArgs,
    LegacyExtractAndMergeArgs,
    filename_parts_from_path,
    gcsfs_direct_ingest_storage_directory_path_for_region,
    gcsfs_direct_ingest_temporary_output_directory_path,
)
from recidiviz.ingest.direct.controllers.ingest_view_processor import (
    IngestViewProcessor,
    IngestViewProcessorImpl,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
    build_handle_new_files_task_id,
    build_scheduler_task_id,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_file_parser import (
    MANIFEST_LANGUAGE_VERSION_KEY,
    IngestViewFileParser,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_file_parser_delegate import (
    IngestViewFileParserDelegateImpl,
    yaml_mappings_filepath,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.legacy_ingest_view_processor import (
    LegacyIngestViewProcessor,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestFileMetadataManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.direct_ingest_instance_factory import (
    DirectIngestInstanceFactory,
)
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestIngestFileMetadata,
)
from recidiviz.utils import environment, regions, trace
from recidiviz.utils.regions import Region
from recidiviz.utils.yaml_dict import YAMLDict


class DirectIngestFileSplittingGcsfsCsvReaderDelegate(SplittingGcsfsCsvReaderDelegate):
    def __init__(
        self,
        path: GcsfsFilePath,
        fs: DirectIngestGCSFileSystem,
        output_directory_path: GcsfsDirectoryPath,
    ):
        super().__init__(path, fs, include_header=True)
        self.output_directory_path = output_directory_path

    def transform_dataframe(self, df: pandas.DataFrame) -> pandas.DataFrame:
        return df

    def get_output_path(self, chunk_num: int) -> GcsfsFilePath:
        name, _extension = os.path.splitext(self.path.file_name)

        return GcsfsFilePath.from_directory_and_file_name(
            self.output_directory_path, f"temp_direct_ingest_{name}_{chunk_num}.csv"
        )


class BaseDirectIngestController:
    """Parses and persists individual-level info from direct ingest partners."""

    _INGEST_FILE_SPLIT_LINE_LIMIT = 2500

    def __init__(
        self,
        ingest_bucket_path: GcsfsBucketPath,
        region_module_override: Optional[ModuleType] = None,
    ) -> None:
        """Initialize the controller."""
        self.region_module_override = region_module_override
        self.cloud_task_manager = DirectIngestCloudTaskManagerImpl()
        self.ingest_instance = DirectIngestInstanceFactory.for_ingest_bucket(
            ingest_bucket_path
        )
        self.region_lock_manager = DirectIngestRegionLockManager.for_direct_ingest(
            region_code=self.region.region_code,
            schema_type=self.system_level.schema_type(),
            ingest_instance=self.ingest_instance,
        )
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.ingest_bucket_path = ingest_bucket_path
        self.storage_directory_path = (
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code=self.region_code(),
                system_level=self.system_level,
                ingest_instance=self.ingest_instance,
            )
        )

        self.temp_output_directory_path = (
            gcsfs_direct_ingest_temporary_output_directory_path()
        )

        # TODO(#9717): We will need to select the prioritizer based on where we are
        #  reading ingest view results from (BQ vs GCS). Eventually this will always be
        #  set to the BQ-based implementation.
        self.job_prioritizer: ExtractAndMergeJobPrioritizer = (
            GcsfsDirectIngestJobPrioritizer(
                self.fs,
                self.ingest_bucket_path,
                self.get_file_tag_rank_list(),
            )
        )

        self.ingest_file_split_line_limit = self._INGEST_FILE_SPLIT_LINE_LIMIT

        self.file_metadata_manager = PostgresDirectIngestFileMetadataManager(
            region_code=self.region.region_code,
            ingest_database_name=self.ingest_database_key.db_name,
        )

        self.raw_file_import_manager = DirectIngestRawFileImportManager(
            region=self.region,
            fs=self.fs,
            ingest_bucket_path=self.ingest_bucket_path,
            temp_output_directory_path=self.temp_output_directory_path,
            big_query_client=BigQueryClientImpl(),
        )

        self.ingest_view_export_manager = DirectIngestIngestViewExportManager(
            region=self.region,
            fs=self.fs,
            ingest_instance=self.ingest_instance,
            output_bucket_name=self.ingest_bucket_path.bucket_name,
            file_metadata_manager=self.file_metadata_manager,
            big_query_client=BigQueryClientImpl(),
            view_collector=DirectIngestPreProcessedIngestViewCollector(
                self.region, self.get_file_tag_rank_list()
            ),
            launched_file_tags=self.get_file_tag_rank_list(),
        )

        self.ingest_instance_status_manager = DirectIngestInstanceStatusManager(
            self.region_code(), self.ingest_instance
        )
        self.csv_reader = GcsfsCsvReader(GcsfsFactory.build())

    @property
    def region(self) -> Region:
        return regions.get_region(
            self.region_code().lower(),
            is_direct_ingest=True,
            region_module_override=self.region_module_override,
        )

    @classmethod
    @abc.abstractmethod
    def region_code(cls) -> str:
        pass

    # TODO(#9717): Rename this to something like `get_ingest_view_processing_order`
    #  since ingest view results will soon no longer be file-based.
    @abc.abstractmethod
    def get_file_tag_rank_list(self) -> List[str]:
        """Returns the list of ingest view names for ingest views that are shipped in
        the current environment and whose results can be processed and commiteed to
        our central data model.
        """

    @property
    def system_level(self) -> SystemLevel:
        return SystemLevel.for_region(self.region)

    @property
    def ingest_database_key(self) -> SQLAlchemyDatabaseKey:
        schema_type = self.system_level.schema_type()
        if schema_type == SchemaType.STATE:
            state_code = StateCode(self.region_code().upper())
            return self.ingest_instance.database_key_for_state(
                state_code,
            )

        return SQLAlchemyDatabaseKey.for_schema(schema_type)

    # ============== #
    # JOB SCHEDULING #
    # ============== #
    def kick_scheduler(self, just_finished_job: bool) -> None:
        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_scheduler_queue_task(
            region=self.region,
            ingest_bucket=self.ingest_bucket_path,
            just_finished_job=just_finished_job,
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

    def schedule_next_ingest_task(
        self, *, current_task_id: str, just_finished_job: bool
    ) -> None:
        """Finds the next task(s) that need to be scheduled for ingest and queues
        them. Also prunes redundant tasks out of the scheduler queue, if they exist.
        """
        self._prune_redundant_tasks(current_task_id=current_task_id)
        self._schedule_next_ingest_task(just_finished_job=just_finished_job)

    def _schedule_next_ingest_task(self, just_finished_job: bool) -> None:
        """Internal helper for scheduling the next ingest task. DOes"""
        check_is_region_launched_in_env(self.region)

        if self.ingest_instance_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_bucket_path.uri()
            )
            return

        if self._schedule_any_pre_ingest_tasks():
            logging.info("Found pre-ingest tasks to schedule - returning.")
            return

        if self.region_lock_manager.is_locked():
            logging.info("Direct ingest is already locked on region [%s]", self.region)
            return

        process_job_queue_info = self.cloud_task_manager.get_process_job_queue_info(
            self.region, self.ingest_instance
        )
        if (
            process_job_queue_info.has_any_tasks_for_instance(
                region_code=self.region_code(), ingest_instance=self.ingest_instance
            )
            and not just_finished_job
        ):
            logging.info(
                "Already running job [%s] - will not schedule another job for "
                "region [%s]",
                process_job_queue_info.task_names[0],
                self.region.region_code,
            )
            return

        next_job_args = self._get_next_job_args()

        if not next_job_args:
            logging.info(
                "No more extract and merge to run for region [%s] - returning",
                self.region.region_code,
            )
            return

        if process_job_queue_info.is_task_already_queued(
            self.region_code(), next_job_args
        ):
            logging.info(
                "Already have task queued for next extract and merge job [%s] - returning.",
                next_job_args.job_tag(),
            )
            return

        if not self.region_lock_manager.can_proceed():
            logging.info(
                "Postgres to BigQuery export is running, cannot run ingest - returning"
            )
            return

        logging.info(
            "Creating cloud task to run extract and merge job [%s]",
            next_job_args.job_tag(),
        )

        # TODO(#9717): Implement queueing for BQ-based extract and merge job.
        if isinstance(next_job_args, LegacyExtractAndMergeArgs):
            self.cloud_task_manager.create_direct_ingest_process_job_task(
                region=self.region,
                ingest_args=next_job_args,
            )
        else:
            raise ValueError(f"Unexpected args type: [{type(next_job_args)}]")

    def _schedule_any_pre_ingest_tasks(self) -> bool:
        """Schedules any tasks related to SQL preprocessing of new files in preparation
         for ingest of those files into our Postgres database.

        Returns True if any jobs were scheduled or if there were already any pre-ingest
        jobs scheduled. Returns False if there are no remaining ingest jobs to schedule
        and it is safe to proceed with ingest.
        """
        if self._schedule_raw_data_import_tasks():
            logging.info("Found pre-ingest raw data import tasks to schedule.")
            return True
        if self._schedule_ingest_view_export_tasks():
            logging.info("Found pre-ingest view export tasks to schedule.")
            return True
        return False

    def _schedule_raw_data_import_tasks(self) -> bool:
        """Schedules all pending ingest view export tasks for launched ingest view tags,
        if they have not been scheduled. If tasks are scheduled or are still running,
        returns True. Otherwise, if it's safe to proceed with next steps of ingest,
        returns False."""
        queue_info = self.cloud_task_manager.get_raw_data_import_queue_info(self.region)

        did_schedule = False
        tasks_to_schedule = [
            GcsfsRawDataBQImportArgs(path)
            for path in self.raw_file_import_manager.get_unprocessed_raw_files_to_import()
        ]
        for task_args in tasks_to_schedule:
            # If the file path has not actually been discovered by the metadata manager yet, it likely was just added
            # and a subsequent call to handle_files will register it and trigger another call to this function so we can
            # schedule the appropriate job.
            discovered = self.file_metadata_manager.has_raw_file_been_discovered(
                task_args.raw_data_file_path
            )
            # If the file path has been processed, but still in the GCS bucket, it's likely due
            # to either a manual move or an accidental duplicate uploading. In either case, we
            # trust the database to have the source of truth.
            processed = self.file_metadata_manager.has_raw_file_been_processed(
                task_args.raw_data_file_path
            )
            if processed:
                logging.warning(
                    "File [%s] is already marked as processed. Skipping file processing.",
                    task_args.raw_data_file_path,
                )
            if (
                discovered
                and not processed
                and not queue_info.is_raw_data_import_task_already_queued(task_args)
            ):
                self.cloud_task_manager.create_direct_ingest_raw_data_import_task(
                    self.region, task_args
                )
                did_schedule = True

        return queue_info.has_raw_data_import_jobs_queued() or did_schedule

    def _schedule_ingest_view_export_tasks(self) -> bool:
        """Schedules all pending ingest view export tasks for launched ingest view tags,
         if they have not been scheduled. If tasks are scheduled or are still running,
        returns True. Otherwise, if it's safe to proceed with next steps of ingest,
        returns False.
        """
        queue_info = self.cloud_task_manager.get_ingest_view_export_queue_info(
            self.region, self.ingest_instance
        )
        if queue_info.has_ingest_view_export_jobs_queued(
            self.region_code(), self.ingest_instance
        ):
            # Since we schedule all export jobs at once, after all raw files have been processed, we wait for all of the
            # export jobs to be done before checking if we need to schedule more.
            return True

        did_schedule = False
        tasks_to_schedule = (
            self.ingest_view_export_manager.get_ingest_view_export_task_args()
        )

        rank_list = self.get_file_tag_rank_list()
        ingest_view_name_rank = {
            ingest_view_name: i for i, ingest_view_name in enumerate(rank_list)
        }

        # Filter out views that aren't in ingest view tags.
        filtered_tasks_to_schedule = []
        for args in tasks_to_schedule:
            if args.ingest_view_name not in ingest_view_name_rank:
                logging.warning(
                    "Skipping ingest view task export for [%s] - not in controller ingest tags.",
                    args.ingest_view_name,
                )
                continue
            filtered_tasks_to_schedule.append(args)

        tasks_to_schedule = filtered_tasks_to_schedule

        # Sort by tag order and export datetime
        tasks_to_schedule.sort(
            key=lambda args_: (
                ingest_view_name_rank[args_.ingest_view_name],
                args_.upper_bound_datetime_to_export,
            )
        )

        for task_args in tasks_to_schedule:
            if not queue_info.is_ingest_view_export_task_already_queued(
                self.region_code(),
                task_args,
            ):
                self.cloud_task_manager.create_direct_ingest_ingest_view_export_task(
                    self.region, task_args
                )
                did_schedule = True

        return did_schedule

    def _get_next_job_args(self) -> Optional[ExtractAndMergeArgs]:
        """Returns args for the next ingest job, or None if there is nothing to process."""
        args = self.job_prioritizer.get_next_job_args()

        if not args:
            return None

        # TODO(#9717): We will not need this logic anymore once we've migrated to a
        #  BQ-based implementation, but we may need to add other checks to manage
        #  race conditions between results generation and processing.
        if isinstance(args, LegacyExtractAndMergeArgs):
            discovered = (
                self.file_metadata_manager.has_ingest_view_file_been_discovered(
                    args.file_path
                )
            )

            if not discovered:
                # If the file path has not actually been discovered by the controller yet, it likely was just added and a
                # subsequent call to handle_files will register it and trigger another call to this function so we can
                # schedule the appropriate job.
                logging.info(
                    "Found args [%s] for a file that has not been discovered by the metadata manager yet - not scheduling.",
                    args,
                )
                return None

        return args

    # =================== #
    # SINGLE JOB RUN CODE #
    # =================== #
    def default_job_lock_timeout_in_seconds(self) -> int:
        """This method can be overridden by subclasses that need more (or less)
        time to process jobs to completion, but by default enforces a
        one hour timeout on locks.

        Jobs may take longer than the alotted time, but if they do so, they
        will de facto relinquish their hold on the acquired lock."""
        return 3600

    def run_extract_and_merge_job_and_kick_scheduler_on_completion(
        self, args: ExtractAndMergeArgs
    ) -> None:
        check_is_region_launched_in_env(self.region)

        if self.ingest_instance_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_bucket_path.uri()
            )
            return

        if not self.region_lock_manager.can_proceed():
            logging.warning(
                "Postgres to BigQuery export is running, can not run ingest"
            )
            raise GCSPseudoLockAlreadyExists(
                "Postgres to BigQuery export is running, can not run ingest"
            )

        with self.region_lock_manager.using_region_lock(
            expiration_in_seconds=self.default_job_lock_timeout_in_seconds(),
        ):
            should_schedule = self._run_ingest_job(args)

        if should_schedule:
            self.kick_scheduler(just_finished_job=True)
            logging.info("Done running task. Returning.")

    def _run_ingest_job(self, args: ExtractAndMergeArgs) -> bool:
        """
        Runs the full extract and merge process for this controller - reading and
        parsing ingest view query results, transforming it to Python objects that model
        our schema, then writing to the database.
        Returns:
            True if we should try to schedule the next job on completion. False,
             otherwise.
        """
        check_is_region_launched_in_env(self.region)

        start_time = datetime.datetime.now()
        logging.info("Starting ingest for ingest run [%s]", args.job_tag())

        if not self._contents_exist_for_args(args):
            logging.warning(
                "Contents does not exist for ingest run [%s] - returning.",
                args.job_tag(),
            )
            return False

        if isinstance(args, LegacyExtractAndMergeArgs):
            # TODO(#9717): We should be able to delete this check once we're reading
            #   chunks of data directly from BigQuery.
            if not self._ingest_view_file_contents_meet_scale_requirements(args):
                logging.warning(
                    "Cannot proceed with contents for ingest run [%s] - returning.",
                    args.job_tag(),
                )
                # If we get here, we've failed to properly split a file picked up
                # by the scheduler. We don't want to schedule a new job after
                # returning here, otherwise we'll get ourselves in a loop where we
                # continually try to schedule this file.
                return False

        contents_handle = self._get_contents_handle(args)

        if contents_handle is None:
            logging.warning(
                "Failed to get contents handle for ingest run [%s] - returning.",
                args.job_tag(),
            )
            # If the file no-longer exists, we do want to kick the scheduler
            # again to pick up the next file to run. We expect this to happen
            # occasionally as a race when the scheduler picks up a file before
            # it has been properly moved.
            return True

        logging.info("Successfully read contents for ingest run [%s]", args.job_tag())

        # TODO(#9717): Implement ability to check for empty BQ-based ingest view
        #  results.
        if isinstance(args, LegacyExtractAndMergeArgs):
            contents_are_empty = self._are_contents_empty(args)
        else:
            raise ValueError(
                f"Unsupported extract and merge args type: [{type(args)}]."
            )

        if not contents_are_empty:
            self._parse_and_persist_contents(args, contents_handle)
        else:
            logging.warning(
                "Contents are empty for ingest run [%s] - skipping parse and "
                "persist steps.",
                args.job_tag(),
            )

        self._do_cleanup(args)

        duration_sec = (datetime.datetime.now() - start_time).total_seconds()
        logging.info(
            "Finished ingest in [%s] sec for ingest run [%s].",
            str(duration_sec),
            args.job_tag(),
        )

        return True

    def get_ingest_view_processor(
        self, args: ExtractAndMergeArgs
    ) -> IngestViewProcessor:
        """Returns the appropriate ingest view processor for this extract and merge
        job.
        """
        yaml_mappings_dict = YAMLDict.from_path(
            yaml_mappings_filepath(self.region, args.file_tag)
        )
        version_str = yaml_mappings_dict.peek_optional(
            MANIFEST_LANGUAGE_VERSION_KEY, str
        )

        if not version_str:
            # TODO(#8905): Delete this branch once all regions have migrated to new
            #  ingest mappings structure.
            delegate: Optional[LegacyIngestViewProcessorDelegate] = None
            if isinstance(self, LegacyIngestViewProcessorDelegate):
                delegate = self

            if not delegate:
                raise ValueError(
                    f"Must implement "
                    f"{LegacyIngestViewProcessorDelegate.__name__} interface "
                    f"on object with type [{type(self)} to support legacy ingest."
                )

            return LegacyIngestViewProcessor(
                region=self.region,
                ingest_instance=self.ingest_instance,
                delegate=delegate,
            )

        # If a version string is present, it's v2
        return IngestViewProcessorImpl(
            ingest_view_file_parser=IngestViewFileParser(
                delegate=IngestViewFileParserDelegateImpl(
                    self.region, self.system_level.schema_type(), self.ingest_instance
                )
            )
        )

    @trace.span
    def _parse_and_persist_contents(
        self, args: ExtractAndMergeArgs, contents_handle: ContentsHandle
    ) -> None:
        """
        Runs the full ingest process for this controller for files with
        non-empty contents.
        """
        processor = self.get_ingest_view_processor(args)
        persist_success = processor.parse_and_persist_contents(
            args=args,
            contents_handle=contents_handle,
            ingest_metadata=self._get_ingest_metadata(args),
        )

        if not persist_success:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PERSISTENCE_ERROR,
                msg="Persist step failed",
            )

        logging.info("Successfully persisted for ingest run [%s]", args.job_tag())

    def _get_ingest_metadata(self, args: ExtractAndMergeArgs) -> IngestMetadata:
        if isinstance(self, LegacyIngestViewProcessorDelegate):
            # TODO(#8905): Remove this block once we have migrated all direct ingest
            #  states to ingest mappings v2.
            enum_overrides = self.get_enum_overrides()
            if not isinstance(self, BaseDirectIngestController):
                raise ValueError(
                    f"Expected LegacyIngestViewProcessorDelegate to also be a "
                    f"BaseDirectIngestController, found [{type(self)}]."
                )
            return LegacyStateAndJailsIngestMetadata(
                region=self.region.region_code,
                jurisdiction_id=self.region.jurisdiction_id,
                ingest_time=args.ingest_time,
                enum_overrides=enum_overrides,
                system_level=self.system_level,
                database_key=self.ingest_database_key,
            )
        return IngestMetadata(
            region=self.region.region_code,
            ingest_time=args.ingest_time,
            system_level=self.system_level,
            database_key=self.ingest_database_key,
        )

    def _get_contents_handle(
        self, args: ExtractAndMergeArgs
    ) -> Optional[ContentsHandle]:
        """Returns a handle to the ingest view contents allows us to iterate over the
        contents and also manages cleanup of resources once we are done with the
        contents.

        Will return None if the contents could not be read (i.e. if they no
        longer exist).
        """
        if isinstance(args, LegacyExtractAndMergeArgs):
            return self.fs.download_to_temp_file(args.file_path)

        # TODO(#9717): Implement for BQ-based ingest view results.
        raise ValueError(f"Unsupported extract and merge args type: [{type(args)}].")

    # TODO(#9717): We should be able to delete this function once we're reading
    #   chunks of data directly from BigQuery.
    def _are_contents_empty(
        self,
        args: LegacyExtractAndMergeArgs,
    ) -> bool:
        """Returns true if the CSV file is empty, i.e. it contains no non-header
        rows.
        """
        delegate = ReadOneGcsfsCsvReaderDelegate()
        self.csv_reader.streaming_read(
            args.file_path, delegate=delegate, chunk_size=1, skiprows=1
        )
        return delegate.df is None

    def _do_cleanup(self, args: ExtractAndMergeArgs) -> None:
        """Does necessary cleanup once ingest view contents have been successfully
        persisted to Postgres.
        """
        if isinstance(args, LegacyExtractAndMergeArgs):
            self.fs.mv_path_to_processed_path(args.file_path)

            self.file_metadata_manager.mark_ingest_view_file_as_processed(
                args.file_path
            )

            parts = filename_parts_from_path(args.file_path)
            self._move_processed_files_to_storage_as_necessary(
                last_processed_date_str=parts.date_str
            )
            return

        # TODO(#9717): Implement clean up for BQ-based ingest view results.
        raise ValueError(f"Unsupported extract and merge args type: [{type(args)}].")

    def _contents_exist_for_args(self, args: ExtractAndMergeArgs) -> bool:
        if isinstance(args, LegacyExtractAndMergeArgs):
            if not self.fs.exists(args.file_path):
                logging.warning(
                    "Path [%s] does not exist - returning.",
                    args.file_path.abs_path(),
                )
                return False
            return True

        # TODO(#9717): Implement clean up for BQ-based ingest view results.
        raise ValueError(f"Unsupported extract and merge args type: [{type(args)}].")

    # TODO(#9717): We should be able to delete this check once we're reading
    #   chunks of data directly from BigQuery.
    def _ingest_view_file_contents_meet_scale_requirements(
        self, args: LegacyExtractAndMergeArgs
    ) -> bool:
        """Given a pointer to the contents, returns whether the controller can continue
        ingest for a file of this size.
        """
        parts = filename_parts_from_path(args.file_path)
        return self._are_contents_empty(args) or not self._must_split_contents(
            parts.file_type, args.file_path
        )

    # TODO(#9717): We should be able to delete this check once we're reading
    #   chunks of data directly from BigQuery.
    def _must_split_contents(
        self, file_type: GcsfsDirectIngestFileType, path: GcsfsFilePath
    ) -> bool:
        if file_type == GcsfsDirectIngestFileType.RAW_DATA:
            return False

        return not self._file_meets_file_line_limit(
            self.ingest_file_split_line_limit, path
        )

    # TODO(#9717): We should be able to delete this check once we're reading
    #   chunks of data directly from BigQuery.
    def _file_meets_file_line_limit(self, line_limit: int, path: GcsfsFilePath) -> bool:
        """Returns True if the file meets the expected line limit, false otherwise."""
        delegate = ReadOneGcsfsCsvReaderDelegate()

        # Read a chunk up to one line bigger than the acceptable size
        try:
            self.csv_reader.streaming_read(
                path, delegate=delegate, chunk_size=(line_limit + 1)
            )
        except GCSBlobDoesNotExistError:
            return True

        if delegate.df is None:
            # If the file is empty, it's fine.
            return True

        # If length of the only chunk is less than or equal to the acceptable
        # size, file meets line limit.
        return len(delegate.df) <= line_limit

    # TODO(#9717): This whole function will be deleted once we have migrated to BQ-based
    #   ingest view results processing.
    def _move_processed_files_to_storage_as_necessary(
        self, last_processed_date_str: str
    ) -> None:
        """Moves files that have already been ingested/processed, up to and including the given date, into storage,
        if there is nothing more left to ingest/process, i.e. we are not expecting more files."""

        if not isinstance(self.job_prioritizer, GcsfsDirectIngestJobPrioritizer):
            raise ValueError(
                f"Unexpected job_prioritizer type: [{self.job_prioritizer}]"
            )

        next_args = self.job_prioritizer.get_next_job_args()

        should_move_last_processed_date = False
        if not next_args:
            are_more_jobs_expected = (
                self.job_prioritizer.are_more_jobs_expected_for_day(
                    last_processed_date_str
                )
            )
            if not are_more_jobs_expected:
                should_move_last_processed_date = True
        else:
            if not isinstance(next_args, LegacyExtractAndMergeArgs):
                raise ValueError(f"Unexpected args type: [{type(next_args)}]")
            next_date_str = filename_parts_from_path(next_args.file_path).date_str
            if next_date_str < last_processed_date_str:
                logging.info(
                    "Found a file [%s] from a date previous to our "
                    "last processed date - not moving anything to "
                    "storage."
                )
                return

            # If there are still more to process on this day, do not move files
            # from this day.
            should_move_last_processed_date = next_date_str != last_processed_date_str

        # Note: at this point, we expect RAW file type files to already have been moved once they were imported to BQ.
        self.fs.mv_processed_paths_before_date_to_storage(
            self.ingest_bucket_path,
            self.storage_directory_path,
            file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW,
            date_str_bound=last_processed_date_str,
            include_bound=should_move_last_processed_date,
        )

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

        if self.fs.is_normalized_file_path(path):
            parts = filename_parts_from_path(path)

            if (
                parts.is_file_split
                and parts.file_split_size
                and parts.file_split_size <= self.ingest_file_split_line_limit
            ):
                self.kick_scheduler(just_finished_job=False)
                logging.info(
                    "File [%s] is already normalized and split split "
                    "with correct size, kicking scheduler.",
                    path.abs_path(),
                )
                return

        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=self.region,
            ingest_bucket=self.ingest_bucket_path,
            can_start_ingest=start_ingest,
        )

    def _register_all_new_paths_in_metadata(self, paths: List[GcsfsFilePath]) -> None:
        for path in paths:
            parts = filename_parts_from_path(path)
            if parts.file_type == GcsfsDirectIngestFileType.RAW_DATA:
                if not self.file_metadata_manager.has_raw_file_been_discovered(path):
                    self.file_metadata_manager.mark_raw_file_as_discovered(path)
            elif parts.file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
                if not self.file_metadata_manager.has_ingest_view_file_been_discovered(
                    path
                ):
                    self.file_metadata_manager.mark_ingest_view_file_as_discovered(path)
            else:
                raise ValueError(f"Unexpected file type [{parts.file_type}]")

    @trace.span
    def handle_new_files(self, *, current_task_id: str, can_start_ingest: bool) -> None:
        """Searches the ingest directory for new/unprocessed files. Normalizes
        file names and splits files as necessary, schedules the next ingest job
        if allowed.


        Should only be called from the scheduler queue.
        """
        if not can_start_ingest and self.region.is_ingest_launched_in_env():
            raise ValueError(
                "The can_start_ingest flag should only be used for regions where ingest is not yet launched in a "
                "particular environment. If we want to be able to selectively pause ingest processing for a state, we "
                "will first have to build a config that is respected by both the /ensure_all_raw_file_paths_normalized "
                "endpoint and any cloud functions that trigger ingest."
            )

        if self.ingest_instance_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_bucket_path.uri()
            )
            return

        self._prune_redundant_tasks(current_task_id=current_task_id)

        unnormalized_paths = self.fs.get_unnormalized_file_paths(
            self.ingest_bucket_path
        )

        for path in unnormalized_paths:
            logging.info("File [%s] is not yet seen, normalizing.", path.abs_path())
            self.fs.mv_path_to_normalized_path(
                path, file_type=GcsfsDirectIngestFileType.RAW_DATA
            )

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

        unprocessed_ingest_view_paths = self.fs.get_unprocessed_file_paths(
            self.ingest_bucket_path,
            file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW,
        )
        unprocessed_raw_paths = self.fs.get_unprocessed_file_paths(
            self.ingest_bucket_path,
            file_type_filter=GcsfsDirectIngestFileType.RAW_DATA,
        )
        if (
            unprocessed_raw_paths
            and self.ingest_instance == DirectIngestInstance.SECONDARY
        ):
            raise ValueError(
                f"Raw data import not supported from SECONDARY ingest bucket "
                f"[{self.ingest_bucket_path}], but found {len(unprocessed_raw_paths)} "
                f"raw files. All raw files should be removed from this bucket and "
                f"uploaded to the primary ingest bucket, if appropriate."
            )

        self._register_all_new_paths_in_metadata(unprocessed_raw_paths)

        self._register_all_new_paths_in_metadata(unprocessed_ingest_view_paths)

        did_split = False
        for path in unprocessed_ingest_view_paths:
            if self._split_file_if_necessary(path):
                did_split = True

        if did_split:
            post_split_unprocessed_ingest_view_paths = (
                self.fs.get_unprocessed_file_paths(
                    self.ingest_bucket_path,
                    file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW,
                )
            )
            self._register_all_new_paths_in_metadata(
                post_split_unprocessed_ingest_view_paths
            )

            logging.info(
                "Split at least one path - returning, will handle split "
                "files separately."
            )
            # Writing new split files to storage will cause the cloud function
            # that calls this function to be re-triggered.
            return

        # Even if there are no unprocessed paths, we still want to look for the next
        # job because there may be new files to generate.
        self._schedule_next_ingest_task(just_finished_job=False)

    def do_raw_data_import(self, data_import_args: GcsfsRawDataBQImportArgs) -> None:
        """Process a raw incoming file by importing it to BQ, tracking it in our metadata tables, and moving it to
        storage on completion.
        """
        check_is_region_launched_in_env(self.region)

        if self.ingest_instance_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_bucket_path.uri()
            )
            return

        if self.ingest_instance == DirectIngestInstance.SECONDARY:
            raise ValueError(
                f"Raw data import not supported from SECONDARY ingest bucket "
                f"[{self.ingest_bucket_path}]. Raw data task for "
                f"[{data_import_args.raw_data_file_path}] should never have been "
                f"scheduled."
            )

        if not self.fs.exists(data_import_args.raw_data_file_path):
            logging.warning(
                "File path [%s] no longer exists - might have already been "
                "processed or deleted",
                data_import_args.raw_data_file_path,
            )
            self.kick_scheduler(just_finished_job=True)
            return

        file_metadata = self.file_metadata_manager.get_raw_file_metadata(
            data_import_args.raw_data_file_path
        )

        if file_metadata.processed_time:
            logging.warning(
                "File [%s] is already marked as processed. Skipping file processing.",
                data_import_args.raw_data_file_path.file_name,
            )
            self.kick_scheduler(just_finished_job=True)
            return

        self.raw_file_import_manager.import_raw_file_to_big_query(
            data_import_args.raw_data_file_path, file_metadata
        )

        processed_path = self.fs.mv_path_to_processed_path(
            data_import_args.raw_data_file_path
        )
        self.file_metadata_manager.mark_raw_file_as_processed(
            path=data_import_args.raw_data_file_path
        )

        self.fs.mv_path_to_storage(processed_path, self.storage_directory_path)
        self.kick_scheduler(just_finished_job=True)

    # TODO(#9717): This function can be deleted once ingest views are materialized in
    #   BQ and we no longer need to export the results to files.
    def do_ingest_view_export(
        self, ingest_view_export_args: GcsfsIngestViewExportArgs
    ) -> None:
        check_is_region_launched_in_env(self.region)

        if self.ingest_instance_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_bucket_path.uri()
            )
            return

        did_export = self.ingest_view_export_manager.export_view_for_args(
            ingest_view_export_args
        )
        if (
            not did_export
            or not self.file_metadata_manager.get_ingest_view_metadata_pending_export()
        ):
            logging.info("Creating cloud task to schedule next job.")
            self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
                region=self.region,
                ingest_bucket=self.ingest_bucket_path,
                can_start_ingest=True,
            )

    def _should_split_file(self, path: GcsfsFilePath) -> bool:
        """Returns a handle to the contents of this path if this file should be split, None otherwise."""
        parts = filename_parts_from_path(path)

        if parts.file_type != GcsfsDirectIngestFileType.INGEST_VIEW:
            raise ValueError(
                f"Should not be attempting to split files other than ingest view files, found path with "
                f"file type: {parts.file_type}"
            )

        if parts.file_tag not in self.get_file_tag_rank_list():
            logging.info(
                "File tag [%s] for path [%s] not in rank list - not splitting.",
                parts.file_tag,
                path.abs_path(),
            )
            return False

        if (
            parts.is_file_split
            and parts.file_split_size
            and parts.file_split_size <= self.ingest_file_split_line_limit
        ):
            logging.info(
                "File [%s] already split with size [%s].",
                path.abs_path(),
                parts.file_split_size,
            )
            return False

        return self._must_split_contents(parts.file_type, path)

    @trace.span
    def _split_file_if_necessary(self, path: GcsfsFilePath) -> bool:
        """Checks if the given file needs to be split according to this controller's |file_split_line_limit|.

        Returns True if the file was split, False if splitting was not necessary.
        """

        should_split = self._should_split_file(path)
        if not should_split:
            logging.info("No need to split file path [%s].", path.abs_path())
            return False

        logging.info("Proceeding to file splitting for path [%s].", path.abs_path())

        original_metadata = self.file_metadata_manager.get_ingest_view_file_metadata(
            path
        )

        output_dir = GcsfsDirectoryPath.from_file_path(path)

        split_contents_paths = self._split_file(path)
        upload_paths = []
        for i, split_contents_path in enumerate(split_contents_paths):
            upload_path = self._create_split_file_path(path, output_dir, split_num=i)

            logging.info(
                "Copying split [%s] to direct ingest directory at path [%s].",
                i,
                upload_path.abs_path(),
            )

            upload_paths.append(upload_path)
            try:
                self.fs.mv(split_contents_path, upload_path)
            except Exception as e:
                logging.error(
                    "Threw error while copying split files from temp bucket - attempting to clean up before rethrowing."
                    " [%s]",
                    e,
                )
                for p in upload_paths:
                    self.fs.delete(p)
                raise e

        # We wait to register files with metadata manager until all files have been successfully copied to avoid leaving
        # the metadata manager in an inconsistent state.
        if not isinstance(original_metadata, DirectIngestIngestFileMetadata):
            raise ValueError("Attempting to split a non-ingest view type file")

        logging.info(
            "Registering [%s] split files with the metadata manager.",
            len(upload_paths),
        )

        for upload_path in upload_paths:
            ingest_file_metadata = (
                self.file_metadata_manager.register_ingest_file_split(
                    original_metadata, upload_path
                )
            )
            self.file_metadata_manager.mark_ingest_view_exported(ingest_file_metadata)

        self.file_metadata_manager.mark_ingest_view_file_as_processed(path)

        logging.info(
            "Done splitting file [%s] into [%s] paths, moving it to storage.",
            path.abs_path(),
            len(split_contents_paths),
        )

        self.fs.mv_path_to_storage(path, self.storage_directory_path)

        return True

    def _create_split_file_path(
        self,
        original_file_path: GcsfsFilePath,
        output_dir: GcsfsDirectoryPath,
        split_num: int,
    ) -> GcsfsFilePath:
        parts = filename_parts_from_path(original_file_path)

        rank_str = str(split_num + 1).zfill(5)
        updated_file_name = (
            f"{parts.stripped_file_name}_{rank_str}"
            f"_{SPLIT_FILE_SUFFIX}_size{self.ingest_file_split_line_limit}"
            f".{parts.extension}"
        )

        return GcsfsFilePath.from_directory_and_file_name(
            output_dir,
            to_normalized_unprocessed_file_path(
                updated_file_name,
                file_type=parts.file_type,
                dt=parts.utc_upload_datetime,
            ),
        )

    def _split_file(self, path: GcsfsFilePath) -> List[GcsfsFilePath]:
        """Splits a file accessible via the provided path into multiple
        files and uploads those files to GCS. Returns the list of upload paths.
        """

        parts = filename_parts_from_path(path)

        if parts.file_type == GcsfsDirectIngestFileType.RAW_DATA:
            raise ValueError(
                f"Splitting raw files unsupported. Attempting to split [{path.abs_path()}]"
            )

        delegate = DirectIngestFileSplittingGcsfsCsvReaderDelegate(
            path, self.fs, self.temp_output_directory_path
        )
        self.csv_reader.streaming_read(
            path, delegate=delegate, chunk_size=self.ingest_file_split_line_limit
        )
        output_paths = [path for path, _ in delegate.output_paths_with_columns]

        return output_paths


def check_is_region_launched_in_env(region: Region) -> None:
    """Checks if direct ingest has been launched for the provided |region| in the current GCP env and throws if it has
    not."""
    if not region.is_ingest_launched_in_env():
        gcp_env = environment.get_gcp_environment()
        error_msg = f"Bad environment [{gcp_env}] for region [{region.region_code}]."
        logging.error(error_msg)
        raise DirectIngestError(
            msg=error_msg, error_type=DirectIngestErrorType.ENVIRONMENT_ERROR
        )
