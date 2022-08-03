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

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockAlreadyExists
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import (
    IngestMetadata,
    LegacyStateAndJailsIngestMetadata,
    SystemLevel,
)
from recidiviz.common.io.contents_handle import ContentsHandle
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.ingest.direct.controllers.extract_and_merge_job_prioritizer import (
    ExtractAndMergeJobPrioritizer,
    ExtractAndMergeJobPrioritizerImpl,
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
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
    gcsfs_direct_ingest_temporary_output_directory_path,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser import (
    MANIFEST_LANGUAGE_VERSION_KEY,
    IngestViewResultsParser,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegateImpl,
    yaml_mappings_filepath,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_args_generator import (
    IngestViewMaterializationArgsGenerator,
)
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    InstanceIngestViewContentsImpl,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.legacy_ingest_view_processor import (
    LegacyIngestViewProcessor,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_pause_status_manager import (
    DirectIngestInstancePauseStatusManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    GcsfsRawDataBQImportArgs,
    IngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestErrorType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment, regions, trace
from recidiviz.utils.regions import Region
from recidiviz.utils.yaml_dict import YAMLDict


class BaseDirectIngestController:
    """Parses and persists individual-level info from direct ingest partners."""

    def __init__(
        self,
        ingest_instance: DirectIngestInstance,
        region_module_override: Optional[ModuleType] = None,
    ) -> None:
        """Initialize the controller."""
        self.region_module_override = region_module_override
        if (
            region_system_level := SystemLevel.for_region(self.region)
        ) != SystemLevel.STATE:
            raise ValueError(
                f"Direct ingest does not support system level: [{region_system_level}]"
            )

        self.system_level = SystemLevel.STATE
        self.cloud_task_manager = DirectIngestCloudTaskManagerImpl()
        self.ingest_instance = ingest_instance
        self.region_lock_manager = DirectIngestRegionLockManager.for_direct_ingest(
            region_code=self.region.region_code,
            schema_type=self.system_level.schema_type(),
            ingest_instance=self.ingest_instance,
        )
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.instance_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code=self.region.region_code, ingest_instance=self.ingest_instance
        )
        # TODO(#12794): This will have to change based on which instance is the raw data
        #  source instance.
        raw_data_source_instance = DirectIngestInstance.PRIMARY

        self.raw_data_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code=self.region.region_code,
            ingest_instance=raw_data_source_instance,
        )
        self.raw_data_storage_directory_path = (
            gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code=self.region_code(),
                ingest_instance=raw_data_source_instance,
            )
        )

        self.temp_output_directory_path = (
            gcsfs_direct_ingest_temporary_output_directory_path()
        )

        self.raw_file_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
            region_code=self.region.region_code,
            # TODO(#12794): Change to be based on the instance the raw file is
            #  processed in once we can import data from both PRIMARY and SECONDARY.
            raw_data_instance=DirectIngestInstance.PRIMARY,
        )

        big_query_client = BigQueryClientImpl()

        self.raw_file_import_manager = DirectIngestRawFileImportManager(
            region=self.region,
            fs=self.fs,
            temp_output_directory_path=self.temp_output_directory_path,
            big_query_client=big_query_client,
        )

        view_collector = DirectIngestPreProcessedIngestViewCollector(
            self.region, self.get_ingest_view_rank_list()
        )

        self.job_prioritizer: ExtractAndMergeJobPrioritizer

        self.view_materialization_metadata_manager = (
            DirectIngestViewMaterializationMetadataManager(
                region_code=self.region_code(),
                ingest_instance=self.ingest_instance,
            )
        )
        self.ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=big_query_client,
            region_code=self.region_code(),
            ingest_instance=self.ingest_instance,
            dataset_prefix=None,
        )
        self.job_prioritizer = ExtractAndMergeJobPrioritizerImpl(
            ingest_view_contents=self.ingest_view_contents,
            ingest_view_rank_list=self.get_ingest_view_rank_list(),
        )

        self.ingest_view_materialization_args_generator = (
            IngestViewMaterializationArgsGenerator(
                region=self.region,
                metadata_manager=self.view_materialization_metadata_manager,
                raw_file_metadata_manager=self.raw_file_metadata_manager,
                view_collector=view_collector,
                launched_ingest_views=self.get_ingest_view_rank_list(),
            )
        )

        self.ingest_view_materializer = IngestViewMaterializerImpl(
            region=self.region,
            ingest_instance=self.ingest_instance,
            ingest_view_contents=self.ingest_view_contents,
            metadata_manager=self.view_materialization_metadata_manager,
            big_query_client=big_query_client,
            view_collector=view_collector,
            launched_ingest_views=self.get_ingest_view_rank_list(),
        )

        self.ingest_instance_pause_status_manager = (
            DirectIngestInstancePauseStatusManager(
                self.region_code(), self.ingest_instance
            )
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

    @abc.abstractmethod
    def get_ingest_view_rank_list(self) -> List[str]:
        """Returns the list of ingest view names for ingest views that are shipped in
        the current environment and whose results can be processed and commiteed to
        our central data model.
        """

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
            ingest_instance=self.ingest_instance,
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

        if self.ingest_instance_pause_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_instance
            )
            return

        if self._schedule_raw_data_import_tasks():
            logging.info(
                "Found pre-ingest raw data import tasks to schedule - returning."
            )
            return

        if self._schedule_ingest_view_materialization_tasks():
            logging.info(
                "Found ingest view materialization tasks to schedule - returning."
            )
            return

        if self.region_lock_manager.is_locked():
            logging.info("Direct ingest is already locked on region [%s]", self.region)
            return

        extract_and_merge_queue_info = (
            self.cloud_task_manager.get_extract_and_merge_queue_info(
                self.region,
                self.ingest_instance,
            )
        )
        if (
            extract_and_merge_queue_info.has_any_tasks_for_instance(
                region_code=self.region_code(), ingest_instance=self.ingest_instance
            )
            and not just_finished_job
        ):
            logging.info(
                "Already running job [%s] - will not schedule another job for "
                "region [%s]",
                extract_and_merge_queue_info.task_names[0],
                self.region.region_code,
            )
            return

        next_job_args = self.job_prioritizer.get_next_job_args()

        if not next_job_args:
            logging.info(
                "No more extract and merge to run for region [%s] - returning",
                self.region.region_code,
            )
            return

        if extract_and_merge_queue_info.is_task_already_queued(
            self.region_code(), next_job_args
        ):
            logging.info(
                "Already have task queued for next extract and merge job [%s] - returning.",
                next_job_args.job_tag(),
            )
            return

        if not self.region_lock_manager.can_proceed():
            logging.info(
                "CloudSQL to BigQuery refresh is running, cannot run ingest - returning"
            )
            return

        logging.info(
            "Creating cloud task to run extract and merge job [%s]",
            next_job_args.job_tag(),
        )

        self.cloud_task_manager.create_direct_ingest_extract_and_merge_task(
            region=self.region,
            task_args=next_job_args,
        )

    def _schedule_raw_data_import_tasks(self) -> bool:
        """Schedules all pending raw data import tasks for launched ingest view tags, if
        they have not been scheduled. If tasks are scheduled or are still running,
        returns True. Otherwise, if it's safe to proceed with next steps of ingest,
        returns False.
        """
        queue_info = self.cloud_task_manager.get_raw_data_import_queue_info(self.region)

        raw_files_pending_import = (
            self.raw_file_metadata_manager.get_unprocessed_raw_files()
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
                self.cloud_task_manager.create_direct_ingest_raw_data_import_task(
                    self.region, task_args
                )
                did_schedule = True

        for file_tag in sorted(unrecognized_file_tags):
            logging.warning(
                "Unrecognized raw file tag [%s] for region [%s] - skipped import.",
                file_tag,
                self.region_code(),
            )
        return queue_info.has_raw_data_import_jobs_queued() or did_schedule

    def _schedule_ingest_view_materialization_tasks(self) -> bool:
        """Schedules all pending ingest view materialization tasks for launched ingest
        view tags, if they have not been scheduled. If tasks are scheduled or are still
        running, returns True. Otherwise, if it's safe to proceed with next steps of
        ingest, returns False.
        """
        queue_info = self.cloud_task_manager.get_ingest_view_materialization_queue_info(
            self.region,
            self.ingest_instance,
        )
        if queue_info.has_ingest_view_materialization_jobs_queued(
            self.region_code(), self.ingest_instance
        ):
            # Since we schedule all materialization jobs at once, after all raw files
            # have been processed, we wait for all of the materialization jobs to be
            # done before checking if we need to schedule more.
            return True

        did_schedule = False
        tasks_to_schedule = (
            self.ingest_view_materialization_args_generator.get_ingest_view_materialization_task_args()
        )

        rank_list = self.get_ingest_view_rank_list()
        ingest_view_name_rank = {
            ingest_view_name: i for i, ingest_view_name in enumerate(rank_list)
        }

        for args in tasks_to_schedule:
            if args.ingest_view_name not in ingest_view_name_rank:
                raise ValueError(
                    f"Found args for ingest_view_name={args.ingest_view_name} which is "
                    f"not in controller ingest_view_name_rank={ingest_view_name_rank}."
                    f"Args: {args}."
                )

        # Sort by tag order and file datetime
        tasks_to_schedule.sort(
            key=lambda args_: (
                ingest_view_name_rank[args_.ingest_view_name],
                args_.upper_bound_datetime_inclusive,
            )
        )

        for task_args in tasks_to_schedule:
            self.cloud_task_manager.create_direct_ingest_view_materialization_task(
                self.region,
                task_args,
            )
            did_schedule = True

        return did_schedule

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

        if self.ingest_instance_pause_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_instance
            )
            return

        if not self.region_lock_manager.can_proceed():
            logging.warning(
                "CloudSQL to BigQuery refresh is running, can not run ingest"
            )
            raise GCSPseudoLockAlreadyExists(
                "CloudSQL to BigQuery refresh is running, can not run ingest"
            )

        with self.region_lock_manager.using_region_lock(
            expiration_in_seconds=self.default_job_lock_timeout_in_seconds(),
        ):
            should_schedule = self._run_extract_and_merge_job(args)

        if should_schedule:
            self.kick_scheduler(just_finished_job=True)
            logging.info("Done running task. Returning.")

    def _run_extract_and_merge_job(self, args: ExtractAndMergeArgs) -> bool:
        """
        Runs the full extract and merge process for this controller - reading and
        parsing ingest view query results, transforming it to Python objects that model
        our schema, then writing to the database.
        Returns:
            True if we should try to schedule the next job on completion. False,
             otherwise.
        """
        check_is_region_launched_in_env(self.region)

        if args.upper_bound_datetime_inclusive.tzinfo is not None:
            raise ValueError(
                f"Expected timezone unaware upper_bound_datetime_inclusive: "
                f"{args.upper_bound_datetime_inclusive}. Found tzinfo: "
                f"{args.upper_bound_datetime_inclusive.tzinfo}."
            )

        start_time = datetime.datetime.now()
        logging.info("Starting ingest for ingest run [%s]", args.job_tag())

        contents_handle = self._get_contents_handle(args)

        logging.info("Successfully read contents for ingest run [%s]", args.job_tag())

        contents_are_empty = self._are_contents_empty(contents_handle)

        if contents_are_empty:
            # This may happen occasionally if this task gets re-executed after the
            # rows have already been processed.
            logging.warning(
                "Contents are empty for ingest run [%s] - returning.",
                args.job_tag(),
            )

            # In this case, we do want to kick the scheduler again to pick up the next
            # batch to run.
            return True

        self._parse_and_persist_contents(args, contents_handle)

        logging.info("Marking rows processed for ingest run [%s]", args.job_tag())
        self.ingest_view_contents.mark_rows_as_processed(
            ingest_view_name=args.ingest_view_name,
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            batch_number=args.batch_number,
        )

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
            yaml_mappings_filepath(self.region, args.ingest_view_name)
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
            ingest_view_file_parser=IngestViewResultsParser(
                delegate=IngestViewResultsParserDelegateImpl(
                    region=self.region,
                    schema_type=self.system_level.schema_type(),
                    ingest_instance=self.ingest_instance,
                    results_update_datetime=args.upper_bound_datetime_inclusive,
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
    ) -> BigQueryResultsContentsHandle:
        """Returns a handle to the ingest view contents allows us to iterate over the
        contents and also manages cleanup of resources once we are done with the
        contents.

        Will return None if the contents could not be read (i.e. if they no
        longer exist).
        """
        return self.ingest_view_contents.get_unprocessed_rows_for_batch(
            ingest_view_name=args.ingest_view_name,
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            batch_number=args.batch_number,
        )

    def _are_contents_empty(self, contents_handle: ContentsHandle) -> bool:
        """Returns True if there any materialized ingest view results in the contents
        handle.
        """
        for _ in contents_handle.get_contents_iterator():
            return False
        return True

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

        if self.ingest_instance_pause_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_instance
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
            self.instance_bucket_path
        )
        if (
            unprocessed_raw_paths
            and self.ingest_instance == DirectIngestInstance.SECONDARY
        ):
            raise ValueError(
                f"Raw data import not supported from SECONDARY ingest bucket "
                f"[{self.instance_bucket_path}], but found {len(unprocessed_raw_paths)} "
                f"raw files. All raw files should be removed from this bucket and "
                f"uploaded to the primary ingest bucket, if appropriate."
            )

        self._register_all_new_raw_file_paths_in_metadata(unprocessed_raw_paths)

        # Even if there are no unprocessed paths, we still want to look for the next
        # job because there may be new files to generate.
        self._schedule_next_ingest_task(just_finished_job=False)

    def do_raw_data_import(self, data_import_args: GcsfsRawDataBQImportArgs) -> None:
        """Process a raw incoming file by importing it to BQ, tracking it in our metadata tables, and moving it to
        storage on completion.
        """
        check_is_region_launched_in_env(self.region)

        if self.ingest_instance_pause_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_instance
            )
            return

        if self.ingest_instance == DirectIngestInstance.SECONDARY:
            raise ValueError(
                f"Raw data import not supported from the SECONDARY instance. Raw "
                f"data task for [{data_import_args.raw_data_file_path}] should never "
                f"have been scheduled."
            )

        try:
            file_metadata = self.raw_file_metadata_manager.get_raw_file_metadata(
                data_import_args.raw_data_file_path
            )
        except ValueError:
            # If there is no operations DB row and the file doesn't exist, then
            # this file was likely deprecated fully.
            file_metadata = None

        if not self.fs.exists(data_import_args.raw_data_file_path):
            if file_metadata is not None and file_metadata.processed_time is None:
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
            self.kick_scheduler(just_finished_job=True)
            return

        if file_metadata is None:
            raise ValueError(
                f"No metadata row for file [{data_import_args.raw_data_file_path}]."
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

        self.raw_file_metadata_manager.mark_raw_file_as_processed(
            path=data_import_args.raw_data_file_path
        )
        processed_path = self.fs.mv_path_to_processed_path(
            data_import_args.raw_data_file_path
        )

        self.fs.mv_raw_file_to_storage(
            processed_path, self.raw_data_storage_directory_path
        )
        self.kick_scheduler(just_finished_job=True)

    def do_ingest_view_materialization(
        self, ingest_view_materialization_args: IngestViewMaterializationArgs
    ) -> None:
        check_is_region_launched_in_env(self.region)

        if self.ingest_instance_pause_status_manager.is_instance_paused():
            logging.info(
                "Ingest out of [%s] is currently paused.", self.ingest_instance
            )
            return

        did_materialize = self.ingest_view_materializer.materialize_view_for_args(
            ingest_view_materialization_args
        )

        if (
            not did_materialize
            or not self.ingest_view_materialization_args_generator.get_registered_jobs_pending_completion()
        ):
            logging.info("Creating cloud task to schedule next job.")
            self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
                region=self.region,
                ingest_instance=self.ingest_instance,
                can_start_ingest=True,
            )


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
