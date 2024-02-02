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
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.io.contents_handle import ContentsHandle
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.ingest.direct.controllers.extract_and_merge_job_prioritizer import (
    ExtractAndMergeJobPrioritizerImpl,
)
from recidiviz.ingest.direct.controllers.ingest_view_processor import (
    IngestViewProcessor,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestCloudTaskQueueManagerImpl,
    build_handle_new_files_task_id,
    build_scheduler_task_id,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.gating import is_ingest_in_dataflow_enabled
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
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegateImpl,
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
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManagerImpl,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusChangeListener,
    PostgresDirectIngestInstanceStatusManager,
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
from recidiviz.ingest.direct.types.instance_database_key import database_key_for_state
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.monitoring import trace
from recidiviz.persistence.database.schema.operations.dao import (
    stale_secondary_raw_data,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import environment


class BaseDirectIngestController(DirectIngestInstanceStatusChangeListener):
    """Parses and persists individual-level info from direct ingest partners."""

    def __init__(
        self,
        ingest_instance: DirectIngestInstance,
        region_module_override: Optional[ModuleType] = None,
    ) -> None:
        """Initialize the controller."""
        self.region_module_override = region_module_override
        self.is_ingest_in_dataflow_enabled = is_ingest_in_dataflow_enabled(
            state_code=self.state_code(),
            instance=ingest_instance,
        )
        self.cloud_task_manager = DirectIngestCloudTaskQueueManagerImpl()
        self.ingest_instance = ingest_instance
        self.region_lock_manager = DirectIngestRegionLockManager.for_direct_ingest(
            region_code=self.region.region_code,
            ingest_instance=self.ingest_instance,
        )
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.instance_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code=self.region.region_code, ingest_instance=self.ingest_instance
        )
        self.csv_reader = GcsfsCsvReader(GcsfsFactory.build())

        self.ingest_instance_status_manager = PostgresDirectIngestInstanceStatusManager(
            region_code=self.region.region_code,
            ingest_instance=self.ingest_instance,
            change_listener=self,
            is_ingest_in_dataflow_enabled=self.is_ingest_in_dataflow_enabled,
        )

        self.temp_output_directory_path = (
            gcsfs_direct_ingest_temporary_output_directory_path()
        )

        self.big_query_client = BigQueryClientImpl()

        if not self.is_ingest_in_dataflow_enabled:
            self.view_collector = DirectIngestViewQueryBuilderCollector(
                self.region, self.get_ingest_view_rank_list()
            )

            self.view_materialization_metadata_manager = (
                DirectIngestViewMaterializationMetadataManagerImpl(
                    region_code=self.region_code(),
                    ingest_instance=self.ingest_instance,
                )
            )
            self.ingest_view_contents: Optional[
                InstanceIngestViewContentsImpl
            ] = InstanceIngestViewContentsImpl(
                big_query_client=self.big_query_client,
                region_code=self.region_code(),
                ingest_instance=self.ingest_instance,
                dataset_prefix=None,
            )
            self.job_prioritizer = ExtractAndMergeJobPrioritizerImpl(
                ingest_view_contents=self.ingest_view_contents,
                ingest_view_rank_list=self.get_ingest_view_rank_list(),
            )
        else:
            self.ingest_view_contents = None

        # We cannot set these objects until we know the raw data source instance, which
        # may not be set at instantiation time (i.e. if there is no rerun in progress
        # for this instance).
        # TODO(#20930): Delete when ingest in dataflow is shipped for all states - once
        #  this controller class is only responsible for raw data import, there
        #  will be no code *reading* from imported raw data, so we can just use the
        #  ingest_instance everywhere.
        self._raw_data_source_instance: Optional[DirectIngestInstance] = None
        self._raw_file_metadata_manager: Optional[
            DirectIngestRawFileMetadataManager
        ] = None

        # TODO(#20930): Initialize this in the constructor when ingest in dataflow is
        #  shipped for all states and the raw_data_source_instance field is deleted.
        self._raw_file_import_manager: Optional[DirectIngestRawFileImportManager] = None

        # TODO(#20930): Delete when ingest in dataflow is shipped for all states
        self._ingest_view_materialization_args_generator: Optional[
            IngestViewMaterializationArgsGenerator
        ] = None
        # TODO(#20930): Delete when ingest in dataflow is shipped for all states
        self._ingest_view_materializer: Optional[IngestViewMaterializerImpl] = None

        self.on_raw_data_source_instance_change(
            self.ingest_instance
            if self.is_ingest_in_dataflow_enabled
            else self.ingest_instance_status_manager.get_raw_data_source_instance()
        )

    @property
    def raw_data_source_instance(self) -> DirectIngestInstance:
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Raw data source instance is not a valid concept for ingest in Dataflow"
            )
        if not self._raw_data_source_instance:
            raise ValueError("Expected nonnull raw data source instance.")
        return self._raw_data_source_instance

    @property
    def raw_file_metadata_manager(self) -> DirectIngestRawFileMetadataManager:
        if not self._raw_file_metadata_manager:
            raise ValueError(
                "Expected nonnull raw file metadata manager - has "
                "on_raw_data_source_instance_change() been called?"
            )
        return self._raw_file_metadata_manager

    @property
    def raw_file_import_manager(self) -> DirectIngestRawFileImportManager:
        if not self._raw_file_import_manager:
            raise ValueError(
                "Expected nonnull raw file import manager - has "
                "on_raw_data_source_instance_change() been called?"
            )
        return self._raw_file_import_manager

    def ingest_view_materializer(self) -> IngestViewMaterializerImpl:
        if not self._ingest_view_materializer:
            raise ValueError(
                "Expected nonnull ingest view materializer - has "
                "on_raw_data_source_instance_change() been called?"
            )
        return self._ingest_view_materializer

    @property
    def ingest_view_materialization_args_generator(
        self,
    ) -> IngestViewMaterializationArgsGenerator:
        if not self._ingest_view_materialization_args_generator:
            raise ValueError(
                "Expected nonnull materialization args generator - has "
                "on_raw_data_source_instance_change() been called?"
            )
        return self._ingest_view_materialization_args_generator

    @property
    def raw_data_bucket_path(self) -> GcsfsBucketPath:
        if self.is_ingest_in_dataflow_enabled:
            ingest_instance = self.ingest_instance
        else:
            if not self._raw_data_source_instance:
                raise ValueError("Expected nonnull raw data source instance.")
            ingest_instance = self._raw_data_source_instance
        return gcsfs_direct_ingest_bucket_for_state(
            region_code=self.region.region_code,
            ingest_instance=ingest_instance,
        )

    @property
    def raw_data_storage_directory_path(self) -> GcsfsDirectoryPath:
        if not self._raw_data_source_instance:
            raise ValueError("Expected nonnull raw data source instance.")
        return gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=self.region_code(),
            ingest_instance=self._raw_data_source_instance,
        )

    def raw_data_deprecated_storage_directory_path(
        self, deprecated_on_date: datetime.date
    ) -> GcsfsDirectoryPath:
        if not self._raw_data_source_instance:
            raise ValueError("Expected nonnull raw data source instance.")
        return gcsfs_direct_ingest_deprecated_storage_directory_path_for_state(
            region_code=self.region_code(),
            ingest_instance=self._raw_data_source_instance,
            deprecated_on_date=deprecated_on_date,
        )

    def on_ingest_instance_status_change(
        self, previous_status: DirectIngestStatus, new_status: DirectIngestStatus
    ) -> None:
        """Listener method called by the PostgresDirectIngestInstanceStatusManager every time a status changes."""
        if DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS in (
            previous_status,
            new_status,
        ):
            if self.ingest_instance == DirectIngestInstance.PRIMARY:
                # Any time we transition to/from importing raw data in PRIMARY, we notify SECONDARY
                # so that its status can change appropriately.
                self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
                    region=self.region,
                    ingest_instance=DirectIngestInstance.SECONDARY,
                    can_start_ingest=True,
                )

    def on_raw_data_source_instance_change(
        self, raw_data_source_instance: Optional[DirectIngestInstance]
    ) -> None:
        """Listener method called by the PostgresDirectIngestInstanceStatusManager
        every time the raw_data_source_instance changes.
        """
        # Do nothing if the raw data source hasn't changed.
        if self._raw_data_source_instance == raw_data_source_instance:
            return

        self._raw_data_source_instance = raw_data_source_instance
        # Clear out all fields that rely on a raw_data_source_instance if it's empty
        if not self._raw_data_source_instance:
            self._raw_file_metadata_manager = None
            self._raw_file_import_manager = None
            self._ingest_view_materialization_args_generator = None
            self._ingest_view_materializer = None
            return

        # TODO(#20930): Set self._raw_file_metadata_manager / self._raw_file_import_manager
        #  directly in the constructor once ingest in dataflow is enabled for all states.
        # Update all fields to rely on the new raw_data_source_instance.
        self._raw_file_metadata_manager = DirectIngestRawFileMetadataManager(
            region_code=self.region.region_code,
            raw_data_instance=self._raw_data_source_instance,
        )

        self._raw_file_import_manager = DirectIngestRawFileImportManager(
            region=self.region,
            fs=self.fs,
            temp_output_directory_path=self.temp_output_directory_path,
            big_query_client=self.big_query_client,
            csv_reader=self.csv_reader,
            instance=self._raw_data_source_instance,
        )

        if not self.is_ingest_in_dataflow_enabled:
            if not self.ingest_view_contents:
                raise ValueError("Expected ingest_view_contents to be set")
            self._ingest_view_materializer = IngestViewMaterializerImpl(
                region=self.region,
                ingest_instance=self.ingest_instance,
                raw_data_source_instance=self.raw_data_source_instance,
                ingest_view_contents=self.ingest_view_contents,
                metadata_manager=self.view_materialization_metadata_manager,
                big_query_client=self.big_query_client,
                view_collector=self.view_collector,
                launched_ingest_views=self.get_ingest_view_rank_list(),
            )

            self._ingest_view_materialization_args_generator = (
                IngestViewMaterializationArgsGenerator(
                    region=self.region,
                    metadata_manager=self.view_materialization_metadata_manager,
                    raw_file_metadata_manager=self.raw_file_metadata_manager,
                    view_collector=self.view_collector,
                    launched_ingest_views=self.get_ingest_view_rank_list(),
                )
            )

    @property
    def region(self) -> DirectIngestRegion:
        return direct_ingest_regions.get_direct_ingest_region(
            self.region_code().lower(),
            region_module_override=self.region_module_override,
        )

    @classmethod
    @abc.abstractmethod
    def state_code(cls) -> StateCode:
        pass

    @classmethod
    def region_code(cls) -> str:
        return cls.state_code().value.lower()

    def get_ingest_view_rank_list(self) -> List[str]:
        """Returns the list of ingest view names for ingest views that are shipped in
        the current environment and whose results can be processed and committed to
        our central data model.
        """
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )
        return self._get_ingest_view_rank_list(self.ingest_instance)

    # TODO(#20930): Delete this function once ingest in Dataflow is enabled for all
    #  states.
    @classmethod
    @abc.abstractmethod
    def _get_ingest_view_rank_list(
        cls, ingest_instance: DirectIngestInstance
    ) -> List[str]:
        """Returns the list of ingest view names for ingest views that are shipped in
        the current environment and whose results can be processed and committed to
        our central data model.
        """

    @property
    def ingest_database_key(self) -> SQLAlchemyDatabaseKey:
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )
        return database_key_for_state(
            self.ingest_instance,
            self.state_code(),
        )

    def _ingest_is_not_running(self) -> bool:
        """Returns True if ingest is not running in this instance and all functions should
        early-return without doing any work.
        """
        current_status = self.ingest_instance_status_manager.get_current_status()

        return current_status in {
            DirectIngestStatus.NO_RERUN_IN_PROGRESS,
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
        }

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
        """Internal helper for scheduling the next ingest task."""
        check_is_region_launched_in_env(self.region)

        if self._ingest_is_not_running():
            logging.info(
                "Ingest is not running in this instance - not proceeding and returning early.",
            )
            return

        if self._schedule_raw_data_import_tasks():
            # If raw data source instance is different from the ingest instance, update the status accordingly.
            if (
                not self.is_ingest_in_dataflow_enabled
                and self.ingest_instance != self.raw_data_source_instance
            ):
                self.ingest_instance_status_manager.change_status_to(
                    DirectIngestStatus.BLOCKED_ON_PRIMARY_RAW_DATA_IMPORT
                )
            else:
                self.ingest_instance_status_manager.change_status_to(
                    DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
                )
            logging.info(
                "Found pre-ingest raw data import tasks to schedule - returning."
            )
            return

        if not self.is_ingest_in_dataflow_enabled:
            if self._schedule_ingest_view_materialization_tasks():
                self.ingest_instance_status_manager.change_status_to(
                    DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS
                )
                logging.info(
                    "Found ingest view materialization tasks to schedule - returning."
                )
                return

            if self._schedule_extract_and_merge_tasks(just_finished_job):
                self.ingest_instance_status_manager.change_status_to(
                    DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS
                )

                logging.info("Found extract and merge tasks to schedule - returning.")
                return

        # If there aren't any more tasks to schedule for the region, update to the appropriate next
        # status, based on the ingest instance.
        if self.ingest_instance == DirectIngestInstance.PRIMARY:
            logging.info(
                "Controller's instance is PRIMARY. No tasks to schedule and instance is now up to date."
            )
            self.ingest_instance_status_manager.change_status_to(
                DirectIngestStatus.RAW_DATA_UP_TO_DATE
                if self.is_ingest_in_dataflow_enabled
                else DirectIngestStatus.UP_TO_DATE
            )
        elif stale_secondary_raw_data(
            self.region_code(), self.is_ingest_in_dataflow_enabled
        ):
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
        # TODO(#20930): Delete this variable and just use self.ingest_instance directly
        #  once ingest in dataflow is shipped.
        raw_data_source_instance = (
            self.ingest_instance
            if self.is_ingest_in_dataflow_enabled
            else self.raw_data_source_instance
        )

        # Fetch the queue information for where the raw data is being processed.
        queue_info = self.cloud_task_manager.get_raw_data_import_queue_info(
            region=self.region, ingest_instance=raw_data_source_instance
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
                    self.region, raw_data_source_instance, task_args
                )
                did_schedule = True

        for file_tag in sorted(unrecognized_file_tags):
            logging.warning(
                "Unrecognized raw file tag [%s] for region [%s] - skipped import.",
                file_tag,
                self.region_code(),
            )
        return queue_info.has_raw_data_import_jobs_queued() or did_schedule

    # TODO(#20930): Delete when ingest in dataflow is shipped for all states
    def _schedule_ingest_view_materialization_tasks(self) -> bool:
        """Schedules all pending ingest view materialization tasks for launched ingest
        view tags, if they have not been scheduled. If tasks are scheduled or are still
        running, returns True. Otherwise, if it's safe to proceed with next steps of
        ingest, returns False.
        """
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )
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

    # TODO(#20930): Delete when ingest in dataflow is shipped for all states
    def _schedule_extract_and_merge_tasks(self, just_finished_job: bool) -> bool:
        """Schedules the next pending extract and merge task, if it has not been
        scheduled. If there is still extract and merge work to do, returns True.
        Otherwise, if it's safe to proceed with next steps of ingest, returns False.
        """
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )
        extract_and_merge_queue_info = (
            self.cloud_task_manager.get_extract_and_merge_queue_info(
                self.region,
                self.ingest_instance,
            )
        )
        next_job_args = self.job_prioritizer.get_next_job_args()

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
            return True

        if not next_job_args:
            logging.info(
                "No more extract and merge to run for region [%s] - returning",
                self.region.region_code,
            )
            return False

        if extract_and_merge_queue_info.is_task_already_queued(
            self.region_code(), next_job_args
        ):
            logging.info(
                "Already have task queued for next extract and merge job [%s] - returning.",
                next_job_args.job_tag(),
            )
            return True

        # At this point we have found a new extract and merge job to schedule.  However, we might have to wait until
        # the locks are released to actually schedule this task.
        if self.region_lock_manager.is_locked():
            logging.info("Direct ingest is already locked on region [%s]", self.region)
            return True

        if not self.region_lock_manager.can_proceed():
            logging.info(
                "CloudSQL to BigQuery refresh is running, cannot run ingest - returning"
            )
            return True

        logging.info(
            "Creating cloud task to run extract and merge job [%s]",
            next_job_args.job_tag(),
        )

        self.cloud_task_manager.create_direct_ingest_extract_and_merge_task(
            region=self.region,
            task_args=next_job_args,
        )

        return True

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

    # TODO(#20930): Delete when ingest in dataflow is shipped for all states
    def run_extract_and_merge_job_and_kick_scheduler_on_completion(
        self, args: ExtractAndMergeArgs
    ) -> None:
        """
        Runs the full extract and merge process for this controller and triggers
        scheduler upon completion, if necessary.
        """
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )
        check_is_region_launched_in_env(self.region)

        if self._ingest_is_not_running():
            logging.info(
                "Ingest is not running in this instance - not proceeding and returning early.",
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

    # TODO(#20930): Delete when ingest in dataflow is shipped for all states
    def _run_extract_and_merge_job(self, args: ExtractAndMergeArgs) -> bool:
        """
        Runs the full extract and merge process for this controller - reading and
        parsing ingest view query results, transforming it to Python objects that model
        our schema, then writing to the database.
        Returns:
            True if we should try to schedule the next job on completion. False,
             otherwise.
        """
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )
        if not self.ingest_view_contents:
            raise ValueError("Expected ingest_view_contents to be set")
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

    @trace.span
    def _parse_and_persist_contents(
        self, args: ExtractAndMergeArgs, contents_handle: ContentsHandle
    ) -> None:
        """
        Runs the full ingest process for this controller for files with
        non-empty contents.
        """
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )

        processor = IngestViewProcessor(
            ingest_view_manifest_compiler=IngestViewManifestCompiler(
                delegate=IngestViewManifestCompilerDelegateImpl(
                    region=self.region, schema_type=SchemaType.STATE
                )
            )
        )
        persist_success = processor.parse_and_persist_contents(
            args=args,
            contents_handle=contents_handle,
            ingest_metadata=IngestMetadata(
                region=self.region.region_code,
                ingest_time=args.ingest_time,
                database_key=self.ingest_database_key,
            ),
        )

        if not persist_success:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PERSISTENCE_ERROR,
                msg="Persist step failed",
            )

        logging.info("Successfully persisted for ingest run [%s]", args.job_tag())

    def _get_contents_handle(
        self, args: ExtractAndMergeArgs
    ) -> BigQueryResultsContentsHandle:
        """Returns a handle to the ingest view contents allows us to iterate over the
        contents and also manages cleanup of resources once we are done with the
        contents.
        Will return None if the contents could not be read (i.e. if they no
        longer exist).
        """
        if not self.ingest_view_contents:
            raise ValueError("Expected ingest_view_contents to be set")
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
        self._schedule_next_ingest_task(just_finished_job=False)

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
            self.kick_scheduler(just_finished_job=True)
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
            self.kick_scheduler(just_finished_job=True)
            return

        should_schedule = False
        with self.region_lock_manager.using_raw_file_lock(
            raw_file_tag=data_import_args.file_id(),
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
            self.kick_scheduler(just_finished_job=True)

    # TODO(#20930): Delete when ingest in dataflow is shipped for all states
    def do_ingest_view_materialization(
        self, ingest_view_materialization_args: IngestViewMaterializationArgs
    ) -> None:
        if self.is_ingest_in_dataflow_enabled:
            raise ValueError(
                "Cannot call this method for an ingest in dataflow enabled state."
            )
        check_is_region_launched_in_env(self.region)

        if self._ingest_is_not_running():
            logging.info(
                "Ingest is not running in this instance - not proceeding and returning early.",
            )
            return

        did_materialize = self.ingest_view_materializer().materialize_view_for_args(
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
