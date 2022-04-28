# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Store used to keep information related to direct ingest operations"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from google.cloud import tasks_v2

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
    get_direct_ingest_queues_for_state,
)
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_region,
    gcsfs_direct_ingest_storage_directory_path_for_region,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_gating_context import (
    IngestViewMaterializationGatingContext,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    IngestViewContentsSummary,
    InstanceIngestViewContentsImpl,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
    IngestViewMaterializationSummary,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestIngestFileMetadataManager,
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata
from recidiviz.utils.environment import in_development
from recidiviz.utils.regions import get_region

_TASK_LOCATION = "us-east1"

QUEUE_STATE_ENUM = tasks_v2.enums.Queue.State

BucketSummaryType = Dict[str, Union[str, int]]


class IngestOperationsStore(AdminPanelStore):
    """
    A store for tracking the current state of direct ingest.
    """

    def __init__(self) -> None:
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.cloud_task_manager = DirectIngestCloudTaskManagerImpl()
        self.cloud_tasks_client = tasks_v2.CloudTasksClient()
        self.ingest_view_materialization_gating_context: Optional[
            IngestViewMaterializationGatingContext
        ] = None

    def recalculate_store(self) -> None:
        # This store currently does not store any internal state that can be refreshed.
        # Data must be fetched manually from other public methods.
        self.ingest_view_materialization_gating_context = (
            IngestViewMaterializationGatingContext.load_from_gcs()
        )

    @property
    def state_codes_launched_in_env(self) -> List[StateCode]:
        return get_direct_ingest_states_launched_in_env()

    @staticmethod
    def get_queues_for_region(state_code: StateCode) -> List[str]:
        """Returns the list of formatted direct ingest queues for given state"""
        return sorted(get_direct_ingest_queues_for_state(state_code))

    def start_ingest_run(self, state_code: StateCode, instance_str: str) -> None:
        """This function is called through the Ingest Operations UI in the admin panel.
        It calls to start a direct ingest run for the given region_code in the given instance
        Requires:
        - state_code: (required) State code to start ingest for (i.e. "US_ID")
        - instance: (required) Which instance to start ingest for (either PRIMARY or SECONDARY)
        """
        try:
            instance = DirectIngestInstance[instance_str]
        except KeyError as e:
            logging.error("Received an invalid instance: %s.", instance_str)
            raise ValueError(
                f"Invalid instance [{instance_str}] received",
            ) from e

        can_start_ingest = state_code in self.state_codes_launched_in_env

        formatted_state_code = state_code.value.lower()
        region = get_region(formatted_state_code, is_direct_ingest=True)

        # Get the ingest bucket for this region and instance
        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_region(
            region_code=formatted_state_code,
            system_level=SystemLevel.for_region(region),
            ingest_instance=instance,
            project_id=metadata.project_id(),
        )

        logging.info(
            "Creating cloud task to schedule next job and kick ingest for %s instance in %s.",
            instance,
            formatted_state_code,
        )
        self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=region,
            ingest_bucket=ingest_bucket_path,
            can_start_ingest=can_start_ingest,
        )

    def update_ingest_queues_state(
        self, state_code: StateCode, new_queue_state: str
    ) -> None:
        """This function is called through the Ingest Operations UI in the admin panel.
        It updates the state of the following queues by either pausing or resuming the
        queues:
         - direct-ingest-state-<region_code>-process-job-queue
         - direct-ingest-state-<region_code>-process-job-queue-secondary
         - direct-ingest-state-<region_code>-scheduler
         - direct-ingest-state-<region_code>-scheduler-secondary
         - direct-ingest-state-<region_code>-raw-data-import
         - direct-ingest-state-<region_code>-raw-data-import-secondary
         - direct-ingest-state-<region_code>-ingest-view-export
         - direct-ingest-state-<region_code>-ingest-view-export-secondary
         - direct-ingest-state-<region_code>-sftp-queue    (for select regions)

        Requires:
        - state_code: (required) State code to pause queues for
        - new_state: (required) Either 'PAUSED' or 'RUNNING'
        """
        queues_to_update = self.get_queues_for_region(state_code)

        if new_queue_state not in [
            QUEUE_STATE_ENUM.RUNNING.name,
            QUEUE_STATE_ENUM.PAUSED.name,
        ]:
            logging.error(
                "Received an invalid queue state: %s. This method should only be used "
                "to update queue states to PAUSED or RUNNING",
                new_queue_state,
            )
            raise ValueError(
                f"Invalid queue state [{new_queue_state}] received",
            )

        for queue in queues_to_update:
            queue_path = self.cloud_tasks_client.queue_path(
                metadata.project_id(), _TASK_LOCATION, queue
            )

            if new_queue_state == QUEUE_STATE_ENUM.PAUSED.name:
                logging.info("Pausing queue: %s", new_queue_state)
                self.cloud_tasks_client.pause_queue(name=queue_path)
            else:
                logging.info("Resuming queue: %s", new_queue_state)
                self.cloud_tasks_client.resume_queue(name=queue_path)

    def get_ingest_queue_states(self, state_code: StateCode) -> List[Dict[str, str]]:
        """Returns a list of dictionaries that contain the name and states of direct ingest queues for a given region"""
        ingest_queue_states: List[Dict[str, str]] = []
        queues_to_update = self.get_queues_for_region(state_code)

        for queue_name in queues_to_update:
            queue_path = self.cloud_tasks_client.queue_path(
                metadata.project_id(), _TASK_LOCATION, queue_name
            )
            queue = self.cloud_tasks_client.get_queue(name=queue_path)
            queue_state = {
                "name": queue_name,
                "state": QUEUE_STATE_ENUM(queue.state).name,
            }
            ingest_queue_states.append(queue_state)

        return ingest_queue_states

    def _get_bucket_metadata(self, path: GcsfsBucketPath) -> BucketSummaryType:
        """Returns a dictionary containing the following info for a given bucket:
        i.e. {
            name: bucket_name,
            unprocessedFilesRaw: how many unprocessed raw data files in the bucket,
            processedFilesRaw: how many processed raw data files are in the bucket (should be zero),
            unprocessedFilesIngestView: how many unprocessed ingest view files in the bucket,
            processedFilesIngestView: how many processed ingest view files are in the bucket
        }
        """
        bucket_metadata: BucketSummaryType = {
            "name": path.abs_path(),
        }

        for file_type in GcsfsDirectIngestFileType:
            file_type_str = self.get_file_type_api_string(file_type)
            unprocessed_files = self.fs.get_unprocessed_file_paths(path, file_type)
            bucket_metadata[f"unprocessedFiles{file_type_str}"] = len(unprocessed_files)

            processed_files = self.fs.get_processed_file_paths(path, file_type)
            bucket_metadata[f"processedFiles{file_type_str}"] = len(processed_files)

        return bucket_metadata

    @staticmethod
    def get_file_type_api_string(file_type: GcsfsDirectIngestFileType) -> str:
        """Get the string representation of the file type to use in the response."""
        if file_type == GcsfsDirectIngestFileType.INGEST_VIEW:
            return "IngestView"
        if file_type == GcsfsDirectIngestFileType.RAW_DATA:
            return "Raw"
        raise ValueError(f"Unexpected file_type [{file_type}]")

    def get_ingest_instance_summaries(
        self, state_code: StateCode
    ) -> List[Dict[str, Any]]:
        """Returns a list of dictionaries containing the following info for a given instance:
        i.e. {
            instance: the direct ingest instance,
            dbName: database name for this instance,
            storage: storage bucket absolute path,
            ingest: {
                name: bucket_name,
                unprocessedFilesRaw: how many unprocessed raw data files in the bucket,
                processedFilesRaw: how many processed raw data files are in the bucket (should be zero),
                unprocessedFilesIngestView: how many unprocessed ingest view files in the bucket,
                processedFilesIngestView: how many processed ingest view files are in the bucket (should be zero),
            },
            operations: {
                unprocessedFilesRaw: number of unprocessed raw files in the operations database
                unprocessedFilesIngestView: number of unprocessed ingest view files in the operations database
                dateOfEarliestUnprocessedIngestView: date of earliest unprocessed ingest file, if it exists
            }
            isBQMaterializationEnabled: If true, BQ ingest view materialization is
                enabled for this state for this ingest instance.
        }
        """
        formatted_state_code = state_code.value.lower()

        ingest_instance_summaries: List[Dict[str, Any]] = []
        for instance in DirectIngestInstance:
            # Get the ingest bucket path
            ingest_bucket_path = gcsfs_direct_ingest_bucket_for_region(
                region_code=formatted_state_code,
                system_level=SystemLevel.STATE,
                ingest_instance=instance,
                project_id=metadata.project_id(),
            )
            # Get an object containing information about the ingest bucket
            ingest_bucket_metadata = self._get_bucket_metadata(ingest_bucket_path)

            # Get the storage bucket for this instance
            storage_bucket_path = gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code=formatted_state_code,
                system_level=SystemLevel.STATE,
                ingest_instance=instance,
                project_id=metadata.project_id(),
            )

            # Get the database name corresponding to this instance
            ingest_db_name = instance.database_key_for_state(state_code).db_name

            # Get the operations metadata for this ingest instance
            operations_db_metadata = self._get_operations_db_metadata(
                state_code, instance
            )

            if not self.ingest_view_materialization_gating_context:
                self.ingest_view_materialization_gating_context = (
                    IngestViewMaterializationGatingContext.load_from_gcs()
                )
            is_bq_materialization_enabled = self.ingest_view_materialization_gating_context.is_bq_ingest_view_materialization_enabled(
                state_code=state_code, ingest_instance=instance
            )
            ingest_instance_summary: Dict[str, Any] = {
                "instance": instance.value,
                "storage": storage_bucket_path.abs_path(),
                "ingest": ingest_bucket_metadata,
                "dbName": ingest_db_name,
                "operations": operations_db_metadata,
                # TODO(#11424): Delete this flag once BQ materialization has been fully
                #  shipped to all states and admin panel frontend no longer references
                #  this value.
                "isBQMaterializationEnabled": is_bq_materialization_enabled,
            }

            ingest_instance_summaries.append(ingest_instance_summary)

        return ingest_instance_summaries

    @staticmethod
    def _get_operations_db_metadata(
        state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> Dict[
        str,
        Union[
            int,
            Optional[datetime],
            List[Dict[str, Union[str, int, Optional[datetime]]]],
        ],
    ]:
        """Returns the following dictionary with information about the operations database for the state:
        {
            isPaused: <bool>
            unprocessedFilesRaw: <int>
            processedFilesRaw: <int>
            unprocessedFilesIngestView: <int>
            processedFilesIngestView: <int>
            dateOfEarliestUnprocessedIngestView: <datetime>
            ingestViewMaterializationSummaries: [
                {
                    ingestViewName: <str>
                    numPendingJobs: <int>
                    numCompletedJobs: <int>
                    completedJobsMaxDatetime: <datetime>
                    pendingJobsMinDatetime: <datetime>
                }
            ]
        }

        If running locally, this does not hit the live DB instance and only returns fake
        data.
        """
        if in_development():
            return {
                "isPaused": ingest_instance == DirectIngestInstance.SECONDARY,
                "unprocessedFilesRaw": -1,
                "processedFilesRaw": -2,
                "ingestViewMaterializationSummaries": [
                    IngestViewMaterializationSummary(
                        ingest_view_name="some_view",
                        num_pending_jobs=-5,
                        num_completed_jobs=-6,
                        completed_jobs_max_datetime=datetime(2000, 1, 1, 1, 1, 1, 1),
                        pending_jobs_min_datetime=datetime(2000, 2, 2, 2, 2, 2, 2),
                    ).as_api_dict()
                ],
                "ingestViewContentsSummaries": [
                    IngestViewContentsSummary(
                        ingest_view_name="some_view",
                        num_processed_rows=0,
                        processed_rows_max_datetime=None,
                        unprocessed_rows_min_datetime=datetime(2000, 3, 3, 3, 3, 3, 3),
                        num_unprocessed_rows=10,
                    ).as_api_dict()
                ],
                # TODO(#11424): Delete these three fields once BQ materialization
                #  migration is complete.
                "unprocessedFilesIngestView": -3,
                "processedFilesIngestView": -4,
                "dateOfEarliestUnprocessedIngestView": datetime(2021, 4, 28),
            }

        raw_file_metadata_manager = PostgresDirectIngestRawFileMetadataManager(
            region_code=state_code.value,
            ingest_database_name=ingest_instance.database_key_for_state(
                state_code
            ).db_name,
        )

        # TODO(#11424): Remove this reference to the legacy metadata manager once
        #  BQ materialization migration has completed.
        ingest_file_metadata_manager = PostgresDirectIngestIngestFileMetadataManager(
            region_code=state_code.value,
            ingest_database_name=ingest_instance.database_key_for_state(
                state_code
            ).db_name,
        )

        if ingest_instance == DirectIngestInstance.PRIMARY:
            num_unprocessed_raw_files = (
                raw_file_metadata_manager.get_num_unprocessed_raw_files()
            )
            num_processed_raw_files = (
                raw_file_metadata_manager.get_num_processed_raw_files()
            )
        elif ingest_instance == DirectIngestInstance.SECONDARY:
            # TODO(##12387): Raw files are currently processed in the primary instance
            #  only, not secondary. This logic will need to change once we enable
            #  raw data imports in secondary.
            num_unprocessed_raw_files = 0
            num_processed_raw_files = 0
        else:
            raise ValueError(f"Unexpected ingest instance: [{ingest_instance}]")

        ingest_instance_status_manager = DirectIngestInstanceStatusManager(
            state_code.value, ingest_instance
        )
        is_paused = ingest_instance_status_manager.is_instance_paused()

        materialization_job_summaries = DirectIngestViewMaterializationMetadataManager(
            state_code.value, ingest_instance
        ).get_instance_summaries()

        ingest_view_contents = InstanceIngestViewContentsImpl(
            big_query_client=BigQueryClientImpl(),
            region_code=state_code.value,
            ingest_instance=ingest_instance,
            dataset_prefix=None,
        )
        contents_summaries = [
            ingest_view_contents.get_ingest_view_contents_summary(ingest_view_name)
            for ingest_view_name, summary in materialization_job_summaries.items()
        ]

        return {
            "isPaused": is_paused,
            "unprocessedFilesRaw": num_unprocessed_raw_files,
            "processedFilesRaw": num_processed_raw_files,
            "ingestViewMaterializationSummaries": [
                summary.as_api_dict()
                for summary in materialization_job_summaries.values()
            ],
            "ingestViewContentsSummaries": [
                summary.as_api_dict()
                for summary in contents_summaries
                if summary is not None
            ],
            # TODO(#11424): Delete these three fields once BQ materialization
            #  migration is complete.
            "unprocessedFilesIngestView": ingest_file_metadata_manager.get_num_unprocessed_ingest_files(),
            "processedFilesIngestView": ingest_file_metadata_manager.get_num_processed_ingest_files(),
            "dateOfEarliestUnprocessedIngestView": ingest_file_metadata_manager.get_date_of_earliest_unprocessed_ingest_file(),
        }
