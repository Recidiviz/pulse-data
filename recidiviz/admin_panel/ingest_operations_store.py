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
from typing import List, Optional, Dict, Union, Any

from google.cloud import tasks_v2

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    gcsfs_direct_ingest_bucket_for_region,
    gcsfs_direct_ingest_storage_directory_path_for_region,
    GcsfsDirectIngestFileType,
)
from recidiviz.ingest.direct.controllers.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestFileMetadataManager,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
)
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
    get_direct_ingest_states_with_sftp_queue,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import metadata
from recidiviz.utils.environment import in_development
from recidiviz.utils.regions import get_region

_BQ_IMPORT_EXPORT_QUEUE = "direct-ingest-state-{}-bq-import-export"
_PROCESS_JOB_QUEUE = "direct-ingest-state-{}-process-job-queue"
_SCHEDULER_QUEUE = "direct-ingest-state-{}-scheduler"
_SFTP_QUEUE = "direct-ingest-state-{}-sftp-queue"

INGEST_QUEUES: List[str] = [
    _BQ_IMPORT_EXPORT_QUEUE,
    _PROCESS_JOB_QUEUE,
    _SCHEDULER_QUEUE,
]

_TASK_LOCATION = "us-east1"

QUEUE_STATE_ENUM = tasks_v2.enums.Queue.State

BucketSummaryType = Dict[str, Union[str, int]]


class IngestOperationsStore:
    """
    A store for tracking the current state of direct ingest.
    """

    def __init__(self, override_project_id: Optional[str] = None) -> None:
        self.project_id = (
            metadata.project_id()
            if override_project_id is None
            else override_project_id
        )
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.cloud_task_manager = DirectIngestCloudTaskManagerImpl()
        self.cloud_tasks_client = tasks_v2.CloudTasksClient()

    @property
    def state_codes_launched_in_env(self) -> List[StateCode]:
        return get_direct_ingest_states_launched_in_env()

    @staticmethod
    def get_queues_for_region(state_code: StateCode) -> List[str]:
        """Returns the list of formatted direct ingest queues for given state"""
        formatted_region_code = state_code.value.lower().replace("_", "-")
        queues: List[str] = [
            queue.format(formatted_region_code) for queue in INGEST_QUEUES
        ]

        if state_code in get_direct_ingest_states_with_sftp_queue():
            queues.append(_SFTP_QUEUE.format(formatted_region_code))

        return queues

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
            project_id=self.project_id,
        )

        logging.info(
            "Creating cloud task to schedule next job and kick ingest for %s instance in %s.",
            instance,
            formatted_state_code,
        )
        self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=region,
            ingest_instance=instance,
            ingest_bucket=ingest_bucket_path,
            can_start_ingest=can_start_ingest,
        )

    def update_ingest_queues_state(
        self, state_code: StateCode, new_queue_state: str
    ) -> None:
        """This function is called through the Ingest Operations UI in the admin panel.
        It updates the state of the following queues by either pausing or resuming the queues:
         - direct-ingest-state-<region_code>-bq-import-export
         - direct-ingest-state-<region_code>-process-job-queue
         - direct-ingest-state-<region_code>-scheduler
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
                self.project_id, _TASK_LOCATION, queue
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
                self.project_id, _TASK_LOCATION, queue_name
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
                project_id=self.project_id,
            )
            # Get an object containing information about the ingest bucket
            ingest_bucket_metadata = self._get_bucket_metadata(ingest_bucket_path)

            # Get the storage bucket for this instance
            storage_bucket_path = gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code=formatted_state_code,
                system_level=SystemLevel.STATE,
                ingest_instance=instance,
                project_id=self.project_id,
            )

            # Get the database name corresponding to this instance
            ingest_db_name = self._get_database_name_for_state(state_code, instance)

            # Get the operations metadata for this ingest instance
            operations_db_metadata = self._get_operations_db_metadata(
                state_code, ingest_db_name
            )

            ingest_instance_summary: Dict[str, Any] = {
                "instance": instance.value,
                "storage": storage_bucket_path.abs_path(),
                "ingest": ingest_bucket_metadata,
                "dbName": ingest_db_name,
                "operations": operations_db_metadata,
            }

            ingest_instance_summaries.append(ingest_instance_summary)

        return ingest_instance_summaries

    @staticmethod
    def _get_database_name_for_state(
        state_code: StateCode, instance: DirectIngestInstance
    ) -> str:
        """Returns the database name for the given state and instance"""
        return SQLAlchemyDatabaseKey.for_state_code(
            state_code, instance.database_version(SystemLevel.STATE)
        ).db_name

    @staticmethod
    def _get_operations_db_metadata(
        state_code: StateCode, ingest_db_name: str
    ) -> Dict[str, Union[int, Optional[datetime]]]:
        """Returns the following dictionary with information about the operations database for the state:
        {
            unprocessedFilesRaw: <int>
            unprocessedFilesIngestView: <int>
            dateOfEarliestUnprocessedIngestView: <datetime>
        }

        If running locally, this does not hit the live DB instance and only returns fake data.
        """
        if in_development():
            return {
                "unprocessedFilesRaw": -1,
                "unprocessedFilesIngestView": -2,
                "dateOfEarliestUnprocessedIngestView": datetime(2021, 4, 28),
            }

        file_metadata_manager = PostgresDirectIngestFileMetadataManager(
            region_code=state_code.value,
            ingest_database_name=ingest_db_name,
        )

        return {
            "unprocessedFilesRaw": file_metadata_manager.get_num_unprocessed_raw_files(),
            "unprocessedFilesIngestView": file_metadata_manager.get_num_unprocessed_ingest_files(),
            "dateOfEarliestUnprocessedIngestView": file_metadata_manager.get_date_of_earliest_unprocessed_ingest_file(),
        }
