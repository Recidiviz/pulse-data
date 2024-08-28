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
import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

import pytz
from google.cloud import tasks_v2

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
    get_all_latest_ingest_jobs,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.serialization import attr_from_json_dict, attr_to_json_dict
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestCloudTaskQueueManagerImpl,
    get_direct_ingest_queues_for_state,
)
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_resource_lock_manager import (
    DirectIngestRawDataLockStatus,
    DirectIngestRawDataResourceLockManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
    LatestDirectIngestRawFileImportRunSummary,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_file_metadata_manager import (
    DirectIngestRawFileMetadataManager,
    DirectIngestRawFileMetadataSummary,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import (
    DirectIngestError,
    DirectIngestInstanceError,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus
from recidiviz.utils import metadata
from recidiviz.utils.types import assert_type

BucketSummaryType = Dict[str, Union[str, int]]


class IngestOperationsStore(AdminPanelStore):
    """
    A store for tracking the current state of direct ingest.
    """

    def __init__(self) -> None:
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.cloud_task_manager = DirectIngestCloudTaskQueueManagerImpl()
        self.cloud_tasks_client = tasks_v2.CloudTasksClient()
        self.bq_client = BigQueryClientImpl()
        self.cache_key = f"{self.__class__}V2"

    def hydrate_cache(self) -> None:
        latest_jobs = get_all_latest_ingest_jobs()
        self.set_cache(latest_jobs)

    def set_cache(
        self,
        latest_jobs: Dict[
            StateCode,
            Optional[DataflowPipelineMetadataResponse],
        ],
    ) -> None:

        jobs_dict = {
            state_code.value: (attr_to_json_dict(job) if job else None)  # type: ignore[arg-type]
            for state_code, job in latest_jobs.items()
        }

        jobs_json = json.dumps(jobs_dict)

        self.redis.set(
            self.cache_key,
            jobs_json,
        )

    def get_most_recent_dataflow_job_statuses(
        self,
    ) -> Dict[StateCode, Optional[DataflowPipelineMetadataResponse]]:
        """Retrieve the most recent dataflow job status for each state from the cache if available, or via
        new requests to the dataflow API."""
        jobs_json = self.redis.get(self.cache_key)
        if not jobs_json:
            self.hydrate_cache()
            jobs_json = self.redis.get(self.cache_key)

        if not jobs_json:
            raise ValueError(
                "Expected the cache to have dataflow jobs hydrated by this point."
            )
        parsed_jobs_dict = json.loads(jobs_json)

        rehydrated_jobs: Dict[
            StateCode,
            Optional[DataflowPipelineMetadataResponse],
        ] = defaultdict()
        for state_code in get_direct_ingest_states_launched_in_env():
            # There is an edge case where if a state was newly added, it would not
            # appear in the cache results yet. We allow for this and just add a None
            # value for pipeline results until the cache is next hydrated.
            job = parsed_jobs_dict.get(state_code.value)
            rehydrated_jobs[state_code] = (
                assert_type(attr_from_json_dict(job), DataflowPipelineMetadataResponse)
                if job
                else None
            )

        return rehydrated_jobs

    @property
    def state_codes_launched_in_env(self) -> List[StateCode]:
        return get_direct_ingest_states_launched_in_env()

    def _verify_clean_secondary_raw_data_state(self, state_code: StateCode) -> None:
        """Confirm that all raw file metadata / data has been invalidated and the BQ raw data dataset is clean in
        SECONDARY."""
        raw_data_manager = DirectIngestRawFileMetadataManager(
            region_code=state_code.value,
            raw_data_instance=DirectIngestInstance.SECONDARY,
        )

        # Confirm there aren't non-invalidated raw files for the instance. The metadata state should be completely
        # clean before kicking off a rerun.
        if len(raw_data_manager.get_non_invalidated_files()) != 0:
            raise DirectIngestInstanceError(
                "Cannot kick off ingest rerun, as there are still unprocessed raw files on Postgres."
            )

        # Confirm that all the tables in the `us_xx_raw_data_secondary` on BQ are empty
        secondary_raw_data_dataset = raw_tables_dataset_for_region(
            state_code=state_code, instance=DirectIngestInstance.SECONDARY
        )
        query = (
            "SELECT SUM(size_bytes) as total_bytes FROM "
            f"{metadata.project_id()}.{secondary_raw_data_dataset}.__TABLES__"
        )
        query_job = self.bq_client.run_query_async(
            query_str=query, use_query_cache=False
        )
        results = list(query_job)
        if int(results[0]["total_bytes"]) > 0:
            raise DirectIngestInstanceError(
                f"There are tables in {secondary_raw_data_dataset} that are not empty. Cannot proceed with "
                f"ingest rerun."
            )

    def trigger_task_scheduler(
        self, state_code: StateCode, instance: DirectIngestInstance
    ) -> None:
        """This function creates a cloud task to schedule the next job for a given state code and instance.
        Requires:
        - state_code: (required) State code to start ingest for (i.e. "US_ID")
        - instance: (required) Which instance to start ingest for (either PRIMARY or SECONDARY)
        """
        can_start_ingest = state_code in self.state_codes_launched_in_env

        formatted_state_code = state_code.value.lower()
        region = get_direct_ingest_region(formatted_state_code)

        logging.info(
            "Creating cloud task to schedule next job and kick ingest for %s instance in %s.",
            instance,
            formatted_state_code,
        )
        self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=region,
            ingest_instance=instance,
            can_start_ingest=can_start_ingest,
        )

    def update_ingest_queues_state(
        self, state_code: StateCode, new_queue_state: str
    ) -> None:
        """This function is called through the Ingest Operations UI in the admin panel.
        It updates the state of the ingest-related queues by either pausing or resuming the
        queues.

        Requires:
        - state_code: (required) State code to pause queues for
        - new_queue_state: (required) The state to set the queues
        """
        self.cloud_task_manager.update_ingest_queue_states_str(
            state_code=state_code, new_queue_state_str=new_queue_state
        )

    def purge_ingest_queues(
        self,
        state_code: StateCode,
    ) -> None:
        """This function is called through the flash checklist in the admin panel. It purges all tasks in the
        ingest queues for the specified state."""
        queues_to_purge = sorted(get_direct_ingest_queues_for_state(state_code))

        for queue in queues_to_purge:
            self.cloud_task_manager.purge_queue(queue_name=queue)

    def get_ingest_queue_states(self, state_code: StateCode) -> List[Dict[str, str]]:
        """Returns a list of dictionaries that contain the name and states of direct ingest queues for a given region"""
        ingest_queue_states = self.cloud_task_manager.get_ingest_queue_states(
            state_code
        )

        return [
            {"name": queue_info["name"], "state": queue_info["state"].name}
            for queue_info in ingest_queue_states
        ]

    def start_secondary_raw_data_reimport(self, state_code: StateCode) -> None:
        """Enables the SECONDARY instance for |state_code| so that it can import
        any raw files in the SECONDARY GCS ingest bucket to the us_xx_raw_data_secondary
        dataset in BigQuery.
        """
        instance = DirectIngestInstance.SECONDARY

        formatted_state_code = state_code.value.lower()

        region = direct_ingest_regions.get_direct_ingest_region(
            region_code=formatted_state_code
        )
        if not self.cloud_task_manager.all_ingest_instance_queues_are_empty(
            region, instance
        ):
            raise DirectIngestInstanceError(
                "Cannot kick off raw datat reimport because not all related Cloud Task "
                "queues are empty. Please check queues on Ingest Operations Admin "
                "Panel to see which have remaining tasks."
            )

        self._verify_clean_secondary_raw_data_state(state_code)

        instance_status_manager = DirectIngestInstanceStatusManager(
            region_code=formatted_state_code,
            ingest_instance=instance,
        )
        # Validation that this is a valid status transition is handled within the
        # instance manager.
        instance_status_manager.change_status_to(
            DirectIngestStatus.RAW_DATA_REIMPORT_STARTED
        )

        self.trigger_task_scheduler(state_code, instance)

    def get_ingest_instance_resources(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> Dict[str, Any]:
        """Returns a dictionary containing the following info for the provided instance:
        i.e. {
            storageDirectoryPath: storage directory absolute path,
            ingestBucketPath: ingest bucket path,
        }
        """
        formatted_state_code = state_code.value.lower()

        # Get the ingest bucket path
        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code=formatted_state_code,
            ingest_instance=ingest_instance,
            project_id=metadata.project_id(),
        )

        # Get the storage bucket for this instance
        storage_bucket_path = gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=formatted_state_code,
            ingest_instance=ingest_instance,
            project_id=metadata.project_id(),
        )

        return {
            "storageDirectoryPath": storage_bucket_path.abs_path(),
            "ingestBucketPath": ingest_bucket_path.abs_path(),
        }

    def get_ingest_raw_file_processing_status(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> List[Dict[str, Any]]:
        """Returns a list of dictionaries containing the following info for filetags in the provided instance:
        i.e. [{
            fileTag: the file tag name,
            hasConfig: whether a raw file config exists for this file tag,
            numberFilesInBucket: number of files in the ingest bucket for this file tag,
            numberUnprocessedFiles: number of files that have not been processed for this file tag,
            numberProcessedFiles: number of files that have been processed,
            latestDiscoveryTime: most recent discovery time for this file tag,
            latestProcessedTime: most recent processed time for this file tag,
            containsDelayedFiles: if there are files that are more than 24 hours delayed from the latestProcessedTime,
        }]
        """
        formatted_state_code = state_code.value.lower()
        region = get_direct_ingest_region(formatted_state_code)

        ingest_bucket_file_tag_counts = self._get_ingest_bucket_file_tag_counts(
            state_code, ingest_instance
        )
        operations_db_file_tag_summaries = self._get_raw_file_metadata_summaries(
            state_code, ingest_instance
        )
        region_config = DirectIngestRegionRawFileConfig(
            region_code=region.region_code,
            region_module=region.region_module,
        )

        tags_with_configs = region_config.raw_file_tags

        all_file_tags = {
            *ingest_bucket_file_tag_counts.keys(),
            *operations_db_file_tag_summaries.keys(),
            *tags_with_configs,
        }

        all_file_tag_metadata = []
        for file_tag in all_file_tags:
            file_tag_metadata = {
                "fileTag": file_tag,
                "hasConfig": False,
                "numberFilesInBucket": 0,
                "numberUnprocessedFiles": 0,
                "numberProcessedFiles": 0,
                "latestDiscoveryTime": None,
                "latestProcessedTime": None,
                "latestUpdateDatetime": None,
                "isStale": False,
            }

            if file_tag in tags_with_configs:
                file_tag_metadata["hasConfig"] = True
            if file_tag in ingest_bucket_file_tag_counts:
                file_tag_metadata[
                    "numberFilesInBucket"
                ] = ingest_bucket_file_tag_counts[file_tag]

            if file_tag in operations_db_file_tag_summaries:
                summary = operations_db_file_tag_summaries[file_tag]

                if file_tag in region_config.raw_file_configs:
                    raw_file_config = region_config.raw_file_configs[file_tag]
                    file_tag_metadata["isStale"] = self.calculate_if_file_is_stale(
                        summary.latest_discovery_time, raw_file_config
                    )

                file_tag_metadata = {
                    **file_tag_metadata,
                    "numberUnprocessedFiles": summary.num_unprocessed_files,
                    "numberProcessedFiles": summary.num_processed_files,
                    "latestDiscoveryTime": summary.latest_discovery_time.isoformat(),
                    "latestProcessedTime": (
                        summary.latest_processed_time.isoformat()
                        if summary.latest_processed_time
                        else None
                    ),
                    "latestUpdateDatetime": (
                        summary.latest_update_datetime.isoformat()
                        if summary.latest_update_datetime
                        else None
                    ),
                }
            all_file_tag_metadata.append(file_tag_metadata)

        return all_file_tag_metadata

    def _get_ingest_bucket_file_tag_counts(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> Counter[str]:
        """
        Returns a counter of file tag names to the number of files in the ingest bucket for that file tag.
        """
        ingest_bucket_path = gcsfs_direct_ingest_bucket_for_state(
            region_code=state_code.value.lower(),
            ingest_instance=ingest_instance,
            project_id=metadata.project_id(),
        )

        files_in_bucket = [
            p
            for p in self.fs.ls_with_blob_prefix(
                bucket_name=ingest_bucket_path.bucket_name, blob_prefix=""
            )
            if isinstance(p, GcsfsFilePath)
        ]

        file_tag_counts: Counter[str] = Counter()
        for file_path in files_in_bucket:
            if GcsfsDirectoryPath.from_file_path(file_path).relative_path != "":
                file_tag_counts["IGNORED_IN_SUBDIRECTORY"] += 1
                continue
            try:
                file_tag_counts[filename_parts_from_path(file_path).file_tag] += 1
            except DirectIngestError as e:
                logging.warning(
                    "Error getting file tag for file [%s]: %s", file_path, e
                )
                file_tag_counts["UNNORMALIZED"] += 1

        return file_tag_counts

    @staticmethod
    def _get_raw_file_metadata_summaries(
        state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> Dict[str, DirectIngestRawFileMetadataSummary]:
        """Returns the raw file metadata summary for all file tags
        in a given state_code in the operations DB
        """
        raw_file_metadata_manager = DirectIngestRawFileMetadataManager(
            region_code=state_code.value,
            raw_data_instance=ingest_instance,
        )
        return {
            raw_file_metadata.file_tag: raw_file_metadata
            for raw_file_metadata in raw_file_metadata_manager.get_metadata_for_all_raw_files_in_region()
        }

    def get_all_current_ingest_instance_statuses(
        self,
    ) -> Dict[StateCode, Dict[DirectIngestInstance, DirectIngestInstanceStatus]]:
        """Returns the current status of each ingest instance for states in the given project."""

        ingest_statuses = {}
        for state_code in get_direct_ingest_states_launched_in_env():
            instance_to_status_dict: Dict[
                DirectIngestInstance, DirectIngestInstanceStatus
            ] = {}
            for i_instance in DirectIngestInstance:  # new direct ingest instance
                status_manager = DirectIngestInstanceStatusManager(
                    region_code=state_code.value,
                    ingest_instance=i_instance,
                )

                curr_status_info = status_manager.get_current_status_info()
                instance_to_status_dict[i_instance] = curr_status_info

            ingest_statuses[state_code] = instance_to_status_dict

        return ingest_statuses

    def get_all_latest_raw_data_import_run_info(
        self,
    ) -> Dict[StateCode, Optional[LatestDirectIngestRawFileImportRunSummary]]:
        raw_data_import_history = {}
        for state_code in get_direct_ingest_states_launched_in_env():
            raw_data_import_history[state_code] = DirectIngestRawFileImportManager(
                state_code.value, DirectIngestInstance.PRIMARY
            ).get_most_recent_import_run_summary()

        return raw_data_import_history

    def get_all_current_lock_summaries(
        self,
    ) -> Dict[StateCode, Dict[DirectIngestRawDataLockStatus, int]]:
        """For each state, returns a map of the lock status to the number of locks with
        that status
        """
        return {
            state_code: DirectIngestRawDataResourceLockManager(
                region_code=state_code.value,
                raw_data_source_instance=DirectIngestInstance.SECONDARY,
            ).get_current_lock_summary()
            for state_code in get_direct_ingest_states_launched_in_env()
        }

    @staticmethod
    def calculate_if_file_is_stale(
        latest_discovery_time: datetime, config: DirectIngestRawFileConfig
    ) -> bool:

        if not config.has_regularly_updated_data():
            return False

        return latest_discovery_time < datetime.now(pytz.utc) - timedelta(
            hours=config.max_hours_before_stale()
        )
