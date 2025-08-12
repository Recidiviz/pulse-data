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

import attr
import pytz

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.admin_panel.ingest_dataflow_operations import (
    DataflowPipelineMetadataResponse,
    get_all_latest_ingest_jobs,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common import attr_validators
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.serialization import attr_from_json_dict, attr_to_json_dict
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_resource_lock_manager import (
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
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.errors import DirectIngestError
from recidiviz.utils import metadata
from recidiviz.utils.types import assert_type

BucketSummaryType = Dict[str, Union[str, int]]


@attr.define
class IngestRawFileProcessingStatus:
    """Contains metadata about a raw file tag's ingest processing status

    Attributes:
        file_tag (str): the file tag associated with the raw files
        has_config (bool): whether or not we have a raw file config yaml for this raw
            data file
        num_processed_files (int): the number of files that have been successfully
            imported into BigQuery
        num_unprocessed_files (int): the number of valid files that have been discovered
            but not yet imported
        num_ungrouped_files (int): the number of chunked, raw GCS files that have been
            discovered but not yet grouped into conceptual files
        latest_discovery_time (datetime.datetime | None): the most recent datetime that
            a GCS file has been discovered for this file tag
        latest_processed_time (datetime.datetime | None): the most recent successful
            import time for a this file tag
        latest_update_datetime (datetime.datetime | None): the greatest update_datetime
            associated with a file that has been successfully imported for this file tag
        is_stale (bool): whether or not this file is deemed "stale", meaning that for
            qualifying files, the number of hours between the |latest_discovery_time| and
            now exceeds the update cadence plus twelve hours
    """

    file_tag: str = attr.ib(validator=attr_validators.is_str)
    latest_discovery_time: Optional[datetime] = attr.ib(
        default=None, validator=attr_validators.is_opt_utc_timezone_aware_datetime
    )
    latest_processed_time: Optional[datetime] = attr.ib(
        default=None, validator=attr_validators.is_opt_utc_timezone_aware_datetime
    )
    latest_update_datetime: Optional[datetime] = attr.ib(
        default=None, validator=attr_validators.is_opt_utc_timezone_aware_datetime
    )
    has_config: bool = attr.ib(default=False, validator=attr_validators.is_bool)
    num_files_in_bucket: int = attr.ib(default=0, validator=attr_validators.is_int)
    num_processed_files: int = attr.ib(default=0, validator=attr_validators.is_int)
    num_unprocessed_files: int = attr.ib(default=0, validator=attr_validators.is_int)
    num_ungrouped_files: int = attr.ib(default=0, validator=attr_validators.is_int)
    is_stale: bool = attr.ib(default=False, validator=attr_validators.is_bool)

    @classmethod
    def from_metadata(
        cls,
        file_tag: str,
        metadata_summary: Optional[DirectIngestRawFileMetadataSummary],
        **kwargs: Any,
    ) -> "IngestRawFileProcessingStatus":
        if metadata_summary:
            return cls(
                file_tag=metadata_summary.file_tag,
                num_processed_files=metadata_summary.num_processed_files,
                num_unprocessed_files=metadata_summary.num_unprocessed_files,
                num_ungrouped_files=metadata_summary.num_ungrouped_files,
                latest_discovery_time=metadata_summary.latest_discovery_time,
                latest_processed_time=metadata_summary.latest_processed_time,
                latest_update_datetime=metadata_summary.latest_update_datetime,
                **kwargs,
            )

        return cls(file_tag=file_tag, **kwargs)

    @staticmethod
    def _datetime_for_api(dt: Optional[datetime]) -> Optional[str]:
        return dt.isoformat() if isinstance(dt, datetime) else dt

    def for_api(self) -> Dict[str, Any]:
        """Returns an instance of IngestRawFileProcessingStatus in a format that is
        ready to consume by the frontend and matches the format of
        constants::IngestRawFileProcessingStatus
        """
        return {
            "fileTag": self.file_tag,
            "hasConfig": self.has_config,
            "numberFilesInBucket": self.num_files_in_bucket,
            "numberUnprocessedFiles": self.num_unprocessed_files,
            "numberProcessedFiles": self.num_processed_files,
            "numberUngroupedFiles": self.num_ungrouped_files,
            "latestDiscoveryTime": self._datetime_for_api(self.latest_discovery_time),
            "latestProcessedTime": self._datetime_for_api(self.latest_processed_time),
            "latestUpdateDatetime": self._datetime_for_api(self.latest_update_datetime),
            "isStale": self.is_stale,
        }


class IngestOperationsStore(AdminPanelStore):
    """
    A store for tracking the current state of direct ingest.
    """

    def __init__(self) -> None:
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
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

    def get_raw_file_config(
        self,
        state_code: StateCode,
        file_tag: str,
    ) -> Optional[DirectIngestRawFileConfig]:
        return get_region_raw_file_config(
            region_code=state_code.value.lower(),
        ).raw_file_configs.get(file_tag)

    def get_ingest_raw_file_processing_statuses(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> List[IngestRawFileProcessingStatus]:
        """Builds an IngestRawFileProcessingStatus object for each file_tag found in the
        |state_code| and |ingest_instance| specific version of the following resources:
            - ingest file bucket
            - file metadata operations db table
            - raw file configs
        """
        ingest_bucket_file_tag_counts = self._get_ingest_bucket_file_tag_counts(
            state_code, ingest_instance
        )
        operations_db_file_tag_summaries = self._get_raw_file_metadata_summaries(
            state_code, ingest_instance
        )

        region_config = get_region_raw_file_config(
            region_code=state_code.value.lower(),
        )

        tags_with_configs = region_config.raw_file_tags

        all_file_tags = {
            *ingest_bucket_file_tag_counts.keys(),
            *operations_db_file_tag_summaries.keys(),
            *tags_with_configs,
        }

        all_file_tag_metadata = []
        for file_tag in all_file_tags:

            metadata_summary = operations_db_file_tag_summaries.get(file_tag)
            has_config = file_tag in tags_with_configs
            num_files_in_bucket = ingest_bucket_file_tag_counts.get(file_tag, 0)
            is_stale = self.calculate_if_file_is_stale(
                metadata_summary.latest_discovery_time if metadata_summary else None,
                region_config.raw_file_configs.get(file_tag),
            )

            processing_status = IngestRawFileProcessingStatus.from_metadata(
                file_tag=file_tag,
                metadata_summary=metadata_summary,
                has_config=has_config,
                num_files_in_bucket=num_files_in_bucket,
                is_stale=is_stale,
            )

            all_file_tag_metadata.append(processing_status)

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
            for p in self.fs.ls(bucket_name=ingest_bucket_path.bucket_name)
            if isinstance(p, GcsfsFilePath)
        ]
        print(files_in_bucket)

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

    def get_all_latest_raw_data_import_run_info(
        self,
    ) -> Dict[StateCode, LatestDirectIngestRawFileImportRunSummary]:
        raw_data_import_history = {}
        for state_code in get_direct_ingest_states_launched_in_env():
            raw_data_import_history[state_code] = DirectIngestRawFileImportManager(
                state_code.value, DirectIngestInstance.PRIMARY
            ).get_most_recent_import_run_summary()

        return raw_data_import_history

    def get_all_current_lock_summaries(
        self,
    ) -> Dict[
        StateCode,
        Dict[
            DirectIngestRawDataResourceLockResource,
            Optional[DirectIngestRawDataLockActor],
        ],
    ]:
        """For each state, returns a map of the lock status to the number of locks with
        that status
        """
        return {
            state_code: DirectIngestRawDataResourceLockManager(
                region_code=state_code.value,
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                with_proxy=False,
            ).get_current_lock_summary()
            for state_code in get_direct_ingest_states_launched_in_env()
        }

    @staticmethod
    def calculate_if_file_is_stale(
        latest_discovery_time: Optional[datetime],
        config: Optional[DirectIngestRawFileConfig],
    ) -> bool:

        if not latest_discovery_time or not config:
            return False

        if not config.has_regularly_updated_data():
            return False

        return latest_discovery_time < datetime.now(pytz.utc) - timedelta(
            hours=config.max_hours_before_stale()
        )
