# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Functions and classes for calling the dataflow API for ingest pipeline metadata."""
import concurrent.futures
import datetime
import os
from collections import defaultdict
from concurrent import futures
from typing import Dict, List, Optional, Union

import attr
from google.cloud import dataflow_v1beta3

import recidiviz
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import state_dataset_for_state_code
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataflow_dataset,
)
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_job_manager import (
    DirectIngestDataflowJobManager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_watermark_manager import (
    DirectIngestDataflowWatermarkManager,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.utils import metadata


@attr.define(kw_only=True)
class DataflowPipelineMetadataResponse:
    id: str
    project_id: str
    name: str
    create_time: float
    start_time: float
    termination_time: float
    termination_state: str
    location: str

    @property
    def duration(self) -> float:
        return self.termination_time - self.start_time

    def for_api(self) -> Dict[str, Union[str, float]]:
        """Serializes the instance status as a dictionary that can be passed to the
        frontend.
        """
        return {
            "id": self.id,
            "projectId": self.project_id,
            "name": self.name,
            "createTime": self.create_time,
            "startTime": self.start_time,
            "terminationTime": self.termination_time,
            "terminationState": self.termination_state,
            "location": self.location,
            "duration": self.duration,
        }


PIPELINE_CONFIG_YAML_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__), "pipelines/calculation_pipeline_templates.yaml"
)


def get_latest_job_for_state_instance(
    state_code: StateCode, instance: DirectIngestInstance
) -> Optional[DataflowPipelineMetadataResponse]:
    """Get the latest job metadata for a state and instance from the Dataflow API."""
    # TODO(#209930): remove this check once dataflow is launched for all states
    if state_code not in DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE:
        return None

    location = DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE[state_code]
    client = dataflow_v1beta3.JobsV1Beta3Client()

    # TODO(#25528): Instead of calling get_job_id_for_most_recent_job() in here every
    #  time, call this once before we dispatch any futures, then dispatch futures
    #  with the job_id as an argument.
    job_id = DirectIngestDataflowJobManager().get_job_id_for_most_recent_job(
        state_code, instance
    )

    if job_id:
        request = dataflow_v1beta3.GetJobRequest(
            project_id=metadata.project_id(),
            job_id=job_id,
            location=location,
        )
        response = client.get_job(request=request)

        return DataflowPipelineMetadataResponse(
            id=response.id,
            project_id=response.project_id,
            name=response.name,
            create_time=response.create_time.timestamp(),  # type: ignore[attr-defined]
            start_time=response.start_time.timestamp(),  # type: ignore[attr-defined]
            termination_time=response.current_state_time.timestamp(),  # type: ignore[attr-defined]
            termination_state=response.current_state.name,
            location=response.location,
        )

    return None


def get_all_latest_ingest_jobs() -> (
    Dict[
        StateCode,
        Dict[DirectIngestInstance, Optional[DataflowPipelineMetadataResponse]],
    ]
):
    """Get the latest job for each ingest pipeline."""
    with futures.ThreadPoolExecutor(max_workers=20) as executor:
        jobs_by_state_instance: Dict[
            StateCode,
            Dict[DirectIngestInstance, Optional[DataflowPipelineMetadataResponse]],
        ] = defaultdict(dict)

        locations_futures = {
            executor.submit(get_latest_job_for_state_instance, state_code, instance): (
                state_code,
                instance,
            )
            for state_code in get_direct_ingest_states_launched_in_env()
            for instance in DirectIngestInstance
        }
        for future in concurrent.futures.as_completed(locations_futures):
            state_code, instance = locations_futures[future]
            job = future.result()
            jobs_by_state_instance[state_code][instance] = job

        return jobs_by_state_instance


def get_latest_run_raw_data_watermarks(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> Dict[str, datetime.datetime]:
    return (
        DirectIngestDataflowWatermarkManager().get_raw_data_watermarks_for_latest_run(
            state_code, ingest_instance
        )
    )


def get_raw_data_tags_not_meeting_watermark(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> List[str]:
    """Returns the raw data file tags with data that is older than the data used in the last non-invalidated ingest dataflow pipeline run."""
    watermarks_by_file_tag: Dict[
        str, datetime.datetime
    ] = DirectIngestDataflowWatermarkManager().get_raw_data_watermarks_for_latest_run(
        state_code, ingest_instance
    )

    latest_upper_bound_by_file_tag: Dict[str, datetime.datetime] = {
        info.file_tag: info.latest_update_datetime
        for info in PostgresDirectIngestRawFileMetadataManager(
            str(state_code.value), ingest_instance
        ).get_metadata_for_all_raw_files_in_region()
        if info.latest_update_datetime is not None
    }

    stale_file_tags = [
        file_tag
        for file_tag, watermark in watermarks_by_file_tag.items()
        if (
            file_tag not in latest_upper_bound_by_file_tag
            or watermarks_by_file_tag[file_tag]
            > latest_upper_bound_by_file_tag[file_tag]
        )
    ]
    return stale_file_tags


def get_latest_run_ingest_view_results(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> Dict[str, int]:
    bq_client = BigQueryClientImpl()
    return bq_client.get_row_counts_for_tables(
        ingest_view_materialization_results_dataflow_dataset(
            state_code, ingest_instance
        )
    )


def get_latest_run_state_results(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> Dict[str, int]:
    bq_client = BigQueryClientImpl()
    return bq_client.get_row_counts_for_tables(
        state_dataset_for_state_code(state_code, ingest_instance)
    )
