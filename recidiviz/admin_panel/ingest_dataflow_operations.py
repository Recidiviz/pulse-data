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
from typing import Dict, Optional, Union

import attr
from google.cloud import dataflow_v1beta3
from google.cloud.dataflow_v1beta3 import JobState, ListJobsRequest

import recidiviz
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.query.state.dataset_config import state_dataset_for_state_code
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataflow_dataset,
)
from recidiviz.ingest.direct.metadata.direct_ingest_dataflow_watermark_mananger import (
    DirectIngestDataflowWatermarkManager,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.pipeline_utils import ingest_pipeline_name
from recidiviz.utils import metadata
from recidiviz.utils.yaml_dict import YAMLDict


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
    return get_all_latest_ingest_jobs()[state_code][instance]


def get_latest_jobs_from_location_by_name(
    location: str,
) -> Dict[str, DataflowPipelineMetadataResponse]:
    """Fetch most recent job for each job name in the specified location (us-west1, us-central3, etc)."""
    client = dataflow_v1beta3.JobsV1Beta3Client()

    request = dataflow_v1beta3.ListJobsRequest(
        filter=ListJobsRequest.Filter.TERMINATED,
        project_id=metadata.project_id(),
        location=location,
    )

    # Returns a list of all jobs, ordered by termination time, descending
    page_result = client.list_jobs(request=request)

    jobs_by_name = {}
    for response in page_result:
        if response.name in jobs_by_name:
            # We have already found a more recent job
            continue
        if response.current_state in (
            JobState.JOB_STATE_DONE,
            JobState.JOB_STATE_FAILED,
        ):
            job = DataflowPipelineMetadataResponse(
                id=response.id,
                project_id=response.project_id,
                name=response.name,
                create_time=response.create_time.timestamp(),  # type: ignore[attr-defined]
                start_time=response.start_time.timestamp(),  # type: ignore[attr-defined]
                termination_time=response.current_state_time.timestamp(),  # type: ignore[attr-defined]
                termination_state=response.current_state.name,
                location=response.location,
            )
            jobs_by_name[response.name] = job

    return jobs_by_name


# TODO(#23319): Refactor to use new database table
def get_all_latest_ingest_jobs() -> (
    Dict[
        StateCode,
        Dict[DirectIngestInstance, Optional[DataflowPipelineMetadataResponse]],
    ]
):
    """Get the latest job for each ingest pipeline."""
    pipeline_configs = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)

    all_locations = {
        ingest_pipeline.peek("region", str)
        for ingest_pipeline in pipeline_configs.pop_dicts("ingest_pipelines")
    }

    # TODO(#24241) cache results inside the ingest operations store
    with futures.ThreadPoolExecutor(max_workers=20) as executor:
        latest_job_by_name = {}
        locations_futures = [
            executor.submit(get_latest_jobs_from_location_by_name, location)
            for location in all_locations
        ]
        for future in concurrent.futures.as_completed(locations_futures):
            data = future.result()
            for job_name, job in data.items():
                latest_job_by_name[job_name] = job

    jobs_by_state_instance: Dict[
        StateCode,
        Dict[DirectIngestInstance, Optional[DataflowPipelineMetadataResponse]],
    ] = defaultdict(lambda: defaultdict(None))
    for state_code in get_direct_ingest_states_launched_in_env():
        for instance in DirectIngestInstance:
            pipeline_name = ingest_pipeline_name(state_code, instance)
            latest_job = latest_job_by_name.get(pipeline_name)
            jobs_by_state_instance[state_code][instance] = latest_job
    return jobs_by_state_instance


def get_latest_run_raw_data_watermarks(
    state_code: StateCode, ingest_instance: DirectIngestInstance
) -> Dict[str, datetime.datetime]:
    return (
        DirectIngestDataflowWatermarkManager().get_raw_data_watermarks_for_latest_run(
            state_code, ingest_instance
        )
    )


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


def is_ingest_enabled_in_secondary(
    state_code: StateCode,  # pylint: disable=unused-argument
) -> bool:
    # TODO(#24240): Look at mappings to determine whether there is instance-specific logic
    return False
