# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""
The DAG configuration to run the calculation pipelines in Dataflow simultaneously.
This file is uploaded to GCS on deploy.
"""
import json
import os
import uuid
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Type

from airflow.decorators import dag
from airflow.models import BaseOperator
from airflow.providers.google.cloud.operators.tasks import CloudTasksTaskCreateOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.retry import Retry
from google.cloud import tasks_v2
from google.cloud.tasks_v2 import CloudTasksClient
from more_itertools import one
from requests import Response

from recidiviz.airflow.dags.calculation.finished_cloud_task_query_generator import (
    FinishedCloudTaskQueryGenerator,
)
from recidiviz.airflow.dags.operators.bq_result_sensor import BQResultSensor
from recidiviz.airflow.dags.operators.iap_httprequest_operator import (
    IAPHTTPRequestOperator,
)
from recidiviz.airflow.dags.operators.iap_httprequest_sensor import IAPHTTPRequestSensor
from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.export_tasks_config import PIPELINE_AGNOSTIC_EXPORTS
from recidiviz.airflow.dags.utils.state_code_branch import create_state_code_branching
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfigs,
    ProductExportConfig,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.pipelines.pipeline_parameters import (
    PipelineParameters,
    PipelineParametersT,
)
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.utils.yaml_dict import YAMLDict

GCP_PROJECT_STAGING = "recidiviz-staging"

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

if (config_file_opt := os.environ.get("CONFIG_FILE")) is None:
    raise ValueError("Configuration file not specified")
if not (project_id_opt := os.environ.get("GCP_PROJECT")):
    raise ValueError("project_id must be configured.")

config_file: str = config_file_opt
project_id: str = project_id_opt

retry: Retry = Retry(predicate=lambda _: False)


def flex_dataflow_operator_for_pipeline(
    pipeline_parameters: PipelineParameters,
) -> RecidivizDataflowFlexTemplateOperator:
    return RecidivizDataflowFlexTemplateOperator(
        task_id=pipeline_parameters.job_name,
        location=pipeline_parameters.region,
        body=pipeline_parameters.flex_template_launch_body(),
        project_id=project_id,
    )


def trigger_update_all_managed_views_operator() -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "bq-view-update"
    task_path = CloudTasksClient.task_path(
        project=project_id,
        location=queue_location,
        queue=queue_name,
        task=uuid.uuid4().hex,
    )
    task = tasks_v2.types.Task(
        name=task_path,
        app_engine_http_request={
            "http_method": "POST",
            "relative_uri": "/view_update/update_all_managed_views",
            "body": json.dumps({}).encode(),
        },
    )
    return CloudTasksTaskCreateOperator(
        task_id="trigger_update_all_managed_views_task",
        location=queue_location,
        queue_name=queue_name,
        task=task,
        retry=retry,
        # This will trigger the task regardless of the failure or success of the
        # upstream pipelines.
        trigger_rule=TriggerRule.ALL_DONE,
    )


def trigger_refresh_bq_dataset_operator(
    schema_type: str, task_group: TaskGroup
) -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "bq-view-update"
    task_path = CloudTasksClient.task_path(
        project=project_id,
        location=queue_location,
        queue=queue_name,
        task=uuid.uuid4().hex,
    )
    task = tasks_v2.types.Task(
        name=task_path,
        app_engine_http_request={
            "http_method": "POST",
            "relative_uri": "/cloud_sql_to_bq/refresh_bq_dataset",
            "body": json.dumps(
                {
                    "schema_type": schema_type.upper(),
                    "ingest_instance": "PRIMARY",  # TODO(#21016): Pass in ingest instance from parameter when added.
                }
            ).encode(),
        },
    )
    return CloudTasksTaskCreateOperator(
        task_id=f"trigger_refresh_bq_dataset_task_{schema_type}",
        location=queue_location,
        queue_name=queue_name,
        task=task,
        retry=retry,
        task_group=task_group,
    )


def trigger_validations_operator(state_code: str) -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "validations"
    task_path = CloudTasksClient.task_path(
        project=project_id,
        location=queue_location,
        queue=queue_name,
        task=uuid.uuid4().hex,
    )

    def create_validation_task(validation_state_code: str) -> tasks_v2.types.Task:
        return tasks_v2.types.Task(
            name=task_path,
            app_engine_http_request={
                "http_method": "POST",
                "relative_uri": f"/validation_manager/validate/{validation_state_code}",
                "body": json.dumps(
                    {
                        "ingest_instance": "PRIMARY",
                        # TODO(#21342): Pass in ingest instance from parameter when added.
                    }
                ).encode(),
            },
        )

    return CloudTasksTaskCreateOperator(
        task_id="trigger_validations_task",
        location=queue_location,
        queue_name=queue_name,
        task=create_validation_task(state_code),
        retry=retry,
        # This will trigger the task regardless of the failure or success of the
        # upstream pipelines.
        trigger_rule=TriggerRule.ALL_DONE,
    )


def trigger_metric_view_data_operator(
    export_job_name: str, state_code: Optional[str]
) -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "metric-view-export"
    endpoint = "/export/metric_view_data"
    task_path = CloudTasksClient.task_path(
        project=project_id,
        location=queue_location,
        queue=queue_name,
        task=uuid.uuid4().hex,
    )
    task = tasks_v2.types.Task(
        name=task_path,
        app_engine_http_request={
            "http_method": "POST",
            "relative_uri": endpoint,
            "body": json.dumps(
                {
                    "export_job_name": export_job_name,
                    "state_code": state_code,
                }
            ).encode(),
        },
    )
    return CloudTasksTaskCreateOperator(
        task_id=f"trigger_{export_job_name.lower()}{f'_{state_code.lower()}' if state_code else ''}_metric_view_data_export",
        location=queue_location,
        queue_name=queue_name,
        task=task,
        retry=retry,
    )


def create_metric_view_data_export_nodes(
    relevant_product_exports: List[ProductExportConfig],
) -> List[CloudTasksTaskCreateOperator]:
    """Creates trigger nodes and wait conditions for metric view data exports based on provided export job filter."""
    metric_view_data_triggers: List[CloudTasksTaskCreateOperator] = []
    for export in relevant_product_exports:
        export_job_name = export["export_job_name"]
        state_code = export["state_code"]
        trigger_metric_view_data = trigger_metric_view_data_operator(
            export_job_name=export_job_name,
            state_code=state_code,
        )

        wait_for_metric_view_data_export = BQResultSensor(
            task_id=f"wait_for_{export_job_name.lower()}{f'_{state_code.lower()}' if state_code else ''}_metric_view_data_export_success",
            query_generator=FinishedCloudTaskQueryGenerator(
                project_id=project_id,
                cloud_task_create_operator_task_id=trigger_metric_view_data.task_id,
                tracker_dataset_id="view_update_metadata",
                tracker_table_id="metric_view_data_export_tracker",
            ),
            timeout=(60 * 60 * 4),
        )

        trigger_metric_view_data >> wait_for_metric_view_data_export

        metric_view_data_triggers.append(trigger_metric_view_data)

    return metric_view_data_triggers


def response_can_refresh_proceed_check(response: Response) -> bool:
    """Checks whether the refresh lock can proceed is true."""
    data = response.text
    return data.lower() == "true"


def create_bq_refresh_nodes(schema_type: str) -> BQResultSensor:
    """Creates nodes that will do a bq refresh for given schema type and returns the last node."""
    task_group = TaskGroup(f"{schema_type.lower()}_bq_refresh")
    acquire_lock = IAPHTTPRequestOperator(
        task_id=f"acquire_lock_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/acquire_lock",
        url_method="POST",
        data=json.dumps(
            {
                "lock_id": str(uuid.uuid4()),
                "schema_type": schema_type.upper(),
                "ingest_instance": "PRIMARY",  # TODO(#21016): Pass in ingest instance from parameter when added.
            }
        ).encode(),
        task_group=task_group,
    )

    wait_for_can_refresh_proceed = IAPHTTPRequestSensor(
        task_id=f"wait_for_acquire_lock_success_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/check_can_refresh_proceed",
        url_method="POST",
        data=json.dumps(
            {
                "schema_type": schema_type.upper(),
                "ingest_instance": "PRIMARY",  # TODO(#21016): Pass in ingest instance from parameter when added.
            }
        ).encode(),
        response_check=response_can_refresh_proceed_check,
        task_group=task_group,
    )

    trigger_refresh_bq_dataset = trigger_refresh_bq_dataset_operator(
        schema_type, task_group
    )

    wait_for_refresh_bq_dataset = BQResultSensor(
        task_id=f"wait_for_refresh_bq_dataset_success_{schema_type}",
        query_generator=FinishedCloudTaskQueryGenerator(
            project_id=project_id,
            cloud_task_create_operator_task_id=trigger_refresh_bq_dataset.task_id,
            tracker_dataset_id="view_update_metadata",
            tracker_table_id="refresh_bq_dataset_tracker",
        ),
        timeout=(60 * 60 * 4),
        task_group=task_group,
    )

    release_lock = IAPHTTPRequestOperator(
        task_id=f"release_lock_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/release_lock",
        url_method="POST",
        data=json.dumps(
            {
                "schema_type": schema_type.upper(),
                "ingest_instance": "PRIMARY",  # TODO(#21016): Pass in ingest instance from parameter when added.
            }
        ).encode(),
        trigger_rule=TriggerRule.ALL_DONE,
        retries=1,
        task_group=task_group,
    )

    (
        acquire_lock
        >> wait_for_can_refresh_proceed
        >> trigger_refresh_bq_dataset
        >> wait_for_refresh_bq_dataset
        >> release_lock
    )

    return wait_for_refresh_bq_dataset


def create_pipeline_parameters_by_state(
    pipeline_configs: Iterable[YAMLDict], parameter_cls: Type[PipelineParametersT]
) -> Dict[str, List[PipelineParametersT]]:
    pipeline_params_by_state: Dict[str, List[PipelineParametersT]] = defaultdict(list)
    for pipeline_config in pipeline_configs:
        params = parameter_cls(project=project_id, **pipeline_config.get())  # type: ignore
        if project_id == GCP_PROJECT_STAGING or not params.staging_only:
            pipeline_params_by_state[params.state_code].append(params)
    return pipeline_params_by_state


def normalization_pipeline_branches_by_state_code() -> Dict[str, BaseOperator]:
    normalization_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "normalization_pipelines"
    )
    normalization_pipeline_params_by_state: Dict[
        str, List[NormalizationPipelineParameters]
    ] = create_pipeline_parameters_by_state(
        normalization_pipelines, NormalizationPipelineParameters
    )

    return {
        state_code: flex_dataflow_operator_for_pipeline(
            # There should only be one normalization pipeline per state
            one(parameters_list)
        )
        for state_code, parameters_list in normalization_pipeline_params_by_state.items()
    }


def post_normalization_pipeline_branches_by_state_code() -> Dict[str, TaskGroup]:
    """Creates a TaskGroup for each state that contains all the post-normalization pipelines for that state."""
    metric_pipelines = YAMLDict.from_path(config_file).pop_dicts("metric_pipelines")
    supplemental_dataset_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "supplemental_dataset_pipelines"
    )
    metric_pipeline_params_by_state = create_pipeline_parameters_by_state(
        metric_pipelines, MetricsPipelineParameters
    )
    supplemental_pipeline_parameters_by_state = create_pipeline_parameters_by_state(
        supplemental_dataset_pipelines, SupplementalPipelineParameters
    )

    all_pipeline_params_by_state: Dict[str, List[PipelineParameters]] = defaultdict(
        list
    )
    for state_code, metric_parameters in metric_pipeline_params_by_state.items():
        all_pipeline_params_by_state[state_code].extend(metric_parameters)
    for (
        state_code,
        supplemental_parameters,
    ) in supplemental_pipeline_parameters_by_state.items():
        all_pipeline_params_by_state[state_code].extend(supplemental_parameters)

    branches_by_state_code = {}
    for state_code, parameters_list in all_pipeline_params_by_state.items():
        with TaskGroup(
            group_id=f"{state_code}_dataflow_pipelines"
        ) as state_code_dataflow_pipelines:
            for parameters in parameters_list:
                flex_dataflow_operator_for_pipeline(parameters)

        branches_by_state_code[state_code] = state_code_dataflow_pipelines

    return branches_by_state_code


def validation_branches_by_state_code(
    states_to_validate: Iterable[str],
) -> Dict[str, TaskGroup]:
    branches_by_state_code = {}
    for state_code in states_to_validate:
        with TaskGroup(group_id=f"{state_code}_validations") as state_specific_group:
            trigger_state_validations = trigger_validations_operator(state_code)

            wait_for_state_validations = BQResultSensor(
                task_id="wait_for_validations_completion",
                query_generator=FinishedCloudTaskQueryGenerator(
                    project_id=project_id,
                    cloud_task_create_operator_task_id=trigger_state_validations.task_id,
                    tracker_dataset_id="validation_results",
                    tracker_table_id="validations_completion_tracker",
                ),
            )
            trigger_state_validations >> wait_for_state_validations

        branches_by_state_code[state_code] = state_specific_group
    return branches_by_state_code


def metric_export_branches_by_state_code(
    post_normalization_pipelines_by_state: Dict[str, TaskGroup],
) -> Dict[str, TaskGroup]:
    branches_by_state_code: Dict[str, TaskGroup] = {}

    # For every state with post-normalization pipelines enabled, we can create metric
    # exports, if any are configured for that state.
    for (
        state_code,
        metric_pipelines_group,
    ) in post_normalization_pipelines_by_state.items():
        relevant_product_exports = ProductConfigs.from_file(
            path=PRODUCTS_CONFIG_PATH
        ).get_export_configs_for_job_filter(state_code)
        if not relevant_product_exports:
            continue
        with TaskGroup(group_id=f"{state_code}_metric_exports") as state_metric_exports:
            create_metric_view_data_export_nodes(relevant_product_exports)

        metric_pipelines_group >> state_metric_exports

        branches_by_state_code[state_code] = state_metric_exports

    return branches_by_state_code


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
@dag(
    dag_id=f"{project_id}_calculation_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
def create_calculation_dag() -> None:
    """This represents the overall execution of our calculation pipelines.

    The series of steps is as follows:
    1. Update the normalized state output for each state.
    2. Update the metric output for each state.
    3. Trigger BigQuery exports for each state and other datasets."""
    with TaskGroup("bq_refresh") as _:
        state_bq_refresh_completion = create_bq_refresh_nodes("STATE")
        operations_bq_refresh_completion = create_bq_refresh_nodes("OPERATIONS")
        case_triage_bq_refresh_completion = create_bq_refresh_nodes("CASE_TRIAGE")

    update_normalized_state = IAPHTTPRequestOperator(
        task_id="update_normalized_state",
        url=f"https://{project_id}.appspot.com/calculation_data_storage_manager/update_normalized_state_dataset",
        # This will trigger the task regardless of the failure or success of the
        # normalization pipelines
        trigger_rule=TriggerRule.ALL_DONE,
        # this endpoint fails ephemerally sometimes and we want to retry and not fail the entire dag when this happens
        retries=3,
        # TODO(#20503): Update to use POST when passing data to endpoint
    )

    with TaskGroup(group_id="view_materialization") as view_materialization:
        trigger_update_all_views = trigger_update_all_managed_views_operator()

        wait_for_update_all_views = BQResultSensor(
            task_id="wait_for_view_update_all_success",
            query_generator=FinishedCloudTaskQueryGenerator(
                project_id=project_id,
                cloud_task_create_operator_task_id=trigger_update_all_views.task_id,
                tracker_dataset_id="view_update_metadata",
                tracker_table_id="view_update_tracker",
            ),
            timeout=(60 * 60 * 4),
        )

        trigger_update_all_views >> wait_for_update_all_views

    (
        [
            state_bq_refresh_completion,
            operations_bq_refresh_completion,
            case_triage_bq_refresh_completion,
        ]
        >> view_materialization
    )

    with TaskGroup(group_id="normalization") as normalization_task_group:
        create_state_code_branching(normalization_pipeline_branches_by_state_code())

    # Normalization pipelines should run after the BQ refresh is complete, but
    # complete before normalized_state dataset is refreshed.
    (state_bq_refresh_completion >> normalization_task_group >> update_normalized_state)

    with TaskGroup(
        group_id="post_normalization_pipelines"
    ) as post_normalization_pipelines:
        post_normalization_pipelines_by_state = (
            post_normalization_pipeline_branches_by_state_code()
        )
        create_state_code_branching(
            post_normalization_pipelines_by_state, TriggerRule.NONE_FAILED
        )

    # This ensures that all of the normalization pipelines for a state will
    # run and the normalized_state dataset will be updated before the
    # metric pipelines for the state are triggered.
    update_normalized_state >> post_normalization_pipelines

    # Metric pipelines should complete before view update starts
    post_normalization_pipelines >> view_materialization

    with TaskGroup(group_id="validations") as validations:
        create_state_code_branching(
            validation_branches_by_state_code(
                post_normalization_pipelines_by_state.keys()
            ),
            TriggerRule.NONE_FAILED,
        )

    view_materialization >> validations

    with TaskGroup(group_id="metric_exports") as metric_exports:
        with TaskGroup(group_id="state_specific_metric_exports"):
            create_state_code_branching(
                metric_export_branches_by_state_code(
                    post_normalization_pipelines_by_state
                ),
                TriggerRule.NONE_FAILED,
            )

        for export in PIPELINE_AGNOSTIC_EXPORTS:
            relevant_product_exports = ProductConfigs.from_file(
                path=PRODUCTS_CONFIG_PATH
            ).get_export_configs_for_job_filter(export)
            if not relevant_product_exports:
                continue
            with TaskGroup(group_id=f"{export}_metric_exports"):
                create_metric_view_data_export_nodes(relevant_product_exports)

    view_materialization >> metric_exports


create_calculation_dag()
