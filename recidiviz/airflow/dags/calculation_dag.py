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
from typing import Dict, List, Optional

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.tasks import CloudTasksTaskCreateOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.retry import Retry
from google.cloud import tasks_v2
from google.cloud.tasks_v2 import CloudTasksClient
from requests import Response

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
try:
    from calculation.finished_cloud_task_query_generator import (  # type: ignore
        FinishedCloudTaskQueryGenerator,
    )
    from operators.bq_result_sensor import BQResultSensor  # type: ignore
    from operators.iap_httprequest_operator import (  # type: ignore
        IAPHTTPRequestOperator,
    )
    from operators.iap_httprequest_sensor import IAPHTTPRequestSensor  # type: ignore
    from operators.recidiviz_dataflow_operator import (  # type: ignore
        RecidivizDataflowFlexTemplateOperator,
    )
    from utils.default_args import DEFAULT_ARGS  # type: ignore
    from utils.export_tasks_config import PIPELINE_AGNOSTIC_EXPORTS  # type: ignore
    from utils.pipeline_parameters import (  # type: ignore
        MetricsPipelineParameters,
        NormalizationPipelineParameters,
        PipelineParameters,
        SupplementalPipelineParameters,
    )
except ImportError:
    from recidiviz.airflow.dags.calculation.finished_cloud_task_query_generator import (
        FinishedCloudTaskQueryGenerator,
    )
    from recidiviz.airflow.dags.operators.bq_result_sensor import (
        BQResultSensor,
    )
    from recidiviz.airflow.dags.utils.export_tasks_config import (
        PIPELINE_AGNOSTIC_EXPORTS,
    )
    from recidiviz.airflow.dags.operators.iap_httprequest_operator import (
        IAPHTTPRequestOperator,
    )
    from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
        RecidivizDataflowFlexTemplateOperator,
    )
    from recidiviz.airflow.dags.operators.iap_httprequest_sensor import (
        IAPHTTPRequestSensor,
    )
    from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
    from recidiviz.airflow.dags.utils.pipeline_parameters import (
        MetricsPipelineParameters,
        PipelineParameters,
        SupplementalPipelineParameters,
        NormalizationPipelineParameters,
    )

from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfigs,
)
from recidiviz.utils.yaml_dict import YAMLDict

GCP_PROJECT_STAGING = "recidiviz-staging"

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT")
config_file = os.environ.get("CONFIG_FILE")

retry: Retry = Retry(predicate=lambda _: False)


def flex_dataflow_operator_for_pipeline(
    pipeline_parameters: PipelineParameters,
    task_group: Optional[TaskGroup] = None,
) -> RecidivizDataflowFlexTemplateOperator:

    region = pipeline_parameters.region

    return RecidivizDataflowFlexTemplateOperator(
        task_id=pipeline_parameters.job_name,
        location=region,
        body=pipeline_parameters.flex_template_launch_body(project_id=project_id),
        project_id=project_id,
        task_group=task_group,
    )


def trigger_update_all_managed_views_operator(
    task_group: TaskGroup,
) -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "bq-view-update"
    if not project_id:
        raise ValueError("project_id must be configured.")
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
        task_group=task_group,
    )


def trigger_refresh_bq_dataset_operator(
    schema_type: str, dry_run: bool, task_group: TaskGroup
) -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "bq-view-update"
    endpoint = f"/cloud_sql_to_bq/refresh_bq_dataset/{schema_type}{'?dry_run=True' if dry_run else ''}"
    if not project_id:
        raise ValueError("project_id must be configured.")
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
            "body": json.dumps({}).encode(),
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


def trigger_validations_operator(
    state_code: str, task_group: TaskGroup
) -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "validations"
    if not project_id:
        raise ValueError("project_id must be configured.")
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
            "relative_uri": f"/validation_manager/validate/{state_code}",
            "body": json.dumps({}).encode(),
        },
    )
    return CloudTasksTaskCreateOperator(
        task_id=f"trigger_{state_code.lower()}_validations_task",
        location=queue_location,
        queue_name=queue_name,
        task=task,
        retry=retry,
        # This will trigger the task regardless of the failure or success of the
        # upstream pipelines.
        trigger_rule=TriggerRule.ALL_DONE,
        task_group=task_group,
    )


def trigger_metric_view_data_operator(
    export_job_name: str, state_code: Optional[str], task_group: TaskGroup
) -> CloudTasksTaskCreateOperator:
    queue_location = "us-east1"
    queue_name = "metric-view-export"
    endpoint = f"/export/metric_view_data?export_job_name={export_job_name}{f'&state_code={state_code}' if state_code else ''}"
    if not project_id:
        raise ValueError("project_id must be configured.")
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
            "body": json.dumps({}).encode(),
        },
    )
    return CloudTasksTaskCreateOperator(
        task_id=f"trigger_{export_job_name.lower()}{f'_{state_code.lower()}' if state_code else ''}_metric_view_data_export",
        location=queue_location,
        queue_name=queue_name,
        task=task,
        retry=retry,
        task_group=task_group,
    )


def create_metric_view_data_export_nodes(
    export_job_filter: str, task_group: TaskGroup
) -> List[CloudTasksTaskCreateOperator]:
    """Creates trigger nodes and wait conditions for metric view data exports based on provided export job filter."""
    relevant_product_exports = ProductConfigs.from_file(
        path=PRODUCTS_CONFIG_PATH
    ).get_export_configs_for_job_filter(export_job_filter)

    metric_view_data_triggers: List[CloudTasksTaskCreateOperator] = []
    for export in relevant_product_exports:
        export_job_name = export["export_job_name"]
        state_code = export["state_code"]
        trigger_metric_view_data = trigger_metric_view_data_operator(
            export_job_name=export_job_name,
            state_code=state_code,
            task_group=task_group,
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
            task_group=task_group,
        )

        trigger_metric_view_data >> wait_for_metric_view_data_export

        metric_view_data_triggers.append(trigger_metric_view_data)

    return metric_view_data_triggers


def response_can_refresh_proceed_check(response: Response) -> bool:
    """Checks whether the refresh lock can proceed is true."""
    data = response.text
    return data.lower() == "true"


def create_bq_refresh_nodes(schema_type: str, dry_run: bool = False) -> BQResultSensor:
    """Creates nodes that will do a bq refresh for given schema type and returns the last node."""
    task_group = TaskGroup(f"{schema_type.lower()}_bq_refresh")
    acquire_lock = IAPHTTPRequestOperator(
        task_id=f"acquire_lock_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/acquire_lock/{schema_type}",
        url_method="POST",
        data=json.dumps({"lock_id": str(uuid.uuid4())}).encode(),
        task_group=task_group,
    )

    wait_for_can_refresh_proceed = IAPHTTPRequestSensor(
        task_id=f"wait_for_acquire_lock_success_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/check_can_refresh_proceed/{schema_type}",
        response_check=response_can_refresh_proceed_check,
        task_group=task_group,
    )

    trigger_refresh_bq_dataset = trigger_refresh_bq_dataset_operator(
        schema_type, dry_run, task_group
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
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/release_lock/{schema_type}",
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


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
@dag(
    dag_id=f"{project_id}_calculation_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)
def create_calculation_dag() -> None:
    """This represents the overall execution of our calculation pipelines.

    The series of steps is as follows:
    1. Update the normalized state output for each state.
    2. Update the metric output for each state.
    3. Trigger BigQuery exports for each state and other datasets."""

    if config_file is None:
        raise ValueError("Configuration file not specified")
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
    )

    view_materialize_task_group = TaskGroup("view_materialization")

    trigger_update_all_views = trigger_update_all_managed_views_operator(
        view_materialize_task_group
    )

    wait_for_update_all_views = BQResultSensor(
        task_id="wait_for_view_update_all_success",
        query_generator=FinishedCloudTaskQueryGenerator(
            project_id=project_id,
            cloud_task_create_operator_task_id=trigger_update_all_views.task_id,
            tracker_dataset_id="view_update_metadata",
            tracker_table_id="view_update_tracker",
        ),
        timeout=(60 * 60 * 4),
        task_group=view_materialize_task_group,
    )

    (
        [
            state_bq_refresh_completion,
            operations_bq_refresh_completion,
            case_triage_bq_refresh_completion,
        ]
        >> trigger_update_all_views
        >> wait_for_update_all_views
    )

    metric_pipelines = YAMLDict.from_path(config_file).pop_dicts("metric_pipelines")

    metric_pipelines_by_state: Dict[
        str,
        RecidivizDataflowFlexTemplateOperator,
    ] = defaultdict(list)

    dataflow_pipeline_task_groups: Dict[str, TaskGroup] = {}
    dataflow_pipeline_task_group = TaskGroup("dataflow_pipelines")

    for metric_pipeline in metric_pipelines:

        # define both a MetricsPipelineParameters for flex templates and a legacy PipelineConfigArgs
        pipeline_config_parameters = MetricsPipelineParameters(**metric_pipeline.get())

        state_code = pipeline_config_parameters.state_code

        if state_code not in dataflow_pipeline_task_groups:
            dataflow_pipeline_task_groups[state_code] = TaskGroup(
                f"{state_code}_dataflow_pipelines",
                parent_group=dataflow_pipeline_task_group,
            )

        if (
            project_id == GCP_PROJECT_STAGING
            or not pipeline_config_parameters.staging_only
        ):
            metric_pipeline_operator = flex_dataflow_operator_for_pipeline(
                pipeline_config_parameters,
                dataflow_pipeline_task_groups[state_code],
            )
            # Metric pipelines should complete before view rematerialization starts
            metric_pipeline_operator >> trigger_update_all_views

            # This ensures that all of the normalization pipelines for a state will
            # run and the normalized_state dataset will be updated before the
            # metric pipelines for the state are triggered.
            update_normalized_state >> metric_pipeline_operator

            # Add the pipeline to the list of metric pipelines for this state
            metric_pipelines_by_state[state_code].append(metric_pipeline_operator)

    normalization_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "normalization_pipelines"
    )
    normalization_task_group = TaskGroup("normalization")
    for normalization_pipeline in normalization_pipelines:
        normalization_pipeline_parameters = NormalizationPipelineParameters(
            **normalization_pipeline.get()
        )

        if (
            project_id == GCP_PROJECT_STAGING
            or not normalization_pipeline_parameters.staging_only
        ):
            normalization_calculation_pipeline = flex_dataflow_operator_for_pipeline(
                normalization_pipeline_parameters,
                normalization_task_group,
            )

            # Normalization pipelines should run after the BQ refresh is complete, but
            # complete before normalized_state dataset is refreshed.
            (
                state_bq_refresh_completion
                >> normalization_calculation_pipeline
                >> update_normalized_state
            )

    supplemental_dataset_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "supplemental_dataset_pipelines"
    )
    for supplemental_pipeline in supplemental_dataset_pipelines:
        supplemental_pipeline_parameters = SupplementalPipelineParameters(
            **supplemental_pipeline.get()
        )
        state_code = supplemental_pipeline_parameters.state_code

        if state_code not in dataflow_pipeline_task_groups:
            dataflow_pipeline_task_groups[state_code] = TaskGroup(
                f"{state_code}_dataflow_pipelines",
                parent_group=dataflow_pipeline_task_group,
            )

        if (
            project_id == GCP_PROJECT_STAGING
            or not supplemental_pipeline_parameters.staging_only
        ):
            supplemental_pipeline_operator = flex_dataflow_operator_for_pipeline(
                supplemental_pipeline_parameters,
                dataflow_pipeline_task_groups[state_code],
            )

            supplemental_pipeline_operator >> trigger_update_all_views

            # This ensures that all of the normalization pipelines for a state will
            # run and the normalized_state dataset will be updated before the
            # supplemental pipelines for the state are triggered.
            update_normalized_state >> supplemental_pipeline_operator

    validation_task_groups: Dict[str, TaskGroup] = {}
    validation_task_group = TaskGroup("validations")
    for state_code in metric_pipelines_by_state:
        if state_code not in validation_task_groups:
            validation_task_groups[state_code] = TaskGroup(
                f"{state_code}_validations", parent_group=validation_task_group
            )
        trigger_state_validations = trigger_validations_operator(
            state_code, validation_task_groups[state_code]
        )

        wait_for_state_validations = BQResultSensor(
            task_id=f"wait_for_{state_code.lower()}_validations_completion",
            query_generator=FinishedCloudTaskQueryGenerator(
                project_id=project_id,
                cloud_task_create_operator_task_id=trigger_state_validations.task_id,
                tracker_dataset_id="validation_results",
                tracker_table_id="validations_completion_tracker",
            ),
            task_group=validation_task_groups[state_code],
        )
        (
            wait_for_update_all_views
            >> trigger_state_validations
            >> wait_for_state_validations
        )

    states_to_trigger = {
        pipeline.peek("state_code", str) for pipeline in metric_pipelines
    }
    metric_export_task_group = TaskGroup("metric_exports")
    metric_export_task_groups = {
        state_code: TaskGroup(
            f"{state_code}_metric_exports", parent_group=metric_export_task_group
        )
        for state_code in states_to_trigger
    }

    state_create_metric_view_data_export_nodes = {
        state_code: create_metric_view_data_export_nodes(
            state_code, metric_export_task_groups[state_code]
        )
        for state_code in states_to_trigger
    }

    all_create_metric_view_data_export_nodes = [
        *state_create_metric_view_data_export_nodes.values(),
        *[
            create_metric_view_data_export_nodes(
                export,
                TaskGroup(
                    f"{export}_metric_exports", parent_group=metric_export_task_group
                ),
            )
            for export in PIPELINE_AGNOSTIC_EXPORTS
        ],
    ]

    for data_export_operator in all_create_metric_view_data_export_nodes:
        wait_for_update_all_views >> data_export_operator

    for state_code, metric_pipeline_operators in metric_pipelines_by_state.items():
        for metric_pipeline_operator in metric_pipeline_operators:
            # If any metric pipeline for a particular state fails, then the exports
            # for that state should not proceed.
            (
                metric_pipeline_operator
                >> state_create_metric_view_data_export_nodes[state_code]
            )


calculation_dag = create_calculation_dag()
