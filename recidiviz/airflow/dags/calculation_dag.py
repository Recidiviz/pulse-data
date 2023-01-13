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
from typing import Any, Dict, List, NamedTuple, Optional

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.tasks import CloudTasksTaskCreateOperator
from airflow.utils.state import State
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
        RecidivizDataflowTemplateOperator,
    )
    from utils.default_args import DEFAULT_ARGS  # type: ignore
    from utils.export_tasks_config import (  # type: ignore
        CASE_TRIAGE_STATES,
        PIPELINE_AGNOSTIC_EXPORTS,
    )
except ImportError:
    from recidiviz.airflow.dags.calculation.finished_cloud_task_query_generator import (
        FinishedCloudTaskQueryGenerator,
    )
    from recidiviz.airflow.dags.operators.bq_result_sensor import (
        BQResultSensor,
    )
    from recidiviz.airflow.dags.utils.export_tasks_config import (
        CASE_TRIAGE_STATES,
        PIPELINE_AGNOSTIC_EXPORTS,
    )
    from recidiviz.airflow.dags.operators.iap_httprequest_operator import (
        IAPHTTPRequestOperator,
    )
    from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
        RecidivizDataflowTemplateOperator,
    )
    from recidiviz.airflow.dags.operators.iap_httprequest_sensor import (
        IAPHTTPRequestSensor,
    )
    from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS

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

PipelineConfigArgs = NamedTuple(
    "PipelineConfigArgs",
    [("state_code", str), ("pipeline_name", str), ("staging_only", Optional[bool])],
)


def get_zone_for_region(pipeline_region: str) -> str:
    if pipeline_region in {"us-west1", "us-west3", "us-central1"}:
        return f"{pipeline_region}-a"

    if pipeline_region == "us-east1":
        return "us-east1-b"  # For some reason, 'us-east1-a' doesn't exist

    raise ValueError(f"Unexpected region: {pipeline_region}")


def get_dataflow_default_args(pipeline_config: YAMLDict) -> Dict[str, Any]:
    region = pipeline_config.peek("region", str)
    zone = get_zone_for_region(region)

    dataflow_args = DEFAULT_ARGS.copy()
    dataflow_args.update(
        {
            "project": project_id,
            "region": region,
            "zone": zone,
            # NOTE: This value must match the default value for temp_location in
            # recidiviz/calculator/pipeline/utils/pipeline_args_utils.py
            "tempLocation": f"gs://{project_id}-dataflow-templates-scratch/temp/",
        }
    )

    return dataflow_args


def get_pipeline_config_args(pipeline_config: YAMLDict) -> PipelineConfigArgs:
    state_code = pipeline_config.peek("state_code", str)
    pipeline_name = pipeline_config.peek("job_name", str)
    staging_only = pipeline_config.peek_optional("staging_only", bool)

    return PipelineConfigArgs(
        state_code=state_code, pipeline_name=pipeline_name, staging_only=staging_only
    )


def dataflow_operator_for_pipeline(
    pipeline_args: PipelineConfigArgs, pipeline_config: YAMLDict
) -> RecidivizDataflowTemplateOperator:
    dataflow_default_args = get_dataflow_default_args(pipeline_config)

    return RecidivizDataflowTemplateOperator(
        task_id=pipeline_args.pipeline_name,
        template=f"gs://{project_id}-dataflow-templates/templates/{pipeline_args.pipeline_name}",
        job_name=pipeline_args.pipeline_name,
        dataflow_default_options=dataflow_default_args,
    )


def trigger_rematerialize_views_operator() -> CloudTasksTaskCreateOperator:
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
            "relative_uri": "/view_update/rematerialize_all_deployed_views",
            "body": json.dumps({}).encode(),
        },
    )
    return CloudTasksTaskCreateOperator(
        task_id="trigger_rematerialize_views_task",
        location=queue_location,
        queue_name=queue_name,
        task=task,
        retry=retry,
        # This will trigger the task regardless of the failure or success of the
        # upstream pipelines.
        trigger_rule=TriggerRule.ALL_DONE,
    )


def trigger_update_all_managed_views_operator() -> CloudTasksTaskCreateOperator:
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
    )


def trigger_refresh_bq_dataset_operator(
    schema_type: str,
    dry_run: bool,
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
    )


def trigger_validations_operator(state_code: str) -> CloudTasksTaskCreateOperator:
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
    )


def trigger_metric_view_data_operator(
    export_job_name: str,
    state_code: Optional[str],
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
    )


def create_metric_view_data_export_nodes(
    export_job_filter: str,
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


def create_bq_refresh_nodes(
    schema_type: str, dry_run: bool = False
) -> ShortCircuitOperator:
    """Creates nodes that will do a bq refresh for given schema type and returns the last node."""
    acquire_lock = IAPHTTPRequestOperator(
        task_id=f"acquire_lock_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/acquire_lock/{schema_type}",
        url_method="POST",
        data=json.dumps({"lock_id": str(uuid.uuid4())}).encode(),
    )

    wait_for_can_refresh_proceed = IAPHTTPRequestSensor(
        task_id=f"wait_for_acquire_lock_success_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/check_can_refresh_proceed/{schema_type}",
        response_check=response_can_refresh_proceed_check,
    )

    trigger_refresh_bq_dataset = trigger_refresh_bq_dataset_operator(
        schema_type, dry_run
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
    )

    release_lock = IAPHTTPRequestOperator(
        task_id=f"release_lock_{schema_type}",
        url=f"https://{project_id}.appspot.com/cloud_sql_to_bq/release_lock/{schema_type}",
        trigger_rule=TriggerRule.ALL_DONE,
        retries=1,
    )

    (
        acquire_lock
        >> wait_for_can_refresh_proceed
        >> trigger_refresh_bq_dataset
        >> wait_for_refresh_bq_dataset
        >> release_lock
    )

    def check_if_bq_refresh_completion_success(**context: Any) -> bool:
        """Checks whether the state bq refresh was successful and returns bool."""
        wait_for_refresh_task_instance = context["dag_run"].get_task_instance(
            wait_for_refresh_bq_dataset.task_id
        )
        return wait_for_refresh_task_instance.state != State.FAILED

    bq_refresh_completion = ShortCircuitOperator(
        task_id=f"post_refresh_short_circuit_{schema_type}",
        python_callable=check_if_bq_refresh_completion_success,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    wait_for_refresh_bq_dataset >> bq_refresh_completion
    return bq_refresh_completion


def update_all_views_branch_func(
    trigger_update_all_views_task_id: str, trigger_source: str
) -> str:
    if trigger_source == "POST_DEPLOY":
        return trigger_update_all_views_task_id
    if trigger_source == "DAILY":
        return "do_not_update_all_views"
    raise ValueError(f" TRIGGER_SOURCE unexpected value: {trigger_source}")


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
        raise Exception("Configuration file not specified")

    state_bq_refresh_completion = create_bq_refresh_nodes("STATE")
    operations_bq_refresh_completion = create_bq_refresh_nodes("OPERATIONS")
    case_triage_bq_refresh_completion = create_bq_refresh_nodes("CASE_TRIAGE")

    update_normalized_state = IAPHTTPRequestOperator(
        task_id="update_normalized_state",
        url=f"https://{project_id}.appspot.com/calculation_data_storage_manager/update_normalized_state_dataset",
        # This will trigger the task regardless of the failure or success of the
        # normalization pipelines
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_view_rematerialize = trigger_rematerialize_views_operator()

    wait_for_rematerialize = BQResultSensor(
        task_id="wait_for_view_rematerialization_success",
        query_generator=FinishedCloudTaskQueryGenerator(
            project_id=project_id,
            cloud_task_create_operator_task_id=trigger_view_rematerialize.task_id,
            tracker_dataset_id="view_update_metadata",
            tracker_table_id="rematerialization_tracker",
        ),
        # TODO(#15413): Change the timeout back to the default once rematerialization
        #  is reliably back down under 2 hours.
        timeout=(60 * 60 * 4),
    )

    trigger_update_all_views = trigger_update_all_managed_views_operator()

    update_all_views_branch = BranchPythonOperator(
        task_id="update_all_views_branch",
        provide_context=True,
        python_callable=update_all_views_branch_func,
        op_kwargs={
            "trigger_update_all_views_task_id": trigger_update_all_views.task_id,
            "trigger_source": "{{ dag_run.conf['TRIGGER_SOURCE'] }}",
        },
        retries=0,
    )

    do_not_update_all_views = EmptyOperator(task_id="do_not_update_all_views")

    (
        [
            state_bq_refresh_completion,
            operations_bq_refresh_completion,
            case_triage_bq_refresh_completion,
        ]
        >> update_all_views_branch
        >> [trigger_update_all_views, do_not_update_all_views]
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
    )

    trigger_update_all_views >> wait_for_update_all_views >> trigger_view_rematerialize

    update_normalized_state >> trigger_view_rematerialize >> wait_for_rematerialize

    metric_pipelines = YAMLDict.from_path(config_file).pop_dicts("metric_pipelines")

    metric_pipelines_by_state: Dict[
        str, List[RecidivizDataflowTemplateOperator]
    ] = defaultdict(list)

    for metric_pipeline in metric_pipelines:
        pipeline_config_args = get_pipeline_config_args(metric_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            metric_pipeline_operator = dataflow_operator_for_pipeline(
                pipeline_config_args, metric_pipeline
            )

            # Metric pipelines should complete before view rematerialization starts
            metric_pipeline_operator >> trigger_view_rematerialize

            # Add the pipeline to the list of metric pipelines for this state
            metric_pipelines_by_state[pipeline_config_args.state_code].append(
                metric_pipeline_operator
            )

    normalization_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "normalization_pipelines"
    )

    for normalization_pipeline in normalization_pipelines:
        pipeline_config_args = get_pipeline_config_args(normalization_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            normalization_calculation_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, normalization_pipeline
            )

            # Normalization pipelines should run after the BQ refresh is complete, but
            # complete before normalized_state dataset is refreshed.
            (
                state_bq_refresh_completion
                >> normalization_calculation_pipeline
                >> update_normalized_state
            )

            for metric_pipeline in metric_pipelines_by_state[
                pipeline_config_args.state_code
            ]:
                # This ensures that all of the normalization pipelines for a state will
                # run before the metric pipelines for the state are triggered.
                normalization_calculation_pipeline >> metric_pipeline

    supplemental_dataset_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "supplemental_dataset_pipelines"
    )
    for supplemental_pipeline in supplemental_dataset_pipelines:
        pipeline_config_args = get_pipeline_config_args(supplemental_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            supplemental_pipeline_operator = dataflow_operator_for_pipeline(
                pipeline_config_args, supplemental_pipeline
            )

            supplemental_pipeline_operator >> trigger_view_rematerialize

    for state_code in metric_pipelines_by_state:
        trigger_state_validations = trigger_validations_operator(state_code)

        wait_for_state_validations = BQResultSensor(
            task_id=f"wait_for_{state_code.lower()}_validations_completion",
            query_generator=FinishedCloudTaskQueryGenerator(
                project_id=project_id,
                cloud_task_create_operator_task_id=trigger_state_validations.task_id,
                tracker_dataset_id="validation_results",
                tracker_table_id="validations_completion_tracker",
            ),
        )
        (
            wait_for_rematerialize
            >> trigger_state_validations
            >> wait_for_state_validations
        )

    states_to_trigger = {
        pipeline.peek("state_code", str) for pipeline in metric_pipelines
    }

    state_create_metric_view_data_export_nodes = {
        state_code: create_metric_view_data_export_nodes(state_code)
        for state_code in states_to_trigger
    }

    case_triage_create_metric_view_data_export_nodes = (
        create_metric_view_data_export_nodes("CASE_TRIAGE")
    )
    all_create_metric_view_data_export_nodes = [
        *state_create_metric_view_data_export_nodes.values(),
        case_triage_create_metric_view_data_export_nodes,
        *[
            create_metric_view_data_export_nodes(export)
            for export in PIPELINE_AGNOSTIC_EXPORTS
        ],
    ]

    for data_export_operator in all_create_metric_view_data_export_nodes:
        wait_for_rematerialize >> data_export_operator

    for state_code, metric_pipeline_operators in metric_pipelines_by_state.items():
        for metric_pipeline_operator in metric_pipeline_operators:
            # If any metric pipeline for a particular state fails, then the exports
            # for that state should not proceed.
            (
                metric_pipeline_operator
                >> state_create_metric_view_data_export_nodes[state_code]
            )
            if state_code in CASE_TRIAGE_STATES:
                (
                    metric_pipeline_operator
                    >> case_triage_create_metric_view_data_export_nodes
                )


calculation_dag = create_calculation_dag()
