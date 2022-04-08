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
import datetime
import os
from typing import Any, Dict, List, NamedTuple, Optional

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.trigger_rule import TriggerRule

# Custom Airflow operators in the recidiviz.airflow.dags.operators package are imported into the
# Cloud Composer environment at the top-level. However, for unit tests, we still need to
# import the recidiviz-top-level.
try:
    from operators.iap_httprequest_operator import (  # type: ignore
        IAPHTTPRequestOperator,
    )
    from operators.recidiviz_dataflow_operator import (  # type: ignore
        RecidivizDataflowTemplateOperator,
    )
    from utils.export_tasks_config import (  # type: ignore
        CASE_TRIAGE_STATES,
        PIPELINE_AGNOSTIC_EXPORTS,
    )
except ImportError:
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

from recidiviz.utils.yaml_dict import YAMLDict

GCP_PROJECT_STAGING = "recidiviz-staging"

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT")
config_file = os.environ.get("CONFIG_FILE")

default_args = {
    "start_date": datetime.date.today().strftime("%Y-%m-%d"),
    "email": ["alerts@recidiviz.org"],
    "email_on_failure": True,
}

PipelineConfigArgs = NamedTuple(
    "PipelineConfigArgs",
    [("state_code", str), ("pipeline_name", str), ("staging_only", Optional[bool])],
)


def trigger_export_operator(export_name: str) -> PubSubPublishMessageOperator:
    # TODO(#4593) Migrate to IAPHTTPOperator
    return PubSubPublishMessageOperator(
        task_id=f"trigger_{export_name.lower()}_bq_metric_export",
        project=project_id,
        topic="v1.export.view.data",
        messages=[{"data": bytes(export_name, "utf-8")}],
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

    dataflow_args = default_args.copy()
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


def execute_calculations(use_historical: bool, should_trigger_exports: bool) -> None:
    """This represents the overall execution of our calculation pipelines.

    The series of steps is as follows:
    1. Update the normalized state output for each state.
    2. Update the metric output for each state.
    3. Trigger BigQuery exports for each state and other datasets."""

    if config_file is None:
        raise Exception("Configuration file not specified")

    update_normalized_state = IAPHTTPRequestOperator(
        task_id="update_normalized_state",
        url=f"https://{project_id}.appspot.com/calculation_data_storage_manager/update_normalized_state_dataset",
        # This will trigger the task regardless of the failure or success of the
        # normalization pipelines
        trigger_rule=TriggerRule.ALL_DONE,
    )

    normalization_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "normalization_pipelines"
    )

    # TODO(#9010): Have the historical DAG mirror incremental DAG in everything but
    #  calculation month counts
    metric_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "historical_metric_pipelines"
        if use_historical
        else "incremental_metric_pipelines"
    )

    supplemental_dataset_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "supplemental_dataset_pipelines"
    )

    states_to_trigger = {
        pipeline.peek("state_code", str) for pipeline in metric_pipelines
    }

    metric_pipelines_by_state: Dict[str, List] = {
        state_code: [] for state_code in states_to_trigger
    }

    state_trigger_export_operators = (
        {
            state_code: trigger_export_operator(state_code)
            for state_code in states_to_trigger
        }
        if should_trigger_exports
        else {}
    )

    for state_export in state_trigger_export_operators.values():
        # This ensures that the normalized_state dataset will be updated before the
        # export for a state is triggered
        update_normalized_state >> state_export

    if should_trigger_exports:
        case_triage_export = trigger_export_operator("CASE_TRIAGE")

        # Case triage export also relies on an updated normalized_state dataset
        update_normalized_state >> case_triage_export

    for metric_pipeline in metric_pipelines:
        pipeline_config_args = get_pipeline_config_args(metric_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            metric_pipeline_operator = dataflow_operator_for_pipeline(
                pipeline_config_args, metric_pipeline
            )
            # Add the pipeline to the list of metric pipelines for this state
            metric_pipelines_by_state[pipeline_config_args.state_code] += [
                metric_pipeline_operator
            ]

            # This >> ensures that all the calculation pipelines will run before the
            # Pub / Sub message is published saying the pipelines are done.
            if should_trigger_exports:
                (
                    metric_pipeline_operator
                    >> state_trigger_export_operators[pipeline_config_args.state_code]
                )
                if pipeline_config_args.state_code in CASE_TRIAGE_STATES:
                    metric_pipeline_operator >> case_triage_export

    for normalization_pipeline in normalization_pipelines:
        pipeline_config_args = get_pipeline_config_args(normalization_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            normalization_calculation_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, normalization_pipeline
            )

            normalization_calculation_pipeline >> update_normalized_state

            for metric_pipeline in metric_pipelines_by_state[
                pipeline_config_args.state_code
            ]:
                # This ensures that all of the normalization pipelines for a state will
                # run before the metric pipelines for the state are triggered.
                normalization_calculation_pipeline >> metric_pipeline

    for supplemental_pipeline in supplemental_dataset_pipelines:
        pipeline_config_args = get_pipeline_config_args(supplemental_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            _ = dataflow_operator_for_pipeline(
                pipeline_config_args, supplemental_pipeline
            )

    # These exports don't depend on pipeline output.
    if should_trigger_exports:
        for export in PIPELINE_AGNOSTIC_EXPORTS:
            _ = trigger_export_operator(export)


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
@dag(
    dag_id=f"{project_id}_incremental_calculation_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)
def incremental_dag() -> None:
    """This executes the calculations for all of the incremental pipelines."""

    execute_calculations(use_historical=False, should_trigger_exports=True)


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
@dag(
    dag_id=f"{project_id}_historical_calculation_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)
def historical_dag() -> None:
    """This executes the calculations for all of the historical pipelines."""

    execute_calculations(use_historical=True, should_trigger_exports=False)


incremental_dag = incremental_dag()
historical_dag = historical_dag()
