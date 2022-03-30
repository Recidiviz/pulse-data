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

from airflow import models
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

from recidiviz.utils.yaml_dict import YAMLDict

# Custom Airflow plugins in the recidiviz.airflow.plugins package are imported into the
# Cloud Composer environment at the top-level. Therefore, the try-catch with the import
# is still necessary in order to satisfy both the DAG's execution and our local tests.
try:
    from recidiviz_dataflow_operator import (  # type: ignore
        RecidivizDataflowTemplateOperator,
    )
except ImportError:
    from recidiviz.airflow.plugins.recidiviz_dataflow_operator import (  # pylint: disable=ungrouped-imports
        RecidivizDataflowTemplateOperator,
    )

GCP_PROJECT_STAGING = "recidiviz-staging"

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT_ID")
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

CASE_TRIAGE_STATES = [
    "US_ID",
]


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
        return pipeline_region + "-a"

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


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
with models.DAG(
    dag_id=f"{project_id}_incremental_calculation_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as daily_dag:
    if config_file is None:
        raise Exception("Configuration file not specified")

    normalization_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "normalization_pipelines"
    )

    incremental_metric_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "incremental_metric_pipelines"
    )

    supplemental_dataset_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "supplemental_dataset_pipelines"
    )

    case_triage_export = trigger_export_operator("CASE_TRIAGE")

    states_to_trigger = {
        pipeline.peek("state_code", str) for pipeline in incremental_metric_pipelines
    }

    state_trigger_export_operators = {
        state_code: trigger_export_operator(state_code)
        for state_code in states_to_trigger
    }

    incremental_metric_pipelines_by_state: Dict[str, List] = {
        state_code: [] for state_code in states_to_trigger
    }

    for metric_pipeline in incremental_metric_pipelines:
        pipeline_config_args = get_pipeline_config_args(metric_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            incremental_metric_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, metric_pipeline
            )
            # Add the pipeline to the list of metric pipelines for this state
            incremental_metric_pipelines_by_state[pipeline_config_args.state_code] += [
                incremental_metric_pipeline
            ]

            # This >> ensures that all the calculation pipelines will run before the
            # Pub / Sub message is published saying the pipelines are done.
            (
                incremental_metric_pipeline
                >> state_trigger_export_operators[pipeline_config_args.state_code]
            )
            if pipeline_config_args.state_code in CASE_TRIAGE_STATES:
                incremental_metric_pipeline >> case_triage_export

    for normalization_pipeline in normalization_pipelines:
        pipeline_config_args = get_pipeline_config_args(normalization_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            normalization_calculation_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, normalization_pipeline
            )

            for incremental_metric_pipeline in incremental_metric_pipelines_by_state[
                pipeline_config_args.state_code
            ]:
                # This ensures that all of the normalization pipelines for a state will
                # run before the metric pipelines for the state are triggered.
                normalization_calculation_pipeline >> incremental_metric_pipeline

    for supplemental_pipeline in supplemental_dataset_pipelines:
        pipeline_config_args = get_pipeline_config_args(supplemental_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            supplemental_dataset_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, supplemental_pipeline
            )

    # These exports don't depend on pipeline output.
    _ = trigger_export_operator("COVID_DASHBOARD")
    _ = trigger_export_operator("INGEST_METADATA")
    _ = trigger_export_operator("VALIDATION_METADATA")
    _ = trigger_export_operator("JUSTICE_COUNTS")


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
with models.DAG(
    dag_id=f"{project_id}_historical_calculation_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as historical_dag:
    # TODO(#9010): Have the historical DAG mirror incremental DAG in everything but
    #  calculation month counts
    if config_file is None:
        raise Exception("Configuration file not specified")

    historical_metric_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "historical_metric_pipelines"
    )

    historical_metric_pipelines_by_state: Dict[str, List] = {
        state_code: [] for state_code in states_to_trigger
    }

    for metric_pipeline in historical_metric_pipelines:
        pipeline_config_args = get_pipeline_config_args(metric_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            historical_metric_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, metric_pipeline
            )
            # Add the pipeline to the list of metric pipelines for this state
            historical_metric_pipelines_by_state[pipeline_config_args.state_code] += [
                historical_metric_pipeline
            ]

    for normalization_pipeline in normalization_pipelines:
        pipeline_config_args = get_pipeline_config_args(normalization_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            normalization_calculation_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, normalization_pipeline
            )

            for historical_metric_pipeline in historical_metric_pipelines_by_state[
                pipeline_config_args.state_code
            ]:
                # This ensures that all of the normalization pipelines for a state will
                # run before the metric pipelines for the state are triggered.
                normalization_calculation_pipeline >> historical_metric_pipeline

    for supplemental_pipeline in supplemental_dataset_pipelines:
        pipeline_config_args = get_pipeline_config_args(supplemental_pipeline)

        if project_id == GCP_PROJECT_STAGING or not pipeline_config_args.staging_only:
            supplemental_dataset_pipeline = dataflow_operator_for_pipeline(
                pipeline_config_args, supplemental_pipeline
            )
