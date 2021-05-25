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
from base64 import b64encode

from airflow import models
from airflow.contrib.operators.pubsub_operator import PubSubPublishOperator

try:
    from yaml_dict import YAMLDict  # type: ignore

    from recidiviz_dataflow_operator import (  # type: ignore
        RecidivizDataflowTemplateOperator,
    )
except ImportError:
    from recidiviz.airflow.dag.recidiviz_dataflow_operator import (
        RecidivizDataflowTemplateOperator,
    )
    from recidiviz.utils.yaml_dict import YAMLDict


# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get("GCP_PROJECT_ID")
config_file = os.environ.get("CONFIG_FILE")

default_args = {
    "start_date": datetime.date.today().strftime("%Y-%m-%d"),
    "email": ["alerts@recidiviz.org"],
    "email_on_failure": True,
}

CASE_TRIAGE_STATES = [
    "US_ID",
]


def trigger_export_operator(export_name: str) -> PubSubPublishOperator:
    # TODO(#4593) Migrate to IAPHTTPOperator
    return PubSubPublishOperator(
        task_id=f"trigger_{export_name.lower()}_bq_metric_export",
        project=project_id,
        topic="v1.export.view.data",
        messages=[{"data": b64encode(bytes(export_name, "utf-8")).decode()}],
    )


def get_zone_for_region(pipeline_region: str) -> str:
    if pipeline_region in {"us-west1", "us-west3", "us-central1"}:
        return pipeline_region + "-a"

    if pipeline_region == "us-east1":
        return "us-east1-b"  # For some reason, 'us-east1-a' doesn't exist

    raise ValueError(f"Unexpected region: {pipeline_region}")


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
with models.DAG(
    dag_id="{}_calculation_pipeline_dag".format(project_id),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    if config_file is None:
        raise Exception("Configuration file not specified")

    pipelines = YAMLDict.from_path(config_file).pop_dicts("daily_pipelines")

    case_triage_export = trigger_export_operator("CASE_TRIAGE")

    states_to_trigger = {pipeline.peek("state_code", str) for pipeline in pipelines}

    state_trigger_export_operators = {
        state_code: trigger_export_operator(state_code)
        for state_code in states_to_trigger
    }

    for pipeline in pipelines:
        region = pipeline.pop("region", str)
        zone = get_zone_for_region(region)
        state_code = pipeline.pop("state_code", str)

        dataflow_default_args = default_args.copy()
        dataflow_default_args.update(
            {
                "project": project_id,
                "region": region,
                "zone": zone,
                "tempLocation": f"gs://{project_id}-dataflow-templates/staging/",
            }
        )

        pipeline_name = pipeline.pop("job_name", str)

        calculation_pipeline = RecidivizDataflowTemplateOperator(
            task_id=pipeline_name,
            template=f"gs://{project_id}-dataflow-templates/templates/{pipeline_name}",
            job_name=pipeline_name,
            dataflow_default_options=dataflow_default_args,
        )
        # This >> ensures that all the calculation pipelines will run before the Pub / Sub message
        # is published saying the pipelines are done.
        calculation_pipeline >> state_trigger_export_operators[state_code]
        if state_code in CASE_TRIAGE_STATES:
            calculation_pipeline >> case_triage_export

    # These exports don't depend on pipeline output.
    _ = trigger_export_operator("COVID_DASHBOARD")
    _ = trigger_export_operator("INGEST_METADATA")
    _ = trigger_export_operator("VALIDATION_METADATA")
    _ = trigger_export_operator("JUSTICE_COUNTS")
