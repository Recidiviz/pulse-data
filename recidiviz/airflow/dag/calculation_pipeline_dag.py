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
from typing import List, Dict
import collections

import yaml

from airflow import models
from airflow.contrib.operators.pubsub_operator import PubSubPublishOperator

try:
    from recidiviz_dataflow_operator import RecidivizDataflowTemplateOperator  # type: ignore
except ImportError:
    from recidiviz.airflow.dag.recidiviz_dataflow_operator import RecidivizDataflowTemplateOperator

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

project_id = os.environ.get('GCP_PROJECT_ID')
config_file = os.environ.get('CONFIG_FILE')

default_args = {
    'start_date': datetime.date.today().strftime('%Y-%m-%d'),
    'email': ['alerts@recidiviz.org'],
    'email_on_failure': True
}

CASE_TRIAGE_STATES = [
    'US_ID',
]


def trigger_export_operator(export_name: str) -> PubSubPublishOperator:
    # TODO(#4593) Migrate to IAPHTTPOperator
    return PubSubPublishOperator(
        task_id=f'trigger_{export_name.lower()}_bq_metric_export',
        project=project_id,
        topic='v1.export.view.data',
        messages=[{'data': b64encode(bytes(export_name, 'utf-8')).decode()}],
    )


def pipelines_by_state(pipelines) -> Dict[str, List[str]]:  # type: ignore
    pipes_by_state = collections.defaultdict(list)
    for pipe in pipelines:
        pipes_by_state[pipe['state_code']].append(pipe['job_name'])
    return pipes_by_state


with models.DAG(dag_id="{}_calculation_pipeline_dag".format(project_id),
                default_args=default_args,
                schedule_interval=None) as dag:
    if config_file is None:
        raise Exception('Configuration file not specified')

    with open(config_file) as f:
        pipeline_yaml_dicts = yaml.full_load(f)
        if pipeline_yaml_dicts:
            pipeline_dict = pipelines_by_state(pipeline_yaml_dicts['daily_pipelines'])

            covid_export = trigger_export_operator('COVID_DASHBOARD')
            case_triage_export = trigger_export_operator('CASE_TRIAGE')

            dataflow_default_args = {
                'project': project_id,
                'region': 'us-west1',
                'zone': 'us-west1-c',
                'tempLocation': 'gs://{}-dataflow-templates/staging/'.format(project_id)
            }

            for state_code, state_pipelines in pipeline_dict.items():
                state_export = trigger_export_operator(state_code)
                for pipeline_to_run in state_pipelines:
                    calculation_pipeline = RecidivizDataflowTemplateOperator(
                        task_id=pipeline_to_run,
                        template="gs://{}-dataflow-templates/templates/{}".format(project_id, pipeline_to_run),
                        job_name=pipeline_to_run,
                        dataflow_default_options=dataflow_default_args
                    )
                    # This >> ensures that all the calculation pipelines will run before the Pub / Sub message
                    # is published saying the pipelines are done.
                    calculation_pipeline >> state_export
                    calculation_pipeline >> covid_export
                    if state_code in CASE_TRIAGE_STATES:
                        calculation_pipeline >> case_triage_export

            _ = trigger_export_operator('INGEST_METADATA')
