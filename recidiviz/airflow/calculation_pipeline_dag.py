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
from typing import List, Dict
import collections

import yaml

from airflow import models
from recidiviz_dataflow_operator import RecidivizDataflowTemplateOperator  # type: ignore
from iap_httprequest_operator import IAPHTTPRequestOperator  # type: ignore

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# This is the path to the directory where Composer can access the configuration file
config_file = "/home/airflow/gcs/dags/production_calculation_pipeline_templates.yaml"
project_id = os.environ.get('GCP_PROJECT_ID')
_VIEW_DATA_EXPORT_CLOUD_FUNCTION_URL = 'http://{}.appspot.com/cloud_function/view_data_export?export_job_filter={}'

default_args = {
    'start_date': datetime.date.today().strftime('%Y-%m-%d'),
    'email': ['alerts@recidiviz.org'],
    'email_on_failure': True
}


def pipelines_by_state(pipelines) -> Dict[str, List[str]]:  # type: ignore
    pipes_by_state = collections.defaultdict(list)
    for pipe in pipelines:
        pipes_by_state[pipe['state_code']].append(pipe['job_name'])
    return pipes_by_state


with models.DAG(dag_id="calculation_pipeline_dag",
                default_args=default_args,
                schedule_interval=None) as dag:
    with open(config_file) as f:
        pipeline_yaml_dicts = yaml.full_load(f)
        if pipeline_yaml_dicts:
            pipeline_dict = pipelines_by_state(pipeline_yaml_dicts['daily_pipelines'])
            covid_export = IAPHTTPRequestOperator(
                task_id='COVID_bq_metric_export',
                url=_VIEW_DATA_EXPORT_CLOUD_FUNCTION_URL.format(project_id, "COVID_DASHBOARD")
            )
            dataflow_default_args = {
                'project': project_id,
                'region': 'us-west1',
                'zone': 'us-west1-c',
                'tempLocation': 'gs://{}-dataflow-templates/staging/'.format(project_id)
            }
            for state_code, state_pipelines in pipeline_dict.items():
                state_export = IAPHTTPRequestOperator(
                    task_id='{}_bq_metric_export'.format(state_code),
                    url=_VIEW_DATA_EXPORT_CLOUD_FUNCTION_URL.format(project_id, state_code)
                )
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
