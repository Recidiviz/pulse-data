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

import yaml

from google.cloud import pubsub

from airflow import models
from airflow.operators.python_operator import PythonOperator
from recidiviz_dataflow_operator import RecidivizDataflowTemplateOperator  # type: ignore[import]


# If you update this file, you must re-upload it to the airflow/DAG file to this GCS bucket
# pylint: disable=W0104 pointless-statement

# This is the path to the directory where Composer can access the configuration file


config_file = "/home/airflow/gcs/dags/production_calculation_pipeline_templates.yaml"
publisher = pubsub.PublisherClient()
project_id = os.environ.get('GCP_PROJECT_ID')

default_args = {
    'start_date': datetime.date.today().strftime('%Y-%m-%d'),
    'email': ['alerts@recidiviz.org'],
    'email_on_failure': True
}


def send_message_to_pubsub() -> None:
    message = 'Cloud Composer DAG triggering export of data from BigQuery to GCS'
    topic_path = publisher.topic_path(project_id, 'v1.export.view.data')
    publisher.publish(topic_path, data=message.encode('utf-8'))


with models.DAG(dag_id="calculation_pipeline_dag",
                default_args=default_args,
                schedule_interval=None) as dag:
    with open(config_file) as f:
        pipeline_yaml_dicts = yaml.full_load(f)

        if pipeline_yaml_dicts:
            calculation_pipeline_completion_task = PythonOperator(
                task_id='export_view_data',
                python_callable=send_message_to_pubsub
            )
            dataflow_default_args = {
                'project': project_id,
                'region': 'us-west1',
                'zone': 'us-west1-c',
                'tempLocation': 'gs://{}-dataflow-templates/staging/'.format(project_id)
            }
            for pipeline_yaml_dict in pipeline_yaml_dicts['daily_pipelines']:
                job_name = pipeline_yaml_dict['job_name']
                calculation_pipeline = RecidivizDataflowTemplateOperator(
                    task_id=job_name,
                    template="gs://{}-dataflow-templates/templates/{}".format(project_id, job_name),
                    job_name=job_name,
                    dataflow_default_options=dataflow_default_args
                )
                # This >> ensures that all the calculation pipelines will run before the Pub / Sub message
                # is published saying the pipelines are done.
                calculation_pipeline >> calculation_pipeline_completion_task
