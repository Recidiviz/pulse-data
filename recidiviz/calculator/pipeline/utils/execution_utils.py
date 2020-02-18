# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for executing calculation pipelines."""
import argparse
import logging

from typing import Dict

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials


def get_job_id(pipeline_options: Dict[str, str]) -> str:
    """Captures the job_id of the pipeline job specified by the given options.

    For local jobs, generates a job_id using the given job_timestamp. For jobs
    running on Dataflow, finds the currently running job with the same job name
    as the current pipeline. Note: this works because there can only be one job
    with the same job name running on Dataflow at a time.

    Args:
        pipeline_options: Dictionary containing details about the pipeline.

    Returns:
        The job_id string of the current pipeline job.

    """
    runner = pipeline_options.get('runner')

    if runner == 'DataflowRunner':
        # Job is running on Dataflow. Get job_id.
        project = pipeline_options.get('project')
        region = pipeline_options.get('region')
        job_name = pipeline_options.get('job_name')

        if not project:
            raise ValueError("No project provided in pipeline options: "
                             f"{pipeline_options}")
        if not region:
            raise ValueError("No region provided in pipeline options: "
                             f"{pipeline_options}")
        if not job_name:
            raise ValueError("No job_name provided in pipeline options: "
                             f"{pipeline_options}")

        try:
            logging.info("Looking for job_id on Dataflow.")

            service_name = 'dataflow'
            dataflow_api_version = 'v1b3'
            credentials = GoogleCredentials.get_application_default()

            dataflow = build(serviceName=service_name,
                             version=dataflow_api_version,
                             credentials=credentials)

            result = dataflow.projects().locations().jobs().list(
                projectId=project,
                location=region,
            ).execute()

            pipeline_job_id = 'none'

            for job in result['jobs']:
                if job['name'] == job_name:
                    if job['currentState'] == 'JOB_STATE_RUNNING':
                        pipeline_job_id = job['id']
                    break

            if pipeline_job_id == 'none':
                msg = "Could not find currently running job with the " \
                    f"name: {job_name}."
                logging.error(msg)
                raise LookupError(msg)

        except Exception as e:
            logging.error("Error retrieving Job ID")
            raise LookupError(e)

    else:
        # Job is running locally. Generate id from the timestamp.
        pipeline_job_id = '_local_job'
        job_timestamp = pipeline_options.get('job_timestamp')

        if not job_timestamp:
            raise ValueError("Must provide a job_timestamp for local jobs.")

        pipeline_job_id = job_timestamp + pipeline_job_id

    return pipeline_job_id


def get_dataflow_job_with_id(project, job_id, location) -> Dict[str, str]:
    """Returns information about the Dataflow job with the given `job_id`."""
    service_name = 'dataflow'
    dataflow_api_version = 'v1b3'
    credentials = GoogleCredentials.get_application_default()

    dataflow = build(serviceName=service_name,
                     version=dataflow_api_version,
                     credentials=credentials)

    return dataflow.projects().locations().jobs().get(
        projectId=project,
        jobId=job_id,
        location=location).execute()


def calculation_month_limit_arg(value) -> int:
    """Enforces the acceptable values for the calculation_month_limit parameter in the pipelines."""
    int_value = int(value)

    if int_value < -1:
        raise argparse.ArgumentTypeError("Minimum calculation_month_limit is -1")
    if int_value == 0:
        raise argparse.ArgumentTypeError("calculation_month_limit cannot be 0")
    return int_value
