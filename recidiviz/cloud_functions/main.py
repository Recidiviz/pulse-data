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

"""This file contains all of the relevant cloud functions"""
import logging
import os
import traceback

from cloud_function_utils import IAP_CLIENT_ID, make_iap_request, \
    get_state_region_code_from_direct_ingest_bucket, \
    get_dataflow_template_bucket, \
    trigger_dataflow_job_from_template
from covid import covid_ingest


_STATE_AGGREGATE_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/cloud_function/state_aggregate?bucket={}&state={}'
    '&filename={}')
_DIRECT_INGEST_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/direct/handle_direct_ingest_file?region={}'
    '&bucket={}&relative_file_path={}&start_ingest={}')
_VIEW_DATA_EXPORT_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/cloud_function/view_data_export'
)
_DATAFLOW_MONITOR_URL = (
    'http://{}.appspot.com/cloud_function/dataflow_monitor?job_id={}'
    '&location={}&topic={}'
)


def parse_state_aggregate(data, _):
    """This function is triggered when a file is dropped into the state
    aggregate bucket and makes a request to parse and write the data to the
    aggregate table database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.
    """
    bucket = data['bucket']
    state, filename = data['name'].split('/')
    project_id = os.environ.get('GCP_PROJECT')
    logging.info(
        "Running cloud function for bucket %s, state %s, filename %s",
        bucket, state, filename)
    url = _STATE_AGGREGATE_CLOUD_FUNCTION_URL.format(
        project_id, bucket, state, filename)
    # Hit the cloud function backend, which persists the table data to our
    # database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)


def handle_state_direct_ingest_file(data, _):
    """This function is triggered when a file is dropped into any of the state
    direct ingest buckets and makes a request to parse and write the data to
    the database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    _handle_state_direct_ingest_file(data, start_ingest=True)


def handle_state_direct_ingest_file_rename_only(data, _):
    """Cloud functions can be configured to trigger this function instead of
    handle_state_direct_ingest_file when a region has turned on nightly
    ingest before we are ready to schedule and process ingest jobs for that
    region. This will just rename the incoming files to have a normalized path
    with a timestamp so subsequent nightly uploads do not have naming conflicts.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    _handle_state_direct_ingest_file(data, start_ingest=False)


def _handle_state_direct_ingest_file(data,
                                     start_ingest: bool):
    """Calls direct ingest cloud function when a new file is dropped into a
    bucket."""
    project_id = os.environ.get('GCP_PROJECT')
    if not project_id:
        logging.error('No project id set for call to direct ingest cloud '
                      'function, returning.')
        return

    bucket = data['bucket']
    relative_file_path = data['name']
    region_code = get_state_region_code_from_direct_ingest_bucket(bucket)
    if not region_code:
        logging.error('Cannot parse region code from bucket %s, returning.',
                      bucket)
        return

    url = _DIRECT_INGEST_CLOUD_FUNCTION_URL.format(
        project_id, region_code, bucket, relative_file_path, str(start_ingest))

    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which will schedule jobs to parse
    # data for unprocessed files in this bucket and persist to our database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)


def export_view_data(_event, _context):
    """This function is triggered by a Pub/Sub event to begin the export of data contained in BigQuery views to files
    in cloud storage buckets.
    """
    project_id = os.environ.get('GCP_PROJECT')
    if not project_id:
        logging.error('No project id set for call to export view data, returning.')
        return
    url = _VIEW_DATA_EXPORT_CLOUD_FUNCTION_URL.format(project_id)

    logging.info("project_id: %s", project_id)
    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which exports view data to their assigned cloud storage bucket
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)


def trigger_calculation_pipeline_dag(_event, _context):
    """This function is triggered by a Pub/Sub event, triggers an Airflow DAG where all
    the calculation pipelines run simultaneously.
    """
    project_id = os.environ.get('GCP_PROJECT') + '-airflow'
    if not project_id:
        logging.error('No project id set for call to run the calculation pipelines, returning.')
        return

    webserver_id = os.environ.get('WEBSERVER_ID')
    if not webserver_id:
        logging.error("The environment variable 'WEBSERVER_ID' is not set")
        return
    # The name of the DAG you wish to trigger
    dag_name = 'calculation_pipeline_dag'
    webserver_url = 'https://{}.appspot.com/api/experimental/dags/{}/dag_runs'.format(webserver_id, dag_name)

    monitor_response = make_iap_request(webserver_url, IAP_CLIENT_ID[project_id], method='POST')
    logging.info("The monitoring Airflow response is %s", monitor_response)


def run_calculation_pipelines(_event, _context):
    """This function, which is triggered by a Pub/Sub event, kicks off a
    Dataflow job with the given job_name where the template for the job lives at
    gs://{bucket}/templates/{template_name} for the given project.

    On successful triggering of the job, this function makes a call to the app
    to begin monitoring the progress of the job.
    """
    project_id = os.environ.get('GCP_PROJECT')
    if not project_id:
        logging.error('No project id set for call to run a calculation'
                      ' pipeline, returning.')
        return

    bucket = get_dataflow_template_bucket(project_id)

    template_name = os.environ.get('TEMPLATE_NAME')
    if not template_name:
        logging.error('No template_name set, returning.')
        return

    job_name = os.environ.get('JOB_NAME')
    if not job_name:
        logging.error('No job_name set, returning.')
        return

    on_dataflow_job_completion_topic = os.environ.get('ON_DATAFLOW_JOB_COMPLETION_TOPIC')
    if not on_dataflow_job_completion_topic:
        logging.error('No on-completion topic set, returning.')
        return

    response = trigger_dataflow_job_from_template(project_id, bucket,
                                                  template_name, job_name)

    logging.info("The response to triggering the Dataflow job is: %s", response)

    job_id = response['id']
    location = response['location']
    on_dataflow_job_completion_topic = on_dataflow_job_completion_topic.replace('.', '-')

    # Monitor the successfully triggered Dataflow job
    url = _DATAFLOW_MONITOR_URL.format(project_id, job_id, location, on_dataflow_job_completion_topic)

    monitor_response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The monitoring Dataflow response is %s", monitor_response)


def handle_covid_ingest_on_upload(_data, _context):
    """Ingests and aggregates the currently available COVID data into an output
    file stored in a GCP bucket.

    This function is triggered when a new source file is uploaded to the input
    GCP bucket, as defined in the handle-covid-source-upload cloud function on
    GCP.
    """
    _ingest_and_aggregate_covid_data()


def handle_covid_ingest_on_trigger(_request):
    """Ingests and aggregates the currently available COVID data into an output
    file stored in a GCP bucket.

    This function is triggered by call to a URL endpoint defined in the
    execute-covid-aggregation cloud function on GCP. This URL is intended to be
    called by an App Engine endpoint defined in covid.covid_ingest_endpoint.

    (Note that this does not follow the same pattern as most of the other App
    Engine-Cloud Function interactions in this file, which delegate in the
    other direction. Keeping the logic in the cloud function here is intended to
    enable quicker redeploys when the ingest data formats change.)
    """
    _ingest_and_aggregate_covid_data()


def _ingest_and_aggregate_covid_data():
    """Calls main COVID ingest function"""
    # TODO(https://issuetracker.google.com/issues/155215191): zdg2102
    #  remove this try-except wrapper once GCP cloud function
    try:
        logging.info('COVID ingest cloud function triggered')
        covid_ingest.ingest_and_aggregate_latest_data()
        logging.info('COVID ingest cloud function completed')
    except Exception:
        raise RuntimeError('Stack trace: {}'.format(traceback.format_exc()))
