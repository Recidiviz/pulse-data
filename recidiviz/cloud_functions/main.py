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

"""This file contains all of the relevant cloud functions"""
import logging
import os
from base64 import b64decode
from http import HTTPStatus
from typing import Any, Dict, Tuple, TypeVar

from flask import Request

# Mypy errors "Cannot find implementation or library stub for module named 'xxxx'" ignored here because cloud functions
# require that imports are declared relative to the cloud functions package itself. In general, we should avoid shipping
# complex code in cloud functions. The function itself should call an API endpoint that can live in an external package
# with proper import resolution.
from direct_ingest_bucket_name_utils import (  # type: ignore[import]
    get_region_code_from_direct_ingest_bucket,
)
from cloud_function_utils import (  # type: ignore[import]
    IAP_CLIENT_ID,
    GCP_PROJECT_ID_KEY,
    make_iap_request,
    get_dataflow_template_bucket,
    trigger_dataflow_job_from_template,
    build_query_param_string,
)


# A stand-in type for google.cloud.functions.Context for which no apparent type is available
ContextType = TypeVar("ContextType", bound=Any)


_STATE_AGGREGATE_CLOUD_FUNCTION_URL = (
    "http://{}.appspot.com/aggregate/persist_file?bucket={}&state={}&filename={}"
)
_DIRECT_INGEST_CLOUD_FUNCTION_URL = (
    "http://{}.appspot.com/direct/handle_direct_ingest_file?region={}"
    "&bucket={}&relative_file_path={}&start_ingest={}"
)

_DIRECT_INGEST_NORMALIZE_RAW_PATH_URL = (
    "http://{}.appspot.com/direct/normalize_raw_file_path"
    "?bucket={}&relative_file_path={}"
)

_METRIC_VIEW_EXPORT_CLOUD_FUNCTION_URL = (
    "http://{}.appspot.com/export/create_metric_view_data_export_task"
)
_APP_ENGINE_PO_MONTHLY_REPORT_GENERATE_EMAILS_URL = (
    "https://{}.appspot.com/reporting/start_new_batch{}"
)
_APP_ENGINE_PO_MONTHLY_REPORT_DELIVER_EMAILS_URL = (
    "https://{}.appspot.com/reporting/deliver_emails_for_batch{}"
)


def parse_state_aggregate(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered when a file is dropped into the state
    aggregate bucket and makes a request to parse and write the data to the
    aggregate table database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.
    """
    bucket = data["bucket"]
    state, filename = data["name"].split("/")
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    logging.info(
        "Running cloud function for bucket %s, state %s, filename %s",
        bucket,
        state,
        filename,
    )
    url = _STATE_AGGREGATE_CLOUD_FUNCTION_URL.format(
        project_id, bucket, state, filename
    )
    # Hit the cloud function backend, which persists the table data to our
    # database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)
    return "", response.status_code


def normalize_raw_file_path(data: Dict[str, Any]) -> Tuple[str, HTTPStatus]:
    """Cloud functions can be configured to trigger this function on any bucket that is being used as a test bed for
    automatic uploads. This will just rename the incoming files to have a normalized path with a timestamp so
    subsequent uploads do not have naming conflicts."""
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = (
            "No project id set for call to direct ingest cloud function, returning."
        )
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    bucket = data["bucket"]
    relative_file_path = data["name"]

    url = _DIRECT_INGEST_NORMALIZE_RAW_PATH_URL.format(
        project_id, bucket, relative_file_path
    )

    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which will schedule jobs to parse
    # data for unprocessed files in this bucket and persist to our database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)
    return "", response.status_code


def handle_state_direct_ingest_file(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered when a file is dropped into any of the state
    direct ingest buckets and makes a request to parse and write the data to
    the database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    return _handle_state_direct_ingest_file(data, start_ingest=True)


def handle_state_direct_ingest_file_rename_only(
    data: Dict[str, Any], _: ContextType
) -> Tuple[str, HTTPStatus]:
    """Cloud functions can be configured to trigger this function instead of
    handle_state_direct_ingest_file when a region has turned on nightly/weekly
    automatic data transfer before we are ready to schedule and process ingest
    jobs for that region (e.g. before ingest is "launched"). This will just
    rename the incoming files to have a normalized path with a timestamp
    so subsequent nightly uploads do not have naming conflicts.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    return _handle_state_direct_ingest_file(data, start_ingest=False)


def _handle_state_direct_ingest_file(
    data: Dict[str, Any], start_ingest: bool
) -> Tuple[str, HTTPStatus]:
    """Calls direct ingest cloud function when a new file is dropped into a
    bucket."""
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = (
            "No project id set for call to direct ingest cloud function, returning."
        )
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    bucket = data["bucket"]
    relative_file_path = data["name"]
    region_code = get_region_code_from_direct_ingest_bucket(bucket)
    if not region_code:
        error_str = f"Cannot parse region code from bucket {bucket}, returning."
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    url = _DIRECT_INGEST_CLOUD_FUNCTION_URL.format(
        project_id, region_code, bucket, relative_file_path, str(start_ingest)
    )

    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which will schedule jobs to parse
    # data for unprocessed files in this bucket and persist to our database.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)
    return "", response.status_code


def export_metric_view_data(
    event: Dict[str, Any], _context: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered by a Pub/Sub event to begin the export of data contained in BigQuery metric views to
    files in cloud storage buckets.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = "No project id set for call to export view data, returning."
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    if "data" in event:
        logging.info("data found")
        url = (
            _METRIC_VIEW_EXPORT_CLOUD_FUNCTION_URL.format(project_id)
            + "?export_job_filter="
            + b64decode(event["data"]).decode("utf-8")
        )
    else:
        error_str = "Missing required export_job_filter in data of the Pub/Sub message."
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    logging.info("project_id: %s", project_id)
    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which exports view data to their assigned cloud storage bucket
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)
    return "", response.status_code


def trigger_daily_calculation_pipeline_dag(
    data: Dict[str, Any], _context: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function is triggered by a Pub/Sub event, triggers an Airflow DAG where all
    the daily calculation pipelines run simultaneously.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY, "")
    if not project_id:
        error_str = (
            "No project id set for call to run the calculation pipelines, returning."
        )
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    iap_client_id = os.environ.get("IAP_CLIENT_ID")
    if not iap_client_id:
        error_str = "The environment variable 'IAP_CLIENT_ID' is not set."
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    airflow_uri = os.environ.get("AIRFLOW_URI")
    if not airflow_uri:
        error_str = "The environment variable 'AIRFLOW_URI' is not set"
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST
    # The name of the DAG you wish to trigger
    dag_name = "{}_calculation_pipeline_dag".format(project_id)
    webserver_url = "{}/api/experimental/dags/{}/dag_runs".format(airflow_uri, dag_name)

    monitor_response = make_iap_request(
        webserver_url, iap_client_id, method="POST", json={"conf": data}
    )
    logging.info("The monitoring Airflow response is %s", monitor_response)
    return "", monitor_response.status_code


def start_calculation_pipeline(
    _event: Dict[str, Any], _context: ContextType
) -> Tuple[str, HTTPStatus]:
    """This function, which is triggered by a Pub/Sub event, can kick off any single Dataflow pipeline template."""
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = (
            "No project_id set for call to run a calculation pipeline, returning."
        )
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    bucket = get_dataflow_template_bucket(project_id)

    template_name = os.environ.get("TEMPLATE_NAME")
    if not template_name:
        error_str = "No template_name set, returning."
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    job_name = os.environ.get("JOB_NAME")
    if not job_name:
        error_str = "No job_name set, returning."
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    region = os.environ.get("REGION")
    if not region:
        error_str = "No region set, returning."
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    response = trigger_dataflow_job_from_template(
        project_id, bucket, template_name, job_name, region
    )

    logging.info("The response to triggering the Dataflow job is: %s", response)
    return "", response.status_code


def handle_start_new_batch_email_reporting(request: Request) -> Tuple[str, HTTPStatus]:
    """Start a new batch of email generation for the indicated state.
    This function is the entry point for generating a new batch. It hits the App Engine endpoint `/start_new_batch`.
    It requires a JSON input containing the following keys:
        state_code: (required) State code for the report (i.e. "US_ID")
        report_type: (required) The type of report (i.e. "po_monthly_report")
        test_address: (optional) A test address to generate emails for
        region_code: (optional) The sub-region of the state to generate emails for (i.e. "US_ID_D5")
        message_body: (optional) If included, overrides the default message body.
    Args:
        request: The HTTP request. Must contain JSON with "state_code" and
        "report_type" keys, and may contain an optional "test_address" key.
    Returns:
        Nothing.
    Raises:
        Nothing. All exception raising is handled within the App Engine logic.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = "No project id set, returning"
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    request_params = request.get_json()
    if not request_params:
        error_str = "No request params, returning"
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    query_params = build_query_param_string(
        request_params,
        [
            "state_code",
            "report_type",
            "test_address",
            "region_code",
            "message_body",
        ],
    )

    url = _APP_ENGINE_PO_MONTHLY_REPORT_GENERATE_EMAILS_URL.format(
        project_id, query_params
    )

    logging.info("Calling URL: %s", url)

    # Hit the App Engine endpoint `reporting/start_new_batch`.
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)
    return "", response.status_code


def handle_deliver_emails_for_batch_email_reporting(
    request: Request,
) -> Tuple[str, HTTPStatus]:
    """Cloud function to deliver a batch of generated emails.
    It hits the App Engine endpoint `reporting/deliver_emails_for_batch`. It requires a JSON input containing the
    following keys:
        batch_id: (required) Identifier for this batch
        redirect_address: (optional) An email address to which all emails should
        be sent instead of to their actual recipients.
        cc_address: (optional) List of email addresses to include in the CC field.
            Example JSON:
            { "batch_id": "XXXX", "cc_address": ["cc_address@domain.org"] }
        subject_override: (optional) Override the subject of the sent out emails
    Args:
        request: HTTP request payload containing JSON with keys as described above
    Returns:
        Nothing.
    Raises:
        Nothing. All exception raising is handled within the App Engine logic.
    """
    project_id = os.environ.get(GCP_PROJECT_ID_KEY)
    if not project_id:
        error_str = "No project id set, returning"
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    request_params = request.get_json()
    if not request_params:
        error_str = "No request params, returning"
        logging.error(error_str)
        return error_str, HTTPStatus.BAD_REQUEST

    query_params = build_query_param_string(
        request_params,
        [
            "batch_id",
            "redirect_address",
            "cc_address",
            "subject_override",
        ],
    )

    url = _APP_ENGINE_PO_MONTHLY_REPORT_DELIVER_EMAILS_URL.format(
        project_id, query_params
    )

    logging.info("Calling URL: %s", url)
    response = make_iap_request(url, IAP_CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)
    return "", response.status_code
