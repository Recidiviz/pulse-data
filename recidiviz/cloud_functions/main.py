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

from gcsfs import GCSFileSystem

from cloud_function_utils import make_iap_request, \
    get_state_region_code_from_direct_ingest_bucket, \
    get_dashboard_data_export_storage_bucket, have_seen_file_path, \
    to_normalized_unprocessed_file_path, GCSFS_NO_CACHING

_STATE_AGGREGATE_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/cloud_function/state_aggregate?bucket={}&state={}'
    '&filename={}')
_DIRECT_INGEST_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/cloud_function/start_direct_ingest?region={}')
_DASHBOARD_EXPORT_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/cloud_function/dashboard_export?bucket={}'
    '&data_type={}'
)

_CLIENT_ID = {
    'recidiviz-staging': ('984160736970-flbivauv2l7sccjsppe34p7436l6890m.apps.'
                          'googleusercontent.com'),
    'recidiviz-123': ('688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.'
                      'googleusercontent.com')
}


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
    response = make_iap_request(url, _CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)


def direct_ingest_county(data, _):
    """This function is triggered when a file is dropped into the county direct
    ingest bucket and makes a request to parse and write the data to
    the database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    bucket = data['bucket']
    relative_file_path = data['name']
    region_code, _ = relative_file_path.split('/')

    _call_direct_ingest(bucket, region_code, relative_file_path)


def direct_ingest_state(data, _):
    """This function is triggered when a file is dropped into any of the state
    direct ingest buckets and makes a request to parse and write the data to
    the database.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    bucket = data['bucket']
    relative_file_path = data['name']
    region_code = get_state_region_code_from_direct_ingest_bucket(bucket)
    if not region_code:
        logging.error('Cannot parse region code from bucket %s, returning.',
                      bucket)
        return

    _call_direct_ingest(bucket, region_code, relative_file_path)


def direct_ingest_rename_only(data, _):
    """Cloud functions can be configured to trigger this function instead of
    direct_ingest_state/direct_ingest_county when a region has turned on nightly
    ingest before we are ready to schedule and process ingest jobs for that
    region. This will just rename the incoming files to have a normalized path
    with a timestamp so subsequent nightly uploads do not have naming conflicts.

    data: A cloud storage object that holds name information and other metadata
    related to the file that was dropped into the bucket.
    _: (google.cloud.functions.Context): Metadata of triggering event.

    """
    bucket = data['bucket']
    relative_file_path = data['name']

    project_id = os.environ.get('GCP_PROJECT')
    if not project_id:
        logging.error('No project id set for call to direct ingest cloud '
                      'function, returning.')
        return

    _normalize_file_path_if_necessary(project_id, bucket, relative_file_path)
    logging.info("Finished processing file [%s]", relative_file_path)


def _normalize_file_path_if_necessary(project_id: str,
                                      bucket: str,
                                      relative_file_path: str):
    original_file_path = os.path.join(bucket, relative_file_path)
    if have_seen_file_path(original_file_path):
        logging.info(
            "Not normalizing file path for already seen file %s",
            original_file_path)
        return

    fs = GCSFileSystem(project=project_id, cache_timeout=GCSFS_NO_CACHING)

    updated_file_path = \
        to_normalized_unprocessed_file_path(original_file_path)

    if fs.exists(updated_file_path):
        logging.error("Desired path [%s] already exists, returning",
                      updated_file_path)
        return

    logging.info("Moving file from %s to %s",
                 original_file_path, updated_file_path)
    fs.mv(original_file_path, updated_file_path)


def _call_direct_ingest(bucket: str,
                        region_code: str,
                        relative_file_path: str):
    """Calls direct ingest cloud function when a new file is dropped into a
    bucket."""
    project_id = os.environ.get('GCP_PROJECT')
    if not project_id:
        logging.error('No project id set for call to direct ingest cloud '
                      'function, returning.')
        return

    _normalize_file_path_if_necessary(project_id, bucket, relative_file_path)

    url = _DIRECT_INGEST_CLOUD_FUNCTION_URL.format(project_id, region_code)

    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which will schedule jobs to parse
    # data for unprocessed files in this bucket and persist to our database.
    response = make_iap_request(url, _CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)


def export_dashboard_standard_data(_event, _context):
    """This function is triggered by a Pub/Sub event to begin the export of
    data needed for the dashboard.
    """

    _call_dashboard_export(data_type='STANDARD')


def export_dashboard_dataflow_data(_event, _context):
    """This function is triggered by a Pub/Sub event to begin the export of
    data needed for the dashboard that relies on the results of completed
    Dataflow jobs.
    """

    _call_dashboard_export(data_type='DATAFLOW')


def _call_dashboard_export(data_type: str):
    project_id = os.environ.get('GCP_PROJECT')
    if not project_id:
        logging.error('No project id set for call to export dashboard data, '
                      'returning.')
        return

    bucket = get_dashboard_data_export_storage_bucket(project_id)

    url = _DASHBOARD_EXPORT_CLOUD_FUNCTION_URL.format(project_id, bucket,
                                                      data_type)
    logging.info("project_id: %s", project_id)
    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which exports the given data type to
    # the given cloud storage bucket
    response = make_iap_request(url, _CLIENT_ID[project_id])
    logging.info("The response status is %s", response.status_code)
