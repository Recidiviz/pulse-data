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

from cloud_function_utils import make_iap_request, \
    get_direct_ingest_storage_bucket, \
    get_state_region_code_from_direct_ingest_bucket, \
    DirectIngestRegionCategory, get_dashboard_data_export_storage_bucket

_STATE_AGGREGATE_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/cloud_function/state_aggregate?bucket={}&state={}'
    '&filename={}')
_DIRECT_INGEST_CLOUD_FUNCTION_URL = (
    'http://{}.appspot.com/cloud_function/direct?bucket={}&region={}'
    '&file_path={}&storage_bucket={}')
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

    _call_direct_ingest(bucket, region_code, relative_file_path,
                        DirectIngestRegionCategory.COUNTY)


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

    _call_direct_ingest(bucket, region_code, relative_file_path,
                        DirectIngestRegionCategory.STATE)


def _call_direct_ingest(bucket: str,
                        region_code: str,
                        relative_file_path: str,
                        region_category: DirectIngestRegionCategory):
    """Calls direct ingest cloud function when a new file is dropped into a
    bucket."""
    project_id = os.environ.get('GCP_PROJECT')
    if not project_id:
        logging.error('No project id set for call to direct ingest cloud '
                      'function, returning.')
        return

    file_path = os.path.join(bucket, relative_file_path)

    # TODO(1628): Immediately rename to have the format
    #  [ISO TIMESTAMP]_[FILE NAME](_[SEQ_NUM])?.[EXT] where sequence number is
    #  optionally added if there is already another file name present on the
    #  same day. This can use some of the same logic that is in the
    #  GcsfsController for avoiding conflicts when writing to storage.
    #  The logic for extracting the file_tag will also need to change.

    storage_bucket = \
        get_direct_ingest_storage_bucket(region_category, project_id)
    logging.info(
        "Running cloud function for bucket %s, region %s, file_path %s",
        bucket, region_code, file_path)
    url = _DIRECT_INGEST_CLOUD_FUNCTION_URL.format(
        project_id, bucket, region_code, file_path, storage_bucket)

    logging.info("Calling URL: %s", url)

    # Hit the cloud function backend, which parses the data and persists to our
    # database.
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
