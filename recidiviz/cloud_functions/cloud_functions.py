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

"""Exposes API to parse and store state aggregates."""
import datetime
import logging
import os
import tempfile
from http import HTTPStatus

from flask import Blueprint, request, jsonify
import gcsfs

from recidiviz.calculator.bq.dashboard import dashboard_export_manager

from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_ingest
from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_ingest
from recidiviz.ingest.aggregate.regions.hi import hi_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ky import ky_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_ingest
from recidiviz.ingest.aggregate.regions.pa import pa_aggregate_ingest
from recidiviz.ingest.aggregate.regions.tn import tn_aggregate_ingest
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_ingest
from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsIngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.utils import metadata
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_value

cloud_functions_blueprint = Blueprint('cloud_functions', __name__)


class StateAggregateError(Exception):
    """Errors thrown in the state aggregate endpoint"""


HISTORICAL_BUCKET = '{}-processed-state-aggregates'


@cloud_functions_blueprint.route('/state_aggregate')
@authenticate_request
def state_aggregate():
    """Calls state aggregates"""

    # Please add new states in alphabetical order
    state_to_parser = {
        'california': ca_aggregate_ingest.parse,
        'florida': fl_aggregate_ingest.parse,
        'georgia': ga_aggregate_ingest.parse,
        'hawaii': hi_aggregate_ingest.parse,
        'kentucky': ky_aggregate_ingest.parse,
        'new_york': ny_aggregate_ingest.parse,
        'pennsylvania': pa_aggregate_ingest.parse,
        'tennessee': tn_aggregate_ingest.parse,
        'texas': tx_aggregate_ingest.parse,
    }

    bucket = get_value('bucket', request.args)
    state = get_value('state', request.args)
    filename = get_value('filename', request.args)
    project_id = metadata.project_id()
    logging.info("The project id is %s", project_id)
    if not bucket or not state or not filename:
        raise StateAggregateError(
            "All of state, bucket, and filename must be provided")
    path = os.path.join(bucket, state, filename)
    parser = state_to_parser[state]
    # Don't use the gcsfs cache
    fs = gcsfs.GCSFileSystem(project=project_id, cache_timeout=-1)
    logging.info("The path to download from is %s", path)
    bucket_path = os.path.join(bucket, state)
    logging.info("The files in the directory are:")
    logging.info(fs.ls(bucket_path))

    # Providing a stream buffer to tabula reader does not work because it
    # tries to load the file into the local filesystem, since appengine is a
    # read only filesystem (except for the tmpdir) we download the file into
    # the local tmpdir and pass that in.
    tmpdir_path = os.path.join(tempfile.gettempdir(), filename)
    fs.get(path, tmpdir_path)
    logging.info("Successfully downloaded file from gcs: %s", path)

    try:
        result = parser(os.path.join(bucket, state), tmpdir_path)
        logging.info('Successfully parsed the report')
        for table, df in result.items():
            dao.write_df(table, df)

        # If we are successful, we want to move the file out of the cloud
        # function triggered directory, and into the historical path.
        historical_path = os.path.join(
            HISTORICAL_BUCKET.format(project_id),
            state, filename
        )
        fs.mv(path, historical_path)
        return '', HTTPStatus.OK
    except Exception as e:
        return jsonify(e), 500


@cloud_functions_blueprint.route('/direct')
@authenticate_request
def direct():
    """Calls direct ingest"""

    # The bucket that the file to ingest lives in
    bucket = get_value('bucket', request.args)
    # The region this data is associated with
    region_name = get_value('region', request.args)
    # The full path to the file to ingest, including the bucket name
    file_path = get_value('file_path', request.args)
    # The name of the bucket to move the file to once it has been processed
    storage_bucket = get_value('storage_bucket', request.args)

    project_id = metadata.project_id()
    logging.info("The project id is %s", project_id)
    if not bucket or not region_name or not file_path or not storage_bucket:
        raise DirectIngestError(
            msg="All of region, bucket, file_path, and storage_bucket must be "
                "provided.",
            error_type=DirectIngestErrorType.INPUT_ERROR,
        )

    # Don't use the gcsfs cache
    logging.info("The file_path to process is %s", file_path)
    logging.info("The storage_bucket is %s", storage_bucket)

    try:
        controller = direct_ingest_control.controller_for_region_code(
            region_name)
        controller.queue_ingest_job(GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path=file_path,
            storage_bucket=storage_bucket,
        ))
    except DirectIngestError as e:
        message = \
            f"Error while processing region [{region_name}] from file " \
            f"[{file_path}] on bucket [{bucket}] and storage_bucket " \
            f"[{storage_bucket}] and project [{project_id}]: [{str(e)}]"
        return message, HTTPStatus.INTERNAL_SERVER_ERROR

    return '', HTTPStatus.OK


@cloud_functions_blueprint.route('/dashboard_export')
@authenticate_request
def dashboard_export():
    """Calls the dashboard export manager.

    Endpoint path parameters:
        bucket: A string indicating the GCP cloud storage bucket to export to
        data_type: A string, either DATAFLOW or STANDARD for the type of data
             that should be exported
    """

    # The cloud storage bucket to export to
    bucket = get_value('bucket', request.args)

    # Get the type of data to export
    data_type = get_value('data_type', request.args)

    logging.info("Attempting to export dashboard %s data to cloud storage"
                 " bucket: %s.", data_type, bucket)

    dashboard_export_manager.export_dashboard_data_to_cloud_storage(bucket,
                                                                    data_type)

    return '', HTTPStatus.OK
