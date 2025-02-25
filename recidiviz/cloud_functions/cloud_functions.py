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
import logging
import os
import tempfile
from http import HTTPStatus

from flask import Blueprint, request, jsonify
import gcsfs

from recidiviz.calculator.bq.dashboard import dashboard_export_manager
from recidiviz.calculator.utils import dataflow_monitor_manager
from recidiviz.cloud_functions.cloud_function_utils import GCSFS_NO_CACHING

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
from recidiviz.ingest.direct.errors import DirectIngestError
from recidiviz.persistence.database.schema.aggregate import dao
from recidiviz.utils import metadata
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_str_param_value

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

    bucket = get_str_param_value('bucket', request.args)
    state = get_str_param_value('state', request.args)
    filename = get_str_param_value('filename', request.args)
    project_id = metadata.project_id()
    logging.info("The project id is %s", project_id)
    if not bucket or not state or not filename:
        raise StateAggregateError(
            "All of state, bucket, and filename must be provided")
    path = os.path.join(bucket, state, filename)
    parser = state_to_parser[state]
    # Don't use the gcsfs cache
    fs = gcsfs.GCSFileSystem(project=project_id, cache_timeout=GCSFS_NO_CACHING)
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

# TODO(1628): Leave this until /direct/handle_direct_ingest_file has been fully
#  deployed and all cloud functions are hitting it in stage/prod
@cloud_functions_blueprint.route('/start_direct_ingest')
@authenticate_request
def start_direct_ingest():
    """Schedules direct ingest jobs for the given region, if necessary."""

    region_name = get_str_param_value('region', request.args)
    try:
        controller = \
            direct_ingest_control.controller_for_region_code(region_name)
        controller.kick_scheduler(just_finished_job=False)
    except DirectIngestError as e:
        project_id = metadata.project_id()
        message = \
            f"Error scheduling next ingest job for region [{region_name}] on " \
            f"project [{project_id}]: [{str(e)}]"
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
    bucket = get_str_param_value('bucket', request.args)

    logging.info("Attempting to export dashboard data to cloud storage"
                 " bucket: %s.", bucket)

    dashboard_export_manager.export_dashboard_data_to_cloud_storage(bucket)

    return '', HTTPStatus.OK


@cloud_functions_blueprint.route('/dataflow_monitor')
@authenticate_request
def dataflow_monitor():
    """Calls the dataflow monitor manager to begin monitoring a Dataflow job.

    Endpoint path parameters:
        job_id: The unique id of the job to monitor
        location: The region where the job is being run
        topic: The Pub/Sub topic to publish a message to if the job is
            successful
    """
    job_id = get_str_param_value('job_id', request.args)
    location = get_str_param_value('location', request.args)
    topic = get_str_param_value('topic', request.args)

    logging.info("Attempting to monitor the job with id: %s. Will "
                 "publish to %s on success.", job_id, topic)

    dataflow_monitor_manager.create_dataflow_monitor_task(job_id, location,
                                                          topic)

    return '', HTTPStatus.OK
