# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

from http import HTTPStatus
import logging
import os
import tempfile
from flask import Blueprint, request
import gcsfs
from recidiviz.utils import metadata

from recidiviz.ingest.aggregate.regions.ca import ca_aggregate_ingest
from recidiviz.ingest.aggregate.regions.fl import fl_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ga import ga_aggregate_ingest
from recidiviz.ingest.aggregate.regions.hi import hi_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ky import ky_aggregate_ingest
from recidiviz.ingest.aggregate.regions.ny import ny_aggregate_ingest
from recidiviz.ingest.aggregate.regions.tx import tx_aggregate_ingest
from recidiviz.persistence.database import database
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
        'texas': tx_aggregate_ingest.parse,
    }

    bucket = get_value('bucket', request.args)
    state = get_value('state', request.args)
    filename = get_value('filename', request.args)
    project_id = metadata.project_id()
    logging.info('The project id is %s', project_id)
    if not bucket or not state or not filename:
        raise StateAggregateError(
            'All of state, bucket, and filename must be provided')
    path = os.path.join(bucket, state, filename)
    parser = state_to_parser[state]
    fs = gcsfs.GCSFileSystem(project=project_id)

    # Providing a stream buffer to tabula reader does not work because it
    # tries to load the file into the local filesystem, since appengine is a
    # read only filesystem (except for the tmpdir) we download the file into
    # the local tmpdir and pass that in.
    tmpdir_path = os.path.join(tempfile.gettempdir(), filename)
    fs.get(path, tmpdir_path)
    logging.info('Successfully downloaded file from gcs: %s', path)

    result = parser(tmpdir_path)
    for table, df in result.items():
        database.write_df(table, df)

    # If we are successful, we want to move the file out of the cloud function
    # triggered directory, and into the historical path.
    historical_path = os.path.join(
        HISTORICAL_BUCKET.format(project_id),
        state, filename
    )
    fs.mv(path, historical_path)

    return '', HTTPStatus.OK
