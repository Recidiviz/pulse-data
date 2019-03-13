# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Contains all HTTP endpoints for communicating with the persistence layer."""

from http import HTTPStatus

from flask import Blueprint

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import persistence
from recidiviz.utils import monitoring
from recidiviz.utils.auth import authenticate_request

actions = Blueprint('actions', __name__)


@actions.route("/v1/records", methods=['POST'])
@authenticate_request
def write_record():
    # TODO: Something like `ingest_info = protobuf.read(request.data)`
    ingest_info = None
    last_scraped_time = None
    region = None
    jurisdiction_id = None

    with monitoring.push_tags({monitoring.TagKey.REGION: region}):
        metadata = IngestMetadata(region, jurisdiction_id, last_scraped_time)

        persistence.write(ingest_info, metadata)

        return '', HTTPStatus.NOT_IMPLEMENTED
