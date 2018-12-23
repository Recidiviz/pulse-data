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

import httplib

from flask import Blueprint

from recidiviz.persistence import persistence
from recidiviz.utils.auth import authenticate_request

actions = Blueprint('actions', __name__)


@actions.route("/v1/records", methods=['POST'])
@authenticate_request
def write_record():
    # TODO: Something like `ingest_info = protobuf.read(request.data)`
    ingest_info = None
    last_scraped_time = None
    region = None
    persistence.write(ingest_info, region, last_scraped_time)

    return '', httplib.NOT_IMPLEMENTED
