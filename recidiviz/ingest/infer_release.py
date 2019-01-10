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

"""Exposes API to infer release of people."""

from http import HTTPStatus
import logging

from flask import Blueprint

from recidiviz.ingest import sessions
from recidiviz.utils.auth import authenticate_request
from recidiviz.persistence import persistence
from recidiviz.utils.regions import get_supported_regions

infer_release_blueprint = Blueprint('infer_release', __name__)


@infer_release_blueprint.route('/release')
@authenticate_request
def infer_release():
    regions = _get_jail_regions()

    if not regions:
        logging.error("No valid regions found in request")
        return 'No valid regions found in request', HTTPStatus.BAD_REQUEST

    for region in regions:
        session = sessions.get_most_recent_completed_session(region)
        if session:
            logging.info(
                'Got most recent completed session for %s with start time %s',
                region, session.start)
            persistence.infer_release_on_open_bookings(region, session.start)
    return '', HTTPStatus.OK


def _get_jail_regions():
    return [region_code for region_code, region_dict in
            get_supported_regions(full_manifest=True).items() if
            region_dict['agency_type'] == 'jail']
