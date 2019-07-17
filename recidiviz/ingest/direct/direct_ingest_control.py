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

"""Requests handlers for direct ingest control requests.
"""

from http import HTTPStatus

from flask import Blueprint, request

from recidiviz.utils import environment, regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_value

direct_ingest_control = Blueprint('direct_ingest_control', __name__)


@direct_ingest_control.route('/start')
@authenticate_request
def start():
    gae_env = environment.get_gae_environment()
    region_value = get_value('region', request.args)

    try:
        region = regions.get_region(region_value, is_direct_ingest=True)
    except FileNotFoundError:
        return (f"Unsupported direct ingest region {region_value}",
                HTTPStatus.BAD_REQUEST)

    controller = region.get_ingestor()

    if region.environment != gae_env:
        return (
            f"Bad environment {gae_env} for direct region {region_value}.",
            HTTPStatus.BAD_REQUEST)

    # TODO (#2099) enqueue a task to run this region.
    controller.go()

    return '', HTTPStatus.OK
