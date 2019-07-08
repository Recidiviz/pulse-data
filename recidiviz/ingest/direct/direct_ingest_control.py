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

from flask import Blueprint

from recidiviz.ingest.direct.regions.us_ma_middlesex.\
    us_ma_middlesex_controller import UsMaMiddlesexController
from recidiviz.utils import environment
from recidiviz.utils.auth import authenticate_request

direct_ingest_control = Blueprint('direct_ingest_control', __name__)


@direct_ingest_control.route('/us_ma_middlesex')
@authenticate_request
def us_ma_middlesex():
    gae_env = environment.get_gae_environment()
    controller = UsMaMiddlesexController()
    if controller.region.environment != gae_env:
        return (f"Skipping Middlesex direct ingest in environment {gae_env}",
                HTTPStatus.OK)

    # Iterate through export dates and write data one export at a time. Raises
    # an exception the first time we fail to ingest an export.
    controller.go()

    return '', HTTPStatus.OK
