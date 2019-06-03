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

import logging
from http import HTTPStatus

from flask import Blueprint

from recidiviz.ingest.direct.errors import DirectIngestError
from recidiviz.ingest.direct.regions.us_ma_middlesex.us_ma_middlesex_collector \
    import UsMaMiddlesexCollector
from recidiviz.utils import environment, metadata
from recidiviz.utils.auth import authenticate_request

direct_ingest_control = Blueprint('direct_ingest_control', __name__)


@direct_ingest_control.route('/us_ma_middlesex')
@authenticate_request
def us_ma_middlesex():
    project_id = metadata.project_id()
    gae_env = environment.get_gae_environment()
    collector = UsMaMiddlesexCollector()
    if collector.region.environment != gae_env:
        return (f"Skipping Middlesex direct ingest in environment {gae_env}",
                HTTPStatus.OK)

    try:
        collector.go()
    except DirectIngestError:
        logging.exception("Direct ingest failed.")
        return ("Direct ingest data processing failed for region "
                "us_ma_middlesex in project {}.".format(project_id),
                HTTPStatus.INTERNAL_SERVER_ERROR)

    return '', HTTPStatus.OK
