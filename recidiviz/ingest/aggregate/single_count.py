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

"""Exposes an endpoint to specify aggregate population data."""

import logging
from http import HTTPStatus

from flask import Blueprint, request

from recidiviz.ingest.models.single_count import SingleCount
from recidiviz.persistence.single_count import store_single_count
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_value

store_single_count_blueprint = Blueprint("store_single_count", __name__)


class StoreSingleCountError(Exception):
    """Errors thrown in the store single count endpoint"""


@store_single_count_blueprint.route("/single_count")
@requires_gae_auth
def store_single_count_endpoint():
    """Endpoint to store a single count"""

    jid = get_str_param_value("jid", request.args)
    ethnicity = get_str_param_value("ethnicity", request.args)
    gender = get_str_param_value("gender", request.args)
    race = get_str_param_value("race", request.args)
    count = get_str_param_value("count", request.args)
    date = get_str_param_value("date", request.args)
    sc = SingleCount(
        count=count,
        ethnicity=ethnicity,
        gender=gender,
        race=race,
        date=date,
    )
    stored = store_single_count(sc, jid)

    if stored:
        logging.info(
            "Stored [%d] as [%s] for [%s]",
            count,
            " ".join(filter(None, (race, gender, ethnicity))),
            jid,
        )
        return "", HTTPStatus.OK

    logging.error("Failed to store single count for [%s]", jid)
    return "", HTTPStatus.INTERNAL_SERVER_ERROR
