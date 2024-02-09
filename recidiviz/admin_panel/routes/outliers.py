# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Endpoints related to Outliers on the admin panel."""

import logging
from datetime import datetime
from http import HTTPStatus
from typing import Any, Dict, List

from flask.views import MethodView
from flask_smorest import Blueprint, abort
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import IntegrityError

from recidiviz.admin_panel.admin_stores import fetch_state_codes
from recidiviz.admin_panel.line_staff_tools.outliers_api_schemas import (
    ConfigurationSchema,
    FullConfigurationSchema,
    StateCodeSchema,
)
from recidiviz.auth.helpers import get_authenticated_user_email
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import ConfigurationStatus
from recidiviz.persistence.database.schema.outliers.schema import Configuration

outliers_blueprint = Blueprint("outliers", "outliers")


@outliers_blueprint.route("enabled_state_codes")
class EnabledStatesAPI(MethodView):
    @outliers_blueprint.response(HTTPStatus.OK, StateCodeSchema(many=True))
    def get(self) -> List[dict]:
        states = fetch_state_codes(
            [
                StateCode[state_code_str]
                for state_code_str in get_outliers_enabled_states()
                if StateCode.is_state_code(state_code_str)
            ]
        )
        return states


@outliers_blueprint.route("<state_code_str>/configurations")
class ConfigurationsAPI(MethodView):
    """CRUD endpoints for /admin/outliers/<state_code_str>/configurations"""

    @outliers_blueprint.response(HTTPStatus.OK, FullConfigurationSchema(many=True))
    def get(
        self,
        state_code_str: str,
    ) -> List[Row]:
        if state_code_str not in get_outliers_enabled_states():
            raise ValueError(
                f"Cannot retrieve Configuration objects for invalid state: {state_code_str}"
            )

        state_code = StateCode(state_code_str.upper())
        configurations = OutliersQuerier(state_code).get_configurations()
        return configurations

    @outliers_blueprint.arguments(
        ConfigurationSchema,
        error_status_code=HTTPStatus.BAD_REQUEST,
    )
    @outliers_blueprint.response(HTTPStatus.OK, FullConfigurationSchema)
    def post(self, request_dict: Dict[str, Any], state_code_str: str) -> Configuration:
        """
        Adds a new active config to Configuration DB table, deactivates any active
        configs that have the same feature variant (if they exist),
        and returns the created config.
        """
        try:
            user_email, error_str = get_authenticated_user_email()
            if error_str:
                logging.error("Error determining logged-in user: %s", error_str)

            request_dict["updated_by"] = user_email.lower()
            request_dict["updated_at"] = datetime.now()
            request_dict["status"] = ConfigurationStatus.ACTIVE.value

            state_code = StateCode(state_code_str.upper())
            querier = OutliersQuerier(state_code)
            config = querier.add_configuration(request_dict)
        except IntegrityError as e:
            logging.error("Error adding configuration: %s", e)
            abort(HTTPStatus.INTERNAL_SERVER_ERROR, message=f"{e}")

        return config
