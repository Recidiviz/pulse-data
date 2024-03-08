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

import requests
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import IntegrityError, NoResultFound

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
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import ConfigurationStatus
from recidiviz.persistence.database.schema.outliers.schema import Configuration
from recidiviz.utils.environment import get_gcp_environment, in_gcp

outliers_blueprint = Blueprint("outliers", "outliers")


def _auth_header() -> Dict[str, str]:
    if not in_gcp():
        return {}

    _id_token = fetch_id_token(
        request=Request(),
        # The audience is the IAP's client id
        audience="688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.googleusercontent.com",
    )
    return {
        "Authorization": f"Bearer {_id_token}",
    }


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

            # Use the email in the request dict if provided, otherwise infer the email
            request_dict["updated_by"] = (
                request_dict["updated_by"]
                if request_dict.get("updated_by", None)
                else user_email.lower()
            )
            request_dict["updated_at"] = datetime.now()
            request_dict["status"] = ConfigurationStatus.ACTIVE.value

            state_code = StateCode(state_code_str.upper())
            querier = OutliersQuerier(state_code)
            config = querier.add_configuration(request_dict)
        except IntegrityError as e:
            logging.error("Error adding configuration: %s", e)
            abort(HTTPStatus.INTERNAL_SERVER_ERROR, message=f"{e}")

        return config


@outliers_blueprint.route("<state_code_str>/configurations/<int:config_id>/deactivate")
class DeactivateConfigurationByIdAPI(MethodView):
    """CRUD endpoints for /admin/outliers/<state_code_str>/configurations/<config_id>/deactivate"""

    @outliers_blueprint.response(HTTPStatus.OK)
    def put(self, state_code_str: str, config_id: int) -> str:
        if state_code_str not in get_outliers_enabled_states():
            raise ValueError(f"Invalid state: {state_code_str}")

        try:
            state_code = StateCode(state_code_str.upper())
            OutliersQuerier(state_code).deactivate_configuration(config_id)
        except NoResultFound:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"Configuration with id {str(config_id)} does not exist",
            )
        except Exception as e:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=str(e),
            )

        return f"Configuration {str(config_id)} has been deactivated"


@outliers_blueprint.route("<state_code_str>/configurations/<int:config_id>/promote")
class PromoteConfigurationsAPI(MethodView):
    """CRUD endpoints for /admin/outliers/<state_code_str>/configurations/<config_id>/promote"""

    @outliers_blueprint.response(HTTPStatus.OK)
    def post(self, state_code_str: str, config_id: int) -> str:
        """
        Promotes the staging Configuration with id: config_id to production by making
        a request to the POST /configurations endpoint in production.
        """
        if get_gcp_environment() == "production":
            abort(
                HTTPStatus.BAD_REQUEST,
                message="This endpoint should not be called from production",
            )

        state_code = StateCode(state_code_str.upper())
        querier = OutliersQuerier(state_code)
        config = querier.get_configuration(config_id)

        if config.status != ConfigurationStatus.ACTIVE.value:
            abort(
                HTTPStatus.BAD_REQUEST,
                message="Must promote an active configuration",
            )

        config_dict = config.to_dict()

        # The updated_by of the promoted config should be the email of the user,
        # and if this wasn't specified here, we'd get the staging CR service account
        user_email, error_str = get_authenticated_user_email()
        if error_str:
            logging.error("Error determining logged-in user: %s", error_str)
        config_dict["updated_by"] = user_email

        # Remove these fields because the expected request body should follow the ConfigurationSchema type
        config_dict.pop("id")
        config_dict.pop("updated_at")
        config_dict.pop("status")

        # Make request
        response = requests.post(
            f"https://admin-panel-prod.recidiviz.org/admin/outliers/{state_code_str.upper()}/configurations",
            headers=_auth_header(),
            json=convert_nested_dictionary_keys(
                config_dict,
                snake_to_camel,
            ),
            timeout=60,
        )

        response.raise_for_status()
        return f"Configuration {str(config_id)} successfully promoted to production"
