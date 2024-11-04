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
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import IntegrityError, NoResultFound

from recidiviz.admin_panel.admin_stores import fetch_state_codes
from recidiviz.admin_panel.line_staff_tools.outliers_api_schemas import (
    ConfigurationSchema,
    FullConfigurationSchema,
    StateCodeSchema,
)
from recidiviz.admin_panel.utils import auth_header_for_request_to_prod
from recidiviz.auth.helpers import get_authenticated_user_email
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states,
)
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.types import ConfigurationStatus
from recidiviz.persistence.database.schema.insights.schema import Configuration
from recidiviz.utils.environment import get_gcp_environment

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


@outliers_blueprint.route(
    "<state_code_str>/configurations/<int:config_id>/promote/production"
)
class PromoteToProdConfigurationsAPI(MethodView):
    """CRUD endpoints for /admin/outliers/<state_code_str>/configurations/<config_id>/promote/production"""

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
        config_dict.pop("duplicate_write", None)

        camel_config = convert_nested_dictionary_keys(
            config_dict,
            snake_to_camel,
        )
        # The FE is expecting snake case keys in the actionStrategyCopy
        # object so we will revert the snake_to_camel for that attr
        camel_config["actionStrategyCopy"] = config_dict["action_strategy_copy"]

        logging.info(
            "Making request to /configurations API in production for [%s]",
            str(camel_config),
        )

        # Make request
        response = requests.post(
            f"https://admin-panel-prod.recidiviz.org/admin/outliers/{state_code_str.upper()}/configurations",
            headers=auth_header_for_request_to_prod(),
            json=camel_config,
            timeout=60,
        )

        response.raise_for_status()
        return f"Configuration {str(config_id)} successfully promoted to production"


def _get_refreshed_active_configuration_metadata(
    config: Configuration,
    new_feature_variant: str | None,
) -> Dict[str, Any]:
    """
    Returns a dictionary for the Configuration object with updated metadata based
    on the provided configuration object and an ACTIVE status.
    """
    config_dict = config.to_dict()

    user_email, error_str = get_authenticated_user_email()
    if error_str:
        logging.error("Error determining logged-in user: %s", error_str)

    # The id is autoincremented upon insert
    config_dict.pop("id")

    # Update the dictionary to have up-to-date information
    config_dict["updated_by"] = user_email.lower()
    config_dict["updated_at"] = datetime.now()
    config_dict["status"] = ConfigurationStatus.ACTIVE.value
    config_dict["feature_variant"] = new_feature_variant

    return config_dict


@outliers_blueprint.route(
    "<state_code_str>/configurations/<int:config_id>/promote/default"
)
class PromoteToDefaultConfigurationsAPI(MethodView):
    """CRUD endpoints for /admin/outliers/<state_code_str>/configurations/<config_id>/promote/default"""

    @outliers_blueprint.response(HTTPStatus.OK)
    def post(self, state_code_str: str, config_id: int) -> str:
        """
        Promotes the Configuration with id: config_id to the default configuration
        by adding a new DB entity for an active configuration with `feature_variant=None`
        """
        state_code = StateCode(state_code_str.upper())
        querier = OutliersQuerier(state_code)
        config = querier.get_configuration(config_id)

        if config.feature_variant is None:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"Configuration {config.id} is already a default configuration, status is {config.status}",
            )

        config_dict = _get_refreshed_active_configuration_metadata(
            config=config, new_feature_variant=None
        )

        try:
            config = querier.add_configuration(config_dict)
        except IntegrityError as e:
            logging.error("Error adding configuration: %s", e)
            abort(HTTPStatus.INTERNAL_SERVER_ERROR, message=f"{e}")

        return f"Configuration {str(config_id)} successfully promoted to the default configuration"


@outliers_blueprint.route("<state_code_str>/configurations/<int:config_id>/reactivate")
class ReactivateConfigurationsAPI(MethodView):
    """CRUD endpoints for /admin/outliers/<state_code_str>/configurations/<config_id>/reactivate"""

    @outliers_blueprint.response(HTTPStatus.OK)
    def post(self, state_code_str: str, config_id: int) -> str:
        """
        Reactivates an inactive configuration
        """
        state_code = StateCode(state_code_str.upper())
        querier = OutliersQuerier(state_code)
        config = querier.get_configuration(config_id)

        if config.status == ConfigurationStatus.ACTIVE.value:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"Configuration {config.id} is already active",
            )

        config_dict = _get_refreshed_active_configuration_metadata(
            config=config,
            new_feature_variant=config.feature_variant,
        )

        try:
            config = querier.add_configuration(config_dict)
        except IntegrityError as e:
            logging.error("Error adding configuration: %s", e)
            abort(HTTPStatus.INTERNAL_SERVER_ERROR, message=f"{e}")

        return f"Configuration {str(config_id)} successfully reactivated"
