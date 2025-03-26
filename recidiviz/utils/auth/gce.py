# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tools for handling authentication of IAP requests in Compute Engine."""
import logging
from functools import wraps
from http import HTTPStatus
from typing import Any, Callable, Optional, Tuple

from flask import request

from recidiviz.utils import metadata, validate_jwt
from recidiviz.utils.environment import in_development
from recidiviz.utils.secrets import get_secret


def authenticate_compute_engine(
    backend_service_id: str,
) -> Optional[Tuple[str, HTTPStatus]]:
    """Validates the JWT provided in the X-header"""
    jwt = request.headers.get("x-goog-iap-jwt-assertion")
    project_number = metadata.project_number()

    if not project_number:
        raise RuntimeError("Expected project_number to be set")

    if jwt:
        (
            user_id,
            user_email,
            error_str,
        ) = validate_jwt.validate_iap_jwt_from_compute_engine(
            jwt, project_number, backend_service_id
        )
        logging.info("Requester authenticated as [%s] ([%s]).", user_id, user_email)
        if error_str:
            logging.info("Error validating user credentials: [%s].", error_str)
            return f"Error: {error_str}", HTTPStatus.UNAUTHORIZED
    else:
        return "Failed: Unauthorized external request.", HTTPStatus.UNAUTHORIZED

    return None


def build_compute_engine_auth_decorator(
    backend_service_id_secret_name: str,
) -> Callable:
    """Builds a decorator that can be used for adding IAP auth to routes"""

    def requires_auth_decorator(func: Callable) -> Callable:
        @wraps(func)
        def auth_and_call(*args: Any, **kwargs: Any) -> Any:
            """Authenticates the inbound request and delegates.

            Args:
                *args: args to the function
                **kwargs: keyword args to the function

            Returns:
                The output of the function, if successfully authenticated.
                An error or redirect response, otherwise.
            """
            if in_development():
                # Bypass GCE auth check in development.
                return None

            backend_service_id = get_secret(backend_service_id_secret_name)
            if not backend_service_id:
                raise RuntimeError(
                    f"Missing backend service id secret named {backend_service_id_secret_name}"
                )

            if auth_result := authenticate_compute_engine(backend_service_id):
                return auth_result

            # If we made it this far, client is authorized - run the decorated func
            return func(*args, **kwargs)

        return auth_and_call

    return requires_auth_decorator
