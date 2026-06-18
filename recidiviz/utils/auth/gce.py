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
from typing import Any, Callable

from flask import g, request

from recidiviz.utils import metadata, validate_jwt
from recidiviz.utils.environment import in_development
from recidiviz.utils.secrets import get_secret


def get_backend_service_id_from_secret(backend_service_id_secret_name: str) -> str:
    """Return the IAP backend service id stored in the named secret.

    Raises a RuntimeError if the secret is missing or empty.
    """
    backend_service_id = get_secret(backend_service_id_secret_name)
    if not backend_service_id:
        raise RuntimeError(
            f"Missing backend service id secret named [{backend_service_id_secret_name}]"
        )
    return backend_service_id


def authenticate_compute_engine(
    backend_service_id: str,
) -> tuple[str | None, tuple[str, HTTPStatus] | None]:
    """Validates the JWT in the X-Goog-IAP-JWT-Assertion header.

    Returns (caller_email, error_response). Exactly one of the two is None:
    on success the caller's email is returned with no error response; on
    failure a flask-compatible (body, status) tuple is returned and the
    email is None.
    """
    jwt = request.headers.get("x-goog-iap-jwt-assertion")
    project_number = metadata.project_number()

    if not project_number:
        raise RuntimeError("Expected project_number to be set")

    if not jwt:
        return None, (
            "Failed: Unauthorized external request.",
            HTTPStatus.UNAUTHORIZED,
        )

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
        return None, (f"Error: {error_str}", HTTPStatus.UNAUTHORIZED)

    return user_email, None


def get_compute_engine_caller_email(
    backend_service_id_secret_name: str,
) -> tuple[str | None, tuple[str, HTTPStatus] | None]:
    """Validate the IAP JWT against the given service, and return the request caller's
    email.

    Returns (caller_email, error_response): on success the caller's email with no error
    response; on failure a flask-compatible (body, status) tuple and no email.
    """
    backend_service_id = get_backend_service_id_from_secret(
        backend_service_id_secret_name
    )
    return authenticate_compute_engine(backend_service_id)


def build_compute_engine_auth_decorator(
    backend_service_id_secret_name: str,
) -> Callable:
    """Builds a decorator that can be used for adding IAP auth to routes"""

    def requires_auth_decorator(func: Callable) -> Callable:
        @wraps(func)
        def auth_and_call(*args: Any, **kwargs: Any) -> Any:
            """Authenticates the inbound request and delegates. Sets g.caller_email on success.

            Args:
                *args: args to the function
                **kwargs: keyword args to the function

            Returns:
                The output of the function, if successfully authenticated.
                An error or redirect response, otherwise.
            """
            if in_development():
                # Individual services can set g.caller_email to their own dev service account
                caller_email = None
            else:
                caller_email, error_response = get_compute_engine_caller_email(
                    backend_service_id_secret_name
                )
                if error_response:
                    return error_response

            # If we made it this far, client is authorized - run the decorated func
            g.caller_email = caller_email
            return func(*args, **kwargs)

        return auth_and_call

    return requires_auth_decorator
