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
"""Tools for handling authentication of requests."""
import logging
from functools import wraps
from http import HTTPStatus
from typing import Any, Callable

from flask import request

from recidiviz.utils import metadata, validate_jwt
from recidiviz.utils.environment import in_development


def requires_gae_auth(func: Callable) -> Callable:
    """Decorator to validate inbound request is authorized for Recidiviz

    Decorator function that checks for end-user authentication or that the
    request came from our app prior to calling the function it's decorating.

    Args:
        func: Function being decorated and its args

    Returns:
        If authenticated, results from the decorated function.
        If not, redirects to user login page
    """

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
            # Bypass GAE auth check in development.
            return func(*args, **kwargs)

        is_cron = request.headers.get("X-Appengine-Cron")
        is_task = request.headers.get("X-AppEngine-QueueName")
        incoming_app_id = request.headers.get("X-Appengine-Inbound-Appid")
        jwt = request.headers.get("x-goog-iap-jwt-assertion")

        project_id = metadata.project_id()
        project_number = metadata.project_number()

        if is_cron:
            logging.info("Requester is one of our cron jobs, proceeding.")

        elif is_task:
            logging.info("Requester is the taskqueue, proceeding.")

        elif incoming_app_id:
            # Check whether this is an intra-app call from our GAE service
            logging.info("Requester authenticated as app-id: [%s].", incoming_app_id)

            if incoming_app_id == project_id:
                logging.info("Authenticated intra-app call, proceeding.")
            else:
                logging.info("App ID is [%s], not allowed - exiting.", incoming_app_id)
                return (
                    "Failed: Unauthorized external request.",
                    HTTPStatus.UNAUTHORIZED,
                )
        elif jwt:
            (
                user_id,
                user_email,
                error_str,
            ) = validate_jwt.validate_iap_jwt_from_app_engine(
                jwt, project_number, project_id
            )
            logging.info("Requester authenticated as [%s] ([%s]).", user_id, user_email)
            if error_str:
                logging.info("Error validating user credentials: [%s].", error_str)
                return (f"Error: {error_str}", HTTPStatus.UNAUTHORIZED)
        else:
            return ("Failed: Unauthorized external request.", HTTPStatus.UNAUTHORIZED)

        # If we made it this far, client is authorized - run the decorated func
        return func(*args, **kwargs)

    return auth_and_call
