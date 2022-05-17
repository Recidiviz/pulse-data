# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for Justice Counts Control Panel"""
import logging
from functools import wraps
from typing import Any, Callable, Dict, List

from flask import g, request, session
from flask_sqlalchemy_session import current_session

from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.exceptions import (
    JusticeCountsAuthorizationError,
    JusticeCountsPermissionError,
)
from recidiviz.justice_counts.report import ReportInterface
from recidiviz.utils.auth.auth0 import (
    AuthorizationError,
    TokenClaims,
    get_permissions_from_token,
    update_session_with_user_info,
)
from recidiviz.utils.environment import in_development

APP_METADATA_CLAIM = "https://dashboard.recidiviz.org/app_metadata"
AGENCY_IDS_KEY = "agency_ids"


def on_successful_authorization(jwt_claims: TokenClaims) -> None:
    auth_error = JusticeCountsAuthorizationError(
        code="no_justice_counts_access",
        description="You are not authorized to access this application",
    )

    try:
        update_session_with_user_info(session, jwt_claims, auth_error)
    except AuthorizationError as e:
        # When using M2M authentication during development testing, our access token won't have
        # a custom email token claim (because the token doesn't actually belong to a user),
        # but this is okay and shouldn't raise an error.
        # Details: https://auth0.com/docs/get-started/authentication-and-authorization-flow/client-credentials-flow
        if in_development() and e.code == "invalid_claims":
            logging.info(
                "Token claims is missing email address; "
                "assuming this is an M2M client-credentials grant."
            )
            session["user_info"] = {}
            return
        raise e

    g.user_context = UserContext(
        auth0_user_id=session["jwt_sub"],
        agency_ids=get_agency_ids_from_token(jwt_claims),
        permissions=get_permissions_from_token(jwt_claims) or [],
    )


def raise_if_user_is_unauthorized(route: Callable) -> Callable:
    """Use this decorator to wrap API routes for which the user making the request
    must have access to the agency indicated in the `agency_id` parameter (or the
    agency connected to the report indicated by the `report_id` parameter), or else
    the request should fail. A user has access to an agency if that agency's name
    is in the user's app_metadata block (which is editable in the Auth0 UI.
    """

    @wraps(route)
    def decorated(*args: List[Any], **kwargs: Dict[str, Any]) -> Callable:
        if "user_context" not in g or g.user_context.user_account is None:
            raise JusticeCountsPermissionError(
                code="justice_counts_agency_permission",
                description="No UserContext found on session.",
            )

        user_id = g.user_context.user_account.id

        request_dict: Dict[str, Any] = {}
        if request.method == "GET":
            request_dict = request.values
        elif request.method == "POST":
            request_dict = request.json or {}
        else:
            raise ValueError(f"Unsupported request method: {request.method}")

        agency_id = request_dict.get("agency_id")

        # If no agency is found, check if there is a `report_id` route parameter
        if not agency_id and "report_id" in kwargs:
            report_id: int = kwargs["report_id"]  # type: ignore[assignment]
            report = ReportInterface.get_report_by_id(
                session=current_session, report_id=report_id
            )
            agency_id = report.source_id

        permission_error = JusticeCountsPermissionError(
            code="justice_counts_agency_permission",
            description=f"User {user_id} does not have access to reports from agency {agency_id}.",
        )

        if agency_id is None:
            raise permission_error

        # List of agency ids in user's app_metadata will have been copied over
        # to the `g.user_context` object on successful authorization
        if int(agency_id) not in g.user_context.agency_ids:
            raise permission_error

        return route(*args, **kwargs)

    return decorated


def get_user_account_id(request_dict: Dict[str, Any]) -> int:
    """If we are not in development, we do not allow passing in `user_id` to a request.
    Doing so would allow users to pretend to be other users. Instead, we infer the `user_id`
    from the Authorization header and store it on the global user context in our authorization
    callback. If we are in development, we do allow passing in `user_id` for testing purposes.
    """
    if "user_context" in g and g.user_context.user_account is not None:
        return g.user_context.user_account.id

    if not in_development():
        raise ValueError(
            "Either no UserContext was found on the session, "
            "or no UserAccount was stored on the UserContext."
        )

    user_id = request_dict.get("user_id")
    if user_id is None:
        raise ValueError("Missing required parameter user_id.")

    return user_id


def get_agency_ids_from_token(claims: TokenClaims) -> Any:
    """This method expects the user's `app_metadata` to be a dictionary with the key `agencies`
    and the value a list of agency ids. It also expects that the `app_metadata` claim has
    been added to the access token via a post-login Auth0 action.
    """
    app_metadata: Dict[str, Any] = claims.get(APP_METADATA_CLAIM, {})  # type: ignore[assignment]
    return app_metadata.get(AGENCY_IDS_KEY, [])
