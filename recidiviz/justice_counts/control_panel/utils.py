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
from typing import Any, Dict

from flask import g, session

from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.exceptions import JusticeCountsAuthorizationError
from recidiviz.utils.auth.auth0 import (
    AuthorizationError,
    TokenClaims,
    get_permissions_from_token,
    update_session_with_user_info,
)
from recidiviz.utils.environment import in_development


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

    session["user_permissions"] = get_permissions_from_token(jwt_claims)
    g.user_context = UserContext(
        auth0_user_id=session["jwt_sub"], permissions=session["user_permissions"]
    )


def get_user_account_id(request_dict: Dict[str, Any]) -> int:
    """If we are not in development, we do not allow passing in `user_id` to a request.
    Doing so would allow users to pretend to be other users. Instead, we infer the `user_id`
    from the Authorization header and store it on the global user context in our authorization
    callback. If we are in development, we do allow passing in `user_id` for testing purposes.
    """
    if "user_context" in g:
        return g.user_context.user_account.id

    if not in_development():
        raise ValueError("No UserContext found.")

    user_id = request_dict.get("user_id")
    if user_id is None:
        raise ValueError("Missing required parameter user_id.")

    return user_id
