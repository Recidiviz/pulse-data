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
from typing import Any, Dict, List

from flask import g, session
from flask_sqlalchemy_session import current_session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.control_panel.constants import ControlPanelPermission
from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
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
    auth_error = JusticeCountsServerError(
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


def raise_if_user_is_not_in_agency(agency_id: int, agency_ids: List[int]) -> None:
    """Use this helper in API routes for which the user making the request
    must have access to the agency indicated in the `agency_id` parameter (or the
    agency connected to the report indicated by the `report_id` parameter), or else
    the request should fail. A user has access to an agency if that agency's name
    is in the user's app_metadata block (which is editable in the Auth0 UI.
    """
    if agency_id is not None and int(agency_id) not in agency_ids:
        raise JusticeCountsServerError(
            code="justice_counts_agency_permission",
            description=(
                "User does not have permission to access "
                f"reports from agency {agency_id}."
            ),
        )


def raise_if_user_is_not_agency_admin_or_recidiviz_admin() -> None:
    """Use this helper in API routes for when the user making the request must have
    either the RECIDIVIZ_ADMIN or AGENCY_ADMIN auth0 permission
    """
    permissions = g.user_context.permissions
    if not permissions or (
        ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
        and ControlPanelPermission.AGENCY_ADMIN.value not in permissions
    ):
        raise JusticeCountsServerError(
            code="justice_counts_admin_permission",
            description=(
                "User is missing recidiviz admin or agency admin permissions."
            ),
        )


def raise_if_user_is_not_recidiviz_admin(auth0_user_id: str) -> None:
    """Use this helper in API routes for when the user making the request must have
    the RECIDIVIZ_ADMIN auth0 permission
    """
    user_account = UserAccountInterface.get_user_by_auth0_user_id(
        current_session,
        auth0_user_id=auth0_user_id,
    )
    permissions = g.user_context.permissions

    if not permissions or (
        ControlPanelPermission.RECIDIVIZ_ADMIN.value not in permissions
    ):
        raise JusticeCountsServerError(
            code="justice_counts_admin_permission",
            description=(
                f"User {user_account.id} is missing recidiviz admin permission."
            ),
        )


def get_auth0_user_id(request_dict: Dict[str, Any]) -> str:
    """If we are not in development, we do not allow passing in `auth0_user_id` to a request.
    Doing so would allow users to pretend to be other users. Instead, we infer the `auth0_user_id`
    from the Authorization header and store it on the global user context in our authorization
    callback. If we are in development, we do allow passing in `auth0_user_id` for testing purposes.
    """
    if "user_context" in g and g.user_context.auth0_user_id is not None:
        return g.user_context.auth0_user_id

    if not in_development():
        raise ValueError("No UserContext was found on the session.")

    auth0_user_id = request_dict.get("auth0_user_id")
    if auth0_user_id is None:
        raise ValueError("Missing required parameter auth0_user_id.")

    return auth0_user_id


def get_agency_ids_from_token(claims: TokenClaims) -> Any:
    """This method expects the user's `app_metadata` to be a dictionary with the key `agencies`
    and the value a list of agency ids. It also expects that the `app_metadata` claim has
    been added to the access token via a post-login Auth0 action.
    """
    app_metadata: Dict[str, Any] = claims.get(APP_METADATA_CLAIM, {})  # type: ignore[assignment]
    return app_metadata.get(AGENCY_IDS_KEY, [])


def get_agency_ids_from_session() -> List[int]:
    agency_ids = g.user_context.agency_ids if "user_context" in g else []
    permissions = g.user_context.permissions if "user_context" in g else []
    if ControlPanelPermission.RECIDIVIZ_ADMIN.value in permissions:
        # Admins "belong" to all agencies
        return AgencyInterface.get_agency_ids(session=current_session)
    return agency_ids


def get_agencies_from_session() -> List[schema.Agency]:
    agency_ids = g.user_context.agency_ids if "user_context" in g else []
    permissions = g.user_context.permissions if "user_context" in g else []
    if ControlPanelPermission.RECIDIVIZ_ADMIN.value in permissions:
        # Admins "belong" to all agencies
        return AgencyInterface.get_agencies(session=current_session)
    return AgencyInterface.get_agencies_by_id(
        session=current_session, agency_ids=agency_ids
    )
