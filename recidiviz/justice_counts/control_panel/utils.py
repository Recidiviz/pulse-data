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
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from flask import g, session
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from recidiviz.justice_counts.control_panel.user_context import UserContext
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.utils.auth.auth0 import (
    AuthorizationError,
    TokenClaims,
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

    g.user_context = UserContext(auth0_user_id=session["jwt_sub"])


def raise_if_user_is_not_in_agency(user: schema.UserAccount, agency_id: int) -> None:
    """Use this helper in API routes for which the user making the request
    must have access to the agency indicated in the `agency_id` parameter (or the
    agency connected to the report indicated by the `report_id` parameter), or else
    the request should fail. A user has access to an agency if there is a corresponding
    row in the AgencyUserAccountAssociation table.
    """
    if int(agency_id) not in [a.agency_id for a in user.agency_assocs]:
        raise JusticeCountsServerError(
            code="justice_counts_agency_permission",
            description=(
                f"User does not have permission to access agency {agency_id}."
            ),
        )


def raise_if_user_is_wrong_role(
    user: schema.UserAccount, agency_id: int, allowed_roles: Set[schema.UserAccountRole]
) -> None:
    """Use this helper in API routes for when the user making the request must have
    one of the given `allowed_roles`, or else the reuqest should fail. A user has
    a given role for an agency if it is specified in the corresponding row of the
    AgencyUserAccountAssociation table.
    """
    assocs = [a for a in user.agency_assocs if a.agency_id == int(agency_id)]
    if not assocs:
        raise JusticeCountsServerError(
            code="justice_counts_agency_permission",
            description=(
                f"User does not have permission to access agency {agency_id}."
            ),
        )
    assoc = assocs[0]
    if assoc.role not in allowed_roles:
        raise JusticeCountsServerError(
            code="justice_counts_admin_permission",
            description="User does not have required role.",
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


def write_data_to_spreadsheet(
    google_credentials: Credentials,
    spreadsheet_id: str,
    logger: logging.Logger,
    new_sheet_title: str,
    index: int,
    data_to_write: Optional[List[List[str]]] = None,
    df: Optional[pd.DataFrame] = None,
    columns: Optional[List[str]] = None,
    overwrite_sheets: Optional[bool] = False,
) -> None:
    """Writes data to the spreadsheet specified by the spreadsheet_id.

    If overwrite_sheets is True and the sheet already exists, we delete the old
    spreadsheet before creating a new one.
    """
    spreadsheet_service = build("sheets", "v4", credentials=google_credentials)

    # Create a new worksheet in the spreadsheet
    request = {"addSheet": {"properties": {"title": new_sheet_title, "index": index}}}

    # Delete existing sheet if we want to overwrite with new
    if overwrite_sheets is True:
        # Get sheet_id to delete
        sheets = (
            spreadsheet_service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id)
            .execute()["sheets"]
        )
        for sheet in sheets:
            if sheet["properties"]["title"] == new_sheet_title:
                sheet_id = sheet["properties"]["sheetId"]
                # Delete old sheet
                delete_request = {"deleteSheet": {"sheetId": sheet_id}}
                spreadsheet_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, body={"requests": [delete_request]}
                ).execute()
                logger.info("Old '%s' Sheet Deleted", new_sheet_title)
                break

    # Create new sheet
    spreadsheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": [request]}
    ).execute()

    logger.info("New Sheet Created")

    # Format data to fit Google API specifications.
    # Google API spec requires a list of lists,
    # each list representing a row.
    if columns is not None and df is not None:
        data_to_write = [columns]
        data_to_write.extend(df.astype(str).values.tolist())

    body = {"values": data_to_write}
    range_name = f"{new_sheet_title}!A1"
    spreadsheet_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body,
    ).execute()

    logger.info("Sheet '%s' added and data written.", new_sheet_title)


def append_row_to_spreadsheet(
    google_credentials: Credentials,
    spreadsheet_id: str,
    logger: logging.Logger,
    sheet_title: str,
    data_to_write: Optional[List[List[str]]] = None,
    sort_by: Optional[str] = None,
    sheet_id: Optional[int] = None,
) -> None:
    """Append data as new rows to an existing spreadsheet.
    If sort_by and sheet_id are both not None, we sort the sheet in descending order
    by the provided sort_by column after the new row is appended.
    """
    spreadsheet_service = build("sheets", "v4", credentials=google_credentials)

    # Append row to end of spreasheet
    spreadsheet_service.spreadsheets().values().append(
        body={"values": data_to_write},
        spreadsheetId=spreadsheet_id,
        range=sheet_title,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
    ).execute()
    logger.info("Data has been appended to sheet '%s'.", sheet_title)

    # Sort spreadsheet by sort_by column in descending order
    if sort_by is not None and sheet_id is not None:
        sort_request = {
            "sortRange": {
                "range": {
                    "sheetId": sheet_id,
                },
                "sortSpecs": [
                    {
                        "dataSourceColumnReference": {"name": sort_by},
                        "sortOrder": "DESCENDING",
                    },
                ],
            }
        }
        spreadsheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [sort_request]}
        ).execute()
        logger.info(
            "'%s' sheet has been sorted by the %s column.", sheet_title, sort_by
        )
