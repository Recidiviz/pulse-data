# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Job that writes to the Justice Counts User Permissions google spreadsheet with data 
about users with incorrect permissions


pipenv run python -m recidiviz.justice_counts.jobs.user_permissions_check \
  --credentials-path=<path>
"""
import argparse
import logging
from collections import defaultdict
from typing import Any, List, Optional

import sentry_sdk
from google.oauth2.service_account import Credentials
from oauth2client.client import GoogleCredentials
from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.control_panel.utils import (
    format_spreadsheet_rows,
    write_data_to_spreadsheet,
)
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override

# Spreadsheet Name: Justice Counts User Permissions
# https://docs.google.com/spreadsheets/d/1wqXzoh7GHj2cNAzXVoQNlwwc-J6ptnUBS-cRTKMyi2o/edit#gid=0
PERMISSION_SPREADSHEET_ID = "1wqXzoh7GHj2cNAzXVoQNlwwc-J6ptnUBS-cRTKMyi2o"
COLUMN_HEADERS = [
    "User Name",
    "Email",
    "User Account ID",
    "Agencies",
    "Role",
]

SCHEMA_TYPE = schema_type = SchemaType.JUSTICE_COUNTS
logger = logging.getLogger(__name__)

sentry_sdk.init(
    dsn=JUSTICE_COUNTS_SENTRY_DSN,
    # Enable performance monitoring
    enable_tracing=True,
)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    parser.add_argument(
        "--project-id",
        choices=[
            GCP_PROJECT_JUSTICE_COUNTS_STAGING,
            GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
        ],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    return parser


def write_and_format_sheet(
    rows: List[List[str]],
    index: int,
    google_credentials: Any,
    sheet_title: str,
    num_rows_to_bold: Optional[int] = 3,
    cell_pixel_size: Optional[int] = 160,
) -> None:
    """
    Writes to the Justice Counts User Permissions sheet with a write request and
    A request to format the sheets with bold lettering and making the cells larger and
    more readable.
    """

    sheet_id = write_data_to_spreadsheet(
        new_sheet_title=sheet_title,
        spreadsheet_id=PERMISSION_SPREADSHEET_ID,
        google_credentials=google_credentials,
        data_to_write=rows,
        logger=logger,
        overwrite_sheets=True,
        index=index,
    )

    format_requests = [
        {  # First request bolds the first three rows
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,  # Start row
                    "endRowIndex": num_rows_to_bold,  # End row
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True},
                    },
                },
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {  # Second request makes the cells a bit larger, this is for readability.
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 15,
                },
                "properties": {"pixelSize": cell_pixel_size},
                "fields": "pixelSize",
            }
        },
    ]

    format_spreadsheet_rows(
        spreadsheet_id=PERMISSION_SPREADSHEET_ID,
        google_credentials=google_credentials,
        sheet_title=sheet_title,
        logger=logger,
        format_requests=format_requests,
    )


def write_non_csg_recidiviz_production_sheet(
    session: Session, google_credentials: Any
) -> None:
    """
    Writes to the `[PRODUCTION] Non-CSG/Recidiviz User Access` sheet with information
    about access/permissions for non-CSG and non-Recidiviz users in production.
    """
    users = UserAccountInterface.get_non_csg_and_recidiviz_users(session=session)
    rows: List[List[str]] = [
        [
            """
            Key
            
            Multiple States: User has access to agencies across multiple states.

            No Child Agency: User has access to a superagency but no child agencies. 

            No Superagency, > 1 Agency: User does not have access to a superagency but has access to 2 or more individual agencies.
            
            > 10 agencies: User has access to > 10 agencies

            """
        ],
        [],
        [
            "User Name",
            "User Email",
            "User Id",
            "Agencies",
            "Multiple States",
            "No Child Agency",
            "No Superagency, > 1 Agency",
            "> 10 Agencies",
        ],
    ]
    user_to_states = defaultdict(set)
    user_to_super_agencies = defaultdict(list)
    user_to_individual_agencies = defaultdict(list)
    user_to_child_agencies = defaultdict(list)
    user_to_agency_names = defaultdict(list)
    for user in users:
        for agency_assoc in user.agency_assocs:
            user_to_states[user].add(agency_assoc.agency.state_code)
            user_to_agency_names[user].append(agency_assoc.agency.name)
            if agency_assoc.agency.is_superagency is True:
                user_to_super_agencies[user].append(agency_assoc.agency.name)
            elif agency_assoc.agency.super_agency_id is not None:
                user_to_child_agencies[user].append(agency_assoc.agency)
            else:
                user_to_individual_agencies[user].append(agency_assoc.agency)

    for user in users:
        add_user_to_table = False

        row = []
        user_data = [
            user.name,
            user.email,
            user.id,
            ", ".join(user_to_agency_names[user]),
        ]

        # Multiple States Column
        if len(user_to_states[user]) > 1:
            row.append("Yes")
            add_user_to_table = True
        else:
            row.append("No")

        # No Child Agency Column
        if (
            len(user_to_super_agencies[user]) >= 1
            and len(user_to_child_agencies[user]) == 0
        ):
            row.append("Yes")
            add_user_to_table = True
        else:
            row.append("No")

        # No Superagency, > 1 Agency Column
        if (
            len(user_to_super_agencies[user]) == 0
            and len(user_to_individual_agencies[user]) >= 2
        ):
            row.append("Yes")
            add_user_to_table = True
        else:
            row.append("No")

        # > 10 Agencies Column
        if (
            len(user_to_super_agencies[user]) + len(user_to_individual_agencies[user])
            > 10
        ):
            row.append("Yes")
            add_user_to_table = True
        else:
            row.append("No")

        if add_user_to_table is True:
            # Only add user to the table if they are an anomaly,
            # in other words, they have `Yes` in one of the columns.
            row = user_data + row
            rows.append(row)

    write_and_format_sheet(
        rows=rows,
        index=5,
        google_credentials=google_credentials,
        cell_pixel_size=200,
        sheet_title="[PRODUCTION] Non-CSG/Recidiviz User Access",
    )


def write_missing_access_sheet(
    session: Session, google_credentials: Any, users: List[schema.UserAccount]
) -> None:
    """
    Writes to the `[PRODUCTION] CSG and Recidiviz Users Missing Access`
    with information about access/permissions for CSG and Recidiviz users
    in production.
    """
    agencies = AgencyInterface.get_agencies(session=session)
    rows = [
        [
            """
            Description: All CSG team members should have read-only access to all agencies
            in production. As a rule, CSG users should not have admin access unless debugging
            for a specific user case.
            """
        ],
        [""],
        ["CSG or JC Users missing access to a production agency"],
        COLUMN_HEADERS,
    ]

    # Add Data for the `CSG or JC Users missing access to a production agency` table
    for user in users:
        user_agency_names = {a.agency.name for a in user.agency_assocs}
        all_agency_names = {a.name for a in agencies}
        if all_agency_names != user_agency_names:
            no_access_agencies = all_agency_names.difference(user_agency_names)
            rows.append(
                [
                    user.name,
                    user.email,
                    user.id,
                    ", ".join(no_access_agencies),
                    "NOT IN AGENCY",
                ]
            )

    write_and_format_sheet(
        rows=rows,
        index=3,
        num_rows_to_bold=4,
        google_credentials=google_credentials,
        sheet_title="[PRODUCTION] CSG and Recidiviz Users Missing Access",
    )


def write_too_much_access_sheet(
    google_credentials: Any, users: List[schema.UserAccount]
) -> None:
    """
    Writes to the `[PRODUCTION] CSG and Recidiviz Users Too Much Access`
    with information about access/permissions for CSG and Recidiviz users
    in production.
    """
    rows = [
        [
            """
            Description: All JC and CSG team members should have read-only access to all non-demo agencies
            in production. As a rule, JC and CSG users should not have admin access unless debugging
            for a specific user case.

            The demo agencies that that each csg member SHOULD have in production are Law Enforcement [DEMO], 
            Courts [DEMO], Defense [DEMO], Prosecution [DEMO], Jails [DEMO], Prisons [DEMO], Supervision [DEMO],
            and Department of Corrections.
            """
        ],
        [""],
        ["CSG users with > read-only access in a production agencies"],
        COLUMN_HEADERS,
    ]
    for user in users:
        if "@csg.org" not in user.email:
            continue
        role_to_agency_names = defaultdict(list)
        for assoc in user.agency_assocs:
            role_to_agency_names[
                assoc.role.value if assoc.role is not None else "NO ROLE"
            ].append(assoc.agency.name)
        for role, agency_names in role_to_agency_names.items():
            if role != schema.UserAccountRole.READ_ONLY.value:
                rows.append(
                    [
                        user.name,
                        user.email,
                        user.id,
                        ", ".join(agency_names),
                        role,
                    ]
                )

    write_and_format_sheet(
        rows=rows,
        index=4,
        num_rows_to_bold=4,
        google_credentials=google_credentials,
        sheet_title="[PRODUCTION] CSG and Recidiviz Users Too Much Access",
    )


def write_csg_recidiviz_production_sheets(
    session: Session, google_credentials: Any
) -> None:
    """
    Writes to the `[PRODUCTION] CSG and Recidiviz Users Too Much Access`
    sheet and the` PRODUCTION] CSG and Recidiviz Users Missing Access sheet`
    with information about access/permissions for CSG and Recidiviz users
    in production.
    """
    users = UserAccountInterface.get_csg_users(session=session)
    write_missing_access_sheet(
        session=session, google_credentials=google_credentials, users=users
    )
    write_too_much_access_sheet(google_credentials=google_credentials, users=users)


def write_csg_recidiviz_staging_sheet(
    session: Session,
    google_credentials: Any,
) -> None:
    """
    Writes to the `[STAGING] CSG and Recidiviz User Access` sheet with information
    about access/permissions for CSG and Recidiviz users in staging.
    """
    users = UserAccountInterface.get_csg_and_recidiviz_users(session=session)
    rows = [
        [
            """
            Description: The users listed in this table are missing admin access to the agencies listed in the
            'Agencies' Column. All CSG team members should have admin access to all agencies in staging.
            """
        ],
        [""],
        COLUMN_HEADERS,
    ]

    for user in users:
        role_to_agency_names = defaultdict(list)
        for assoc in user.agency_assocs:
            role_to_agency_names[
                assoc.role.value if assoc.role is not None else "NO ROLE"
            ].append(assoc.agency.name)
        # All JC and CSG team members should have admin access to all agencies in staging.
        for role, agency_names in role_to_agency_names.items():
            if role not in (
                schema.UserAccountRole.AGENCY_ADMIN.value,
                schema.UserAccountRole.JUSTICE_COUNTS_ADMIN.value,
            ):
                rows.append(
                    [
                        user.name,
                        user.email,
                        user.id,
                        ", ".join(agency_names),
                        role,
                    ]
                )
    write_and_format_sheet(
        rows=rows,
        index=1,
        google_credentials=google_credentials,
        sheet_title="[STAGING] CSG and Recidiviz User Access",
    )


def write_non_csg_recidiviz_staging_sheet(
    session: Session,
    google_credentials: Any,
) -> None:
    """
    Writes to the `[STAGING] Agency Admin User Access` sheet with information
    about access/permissions for non-CSG and non-Recidiviz users in staging.
    """
    users = UserAccountInterface.get_non_csg_and_recidiviz_users(session=session)
    rows = [
        [
            """
            Description: The users listed are non-CSG and non-Recidiviz users who have access to staging.
            """
        ],
        [""],
        COLUMN_HEADERS,
    ]
    for user in users:
        role_to_agency_names = defaultdict(list)
        for assoc in user.agency_assocs:
            role_to_agency_names[
                assoc.role.value if assoc.role is not None else "NO ROLE"
            ].append(assoc.agency.name)
        for role, agency_names in role_to_agency_names.items():
            rows.append(
                [
                    user.name,
                    user.email,
                    user.id,
                    ", ".join(agency_names),
                    role,
                ]
            )
    write_and_format_sheet(
        rows=rows,
        index=2,
        google_credentials=google_credentials,
        sheet_title="[STAGING] Non-CSG/Recidiviz User Access",
    )


def check_staging_permissions(google_credentials: Any, session: Session) -> None:
    """
    Checks the permissions of all users on staging and records the information
    of the outliers in a google sheet.
    """
    write_csg_recidiviz_staging_sheet(
        session=session,
        google_credentials=google_credentials,
    )
    write_non_csg_recidiviz_staging_sheet(
        session=session,
        google_credentials=google_credentials,
    )


def check_production_permissions(google_credentials: Any, session: Session) -> None:
    """
    Checks the permissions of all users in production and records the information
    of the outliers in a google sheet.
    """

    write_csg_recidiviz_production_sheets(
        session=session,
        google_credentials=google_credentials,
    )
    write_non_csg_recidiviz_production_sheet(
        session=session,
        google_credentials=google_credentials,
    )


def check_user_permissions(
    google_credentials: Any,
    session: Session,
    project_id: Optional[str] = None,
) -> None:
    if project_id == GCP_PROJECT_JUSTICE_COUNTS_STAGING:
        check_staging_permissions(
            google_credentials=google_credentials, session=session
        )
    if project_id == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION:
        check_production_permissions(
            google_credentials=google_credentials, session=session
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    database_key = SQLAlchemyDatabaseKey.for_schema(
        SchemaType.JUSTICE_COUNTS,
    )
    if args.credentials_path is None:
        # When running via Cloud Run Job, GoogleCredentials.get_application_default()
        # will use the service account assigned to the Cloud Run Job.
        # The service account has access to the spreadsheet with editor permissions.
        credentials = GoogleCredentials.get_application_default()
        justice_counts_engine = SQLAlchemyEngineManager.init_engine(
            database_key=database_key,
            secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
        )
        global_session = Session(bind=justice_counts_engine)
        check_user_permissions(
            project_id=args.project_id,
            google_credentials=credentials,
            session=global_session,
        )
    else:
        # When running locally, point to JSON file with service account credentials.
        # The service account has access to the spreadsheet with editor permissions.
        credentials = Credentials.from_service_account_file(args.credentials_path)
        with local_project_id_override(args.project_id):
            with cloudsql_proxy_control.connection(
                schema_type=schema_type,
                secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
            ):
                with SessionFactory.for_proxy(
                    database_key=database_key,
                    secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
                    autocommit=False,
                ) as global_session:
                    check_user_permissions(
                        project_id=args.project_id,
                        google_credentials=credentials,
                        session=global_session,
                    )
