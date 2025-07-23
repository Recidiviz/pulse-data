# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
This script is designed to be run manually from the command line.
For each state with active workflows, it creates a new empty sheet/tab in the target Google Spreadsheet, named after the state code.
It then populates this sheet with available opportunities in that state, alongside 
Opportunity information such as System Type, Homepage Position, and Completion Event.

To run the script, navigate to the root directory and use the following command:
python -m recidiviz.tools.utils.workflows_configuration_data_pull --credentials=path=<path>
"""
import argparse
import logging
from typing import Any, Dict, List

from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import Resource, build

from recidiviz.admin_panel.routes.workflows import refine_state_code
from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.justice_counts.control_panel.utils import write_data_to_spreadsheet
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.querier.querier import WorkflowsQuerier

# Spreadsheet Name: Workflows Configuration
# https://docs.google.com/spreadsheets/d/1QgYQXs0Cm0wbxJTbwL19DbqTXRVIlNpXOCzMligFHaI/edit?gid=1935068964#gid=1935068964
WORKFLOWS_CONFIGURATION_SPREADSHEET_ID = "1QgYQXs0Cm0wbxJTbwL19DbqTXRVIlNpXOCzMligFHaI"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--credentials-path",
        help="Used to point to path of JSON file with Google Cloud credentials.",
        required=False,
    )
    return parser


def get_google_credentials(credentials_path: str) -> Credentials:
    """Returns credentials for access to the workflows-configuration google sheet"""
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=SCOPES
    )

    return credentials


def get_format_requests_for_sheet(sheet_id: int) -> List[Dict[str, Any]]:
    """Necessary format request a sheet/tab in our Workflows Spreadsheet."""
    request_bold = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "startColumnIndex": 0,
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold",
        }
    }
    request_frozen_header = {
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {
                    "frozenRowCount": 1  # only want the header to be frozen
                },
            },
            "fields": "gridProperties.frozenRowCount",
        }
    }

    request_text_fit = {
        "autoResizeDimensions": {
            "dimensions": {
                "sheetId": sheet_id,
                "dimension": "COLUMNS",
                "startIndex": 0,
            }
        }
    }

    request_homepage_position_right_align = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "startColumnIndex": 1,
                "endColumnIndex": 2,
            },
            "cell": {"userEnteredFormat": {"horizontalAlignment": "RIGHT"}},
            "fields": "userEnteredFormat.horizontalAlignment",
        }
    }

    return [
        request_bold,
        request_frozen_header,
        request_text_fit,
        request_homepage_position_right_align,
    ]


def get_all_sheet_ids(
    spreadsheet_id: str, google_spreadsheet_service: Resource
) -> List[int]:
    """Returns all the sheet ids in the spreadsheet.
    This function makes 1 google sheet api request.
    """
    rsp = (
        google_spreadsheet_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets/properties(sheetId,title)")
        .execute()
    )
    result = []
    for sheet in rsp["sheets"]:
        result.append(sheet["properties"]["sheetId"])
    return result


def get_workflows_sheet_format_requests(
    spreadsheet_id: str, google_spreadsheet_service: Resource
) -> List[Dict[str, Any]]:
    """Returns all the Format Requests needed for the Workflows Spreadsheet."""
    format_requests = []
    sheet_ids = get_all_sheet_ids(spreadsheet_id, google_spreadsheet_service)
    for sheet_id in sheet_ids:
        format_requests.extend(get_format_requests_for_sheet(sheet_id))
    return format_requests


def format_workflows_spreadsheet(
    google_credentials: Credentials,
    spreadsheet_id: str,
    logger: logging.Logger,
) -> None:
    """Applies all the necessary format changes to our Workflows Spreadsheet.
    The amount of google spreadsheet api calls that this function will ever make at once
    is just 1 call. This is because format_requests contains all the format requests for each state.
    """
    google_spreadsheet_service = build("sheets", "v4", credentials=google_credentials)
    format_requests = get_workflows_sheet_format_requests(
        spreadsheet_id, google_spreadsheet_service
    )
    google_spreadsheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": format_requests,
        },
    ).execute()
    logger.info("Workflows sheet has been formated")


def get_state_code_opportunities(
    state_code: str, schema_type: SchemaType
) -> List[List[str]]:
    """Helper function for the write_to_workflows_sheet function. It returns all the opportunity info for the inputed state_code"""
    Querier = WorkflowsQuerier(
        refine_state_code(state_code),
        database_manager=StateSegmentedDatabaseManager(
            get_workflows_enabled_states(), schema_type, using_proxy=True
        ),
    )
    opportunities = Querier.get_opportunities()
    state_opportunity_data = []
    for opportunity in opportunities:
        opportunity_info = [
            str(opportunity.system_type.name),
            str(opportunity.homepage_position),
            opportunity.opportunity_type,
            ((opportunity.completion_event).split("."))[1],
            opportunity.gating_feature_variant
            if opportunity.gating_feature_variant
            else "",
        ]
        state_opportunity_data.append(opportunity_info)
    return state_opportunity_data


def write_to_workflows_sheet(
    spreadsheet_id: str,
    credentials: Credentials,
    logger: logging.Logger,
    state_codes: List,
) -> None:
    """Writes the available opportunity info for each workflows enabled state into our Workflows Configuration Spreadsheet.
    The amount of google spreadsheet api calls we make is "n", where "n" is the amount of states. Therefore, the
    most amount of api calls that this function will ever make is around 50. Given that Recidiviz expands to all states,
    and other US territories.
    """

    header_data = [
        "System Type",
        "Home Page Position",
        "Opportunity",
        "Completion Event",
        "Feature Variant",
    ]

    # Create a new tab/sheet for each state code where workflows is enabled
    for index, state_code in enumerate(state_codes):
        sheet_data = []
        sheet_data.append(header_data)
        state_code_opportunities = get_state_code_opportunities(
            state_code, SchemaType.WORKFLOWS
        )
        if state_code_opportunities:
            sheet_data.extend(state_code_opportunities)

        # overwrite_sheets is True for now since we're still not making new state tabs for each new day
        write_data_to_spreadsheet(
            credentials,
            spreadsheet_id,
            logger,
            state_code,
            index,
            data_to_write=sheet_data,
            overwrite_sheets=True,
        )


def main() -> None:
    """Populates and Formats our Workflows Spreadsheet"""
    # grab state codes where workflows are enabled
    state_codes = get_workflows_enabled_states()

    # Set up logging to a file named 'app.log' in the current directory
    # In production, this might be changed to have the logs go wherever they usually go in production
    logging.basicConfig(
        filename="logs/app.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
    )

    logger = logging.getLogger(__name__)
    # When running locally, point to JSON file with service account credentials.
    # The service account has access to the spreadsheet with editor permissions.
    args = create_parser().parse_args()
    credentials_path = args.credentials_path
    google_credentials = get_google_credentials(credentials_path)

    schema_type = SchemaType.WORKFLOWS

    with local_project_id_override(GCP_PROJECT_STAGING):
        with cloudsql_proxy_control.connection(schema_type=schema_type):

            write_to_workflows_sheet(
                WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
                google_credentials,
                logger,
                state_codes,
            )
            format_workflows_spreadsheet(
                google_credentials, WORKFLOWS_CONFIGURATION_SPREADSHEET_ID, logger
            )


if __name__ == "__main__":
    main()
