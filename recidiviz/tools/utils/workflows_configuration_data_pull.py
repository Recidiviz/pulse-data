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
from typing import Any, Dict, List, Optional

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
from recidiviz.workflows.types import FullOpportunityConfig, FullOpportunityInfo

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
    """Returns credentials for accessing the Workflows Configuration Google Sheet."""
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=SCOPES
    )

    return credentials


def get_format_requests_for_sheet(sheet_id: int) -> List[Dict[str, Any]]:
    """Returns the necessary format requests for a sheet in the Workflows Spreadsheet."""
    request_bold_header = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold",
        }
    }

    request_bold_opportunity_info = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": 6,
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
        request_bold_header,
        request_bold_opportunity_info,
        request_frozen_header,
        request_text_fit,
        request_homepage_position_right_align,
    ]


def get_single_border_request(sheet_id: str, startRowIndex: int) -> Any:
    """Returns a template for a Google Sheets API request that places a black border in a sheet."""
    request = {
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": startRowIndex,
                "endRowIndex": startRowIndex + 1,
                "startColumnIndex": 0,
            },
            "top": {
                "style": "SOLID_THICK",
                "color": {"red": 0, "green": 0, "blue": 0},
            },
        }
    }
    return request


def get_all_border_requests_for_a_sheet(
    sheet_id: str, border_indices: List[int]
) -> List[Any]:
    """
    Returns all necessary border requests to place a black border line
    between each opportunity in a state's sheet.
    """
    border_requests: List[Any] = []
    if not border_indices:
        return border_requests
    for start_index in border_indices:
        new_request = get_single_border_request(sheet_id, start_index)
        border_requests.append(new_request)
    return border_requests


def calculate_border_indices(
    opportunity_data_blocks: List[List[List[str]]], header_row_count: int = 1
) -> List[int]:
    """
    Helper function for write_to_workflows_sheet. It returns a list of the rows where
    each border in a state's sheet should be placed at for better readibility.
    """
    border_indices = []
    current_row_index = header_row_count

    # We need a border after each block, except for the last one.
    for block in opportunity_data_blocks[:-1]:
        current_row_index += len(block)
        border_indices.append(current_row_index)

    return border_indices


def get_heading_and_opportunity_page_explainer(
    opportunity_config: Optional[FullOpportunityConfig],
) -> List[List[str]]:
    """
    Helper for get_opportunity_data. Returns the heading and opportunity page
    explainer data for the given opportunity config.
    """
    result = []
    heading_row = ["" for _ in range(7)]
    heading_row[5] = "Heading"
    heading_row[6] = (
        opportunity_config.initial_header
        if opportunity_config and opportunity_config.initial_header
        else "null"
    )
    opportunity_page_explainer_row = ["" for _ in range(7)]
    opportunity_page_explainer_row[5] = "OpportunityPageExplainer"
    opportunity_page_explainer_row[6] = (
        opportunity_config.subheading
        if opportunity_config and opportunity_config.subheading
        else "null"
    )
    result.append(heading_row)
    result.append(opportunity_page_explainer_row)
    return result


def get_workflows_caseload_tabs(
    opportunity_config: Optional[FullOpportunityConfig],
) -> List[List[str]]:
    """
    Helper for get_opportunity_data. Returns the data for the workflows
    caseload tabs for a given opportunity config.
    """
    result = []
    workflows_caseload_tabs_header = ["" for _ in range(6)]
    workflows_caseload_tabs_header[5] = "WorkflowsCaseloadTabs:"
    result.append(workflows_caseload_tabs_header)
    # for spacing to seperate the workflows caseload tabs rows with whatever are the next rows
    empty_row: List[str] = []
    # if no config or no .tab_groups for an existing config
    if not opportunity_config or not opportunity_config.tab_groups:
        null_row = ["" for _ in range(6)]
        null_row[5] = "null"
        result.append(null_row)
        result.append(empty_row)
        return result
    # there are tabs
    workflows_caseload_tabs = opportunity_config.tab_groups
    for workflows_caseload_tab_option in workflows_caseload_tabs:
        workflow_tab_row = ["" for _ in range(7)]
        # ELIGIBILITY_STATUS, GENDER, GENDER - Transgender Only, etc
        workflow_tab_row[5] = workflows_caseload_tab_option["key"]
        workflow_tab_row[6] = str(workflows_caseload_tab_option["tabs"])
        result.append(workflow_tab_row)
    result.append(empty_row)
    return result


def get_eligible_and_ineligble_criteria_rows(
    opportunity_config: Optional[FullOpportunityConfig],
) -> List[List[str]]:
    """
    Helper for get_opportunity_data. Returns the eligible and ineligible
    criteria data for the given opportunity config.
    """
    result = []
    eligible_row = ["" for _ in range(7)]
    eligible_row[5] = "CriteriaList__eligible_criteria"
    result.append(eligible_row)
    eligible_criteria_rows = []

    if opportunity_config:
        for criteria in opportunity_config.eligible_criteria_copy:
            new_row = ["" for _ in range(9)]
            # criteria["key"] and criteria["text"] can be None, we assign the string "null" if that's the case
            new_row[7] = criteria["key"] if criteria["key"] else "null"
            new_row[8] = criteria["text"] if criteria["text"] else "null"
            eligible_criteria_rows.append(new_row)
    result.extend(eligible_criteria_rows)

    non_eligible_row = ["" for _ in range(7)]
    non_eligible_row[5] = "CriteriaList__non_eligible_criteria"
    result.append(non_eligible_row)
    non_eligible_criteria_rows = []
    if opportunity_config:
        for criteria in opportunity_config.ineligible_criteria_copy:
            new_row = ["" for _ in range(9)]
            # criteria["key"] and criteria["text"] can be None, we assign the string "null" if that's the case
            new_row[7] = criteria["key"] if criteria["key"] else "null"
            new_row[8] = criteria["text"] if criteria["text"] else "null"
            non_eligible_criteria_rows.append(new_row)
    result.extend(non_eligible_criteria_rows)
    return result


def get_denials(opportunity_config: Optional[FullOpportunityConfig]) -> List[List[str]]:
    """
    Helper for get_opportunity_data. Returns data for the Denied tab title,
    denial reason code, and denial reason display copy for the given opportunity config.
    """
    result = []
    denied_tab_title_row = ["" for _ in range(10)]
    denied_tab_title_row[9] = (
        opportunity_config.denied_tab_title
        if opportunity_config and opportunity_config.denied_tab_title
        else "null"
    )
    result.append(denied_tab_title_row)
    denial_reason_codes_rows = []
    if opportunity_config:
        for denial_reason in opportunity_config.denial_reasons:
            new_row = ["" for _ in range(12)]
            new_row[10] = denial_reason["key"]
            new_row[11] = denial_reason["text"]
            denial_reason_codes_rows.append(new_row)
        result.extend(denial_reason_codes_rows)
    return result


def get_opportunity_to_config(
    configs: List[FullOpportunityConfig],
) -> Dict[str, FullOpportunityConfig]:
    """
    Helper for get_state_code_opportunities. Returns a dictionary mapping an
    opportunity type (e.g., 'usTxAnnualReportStatus') to its corresponding config.
    """
    # we can assume only 1 opportunity_type for each state
    # each opportunity type matches its config data
    hash_map = {config.opportunity_type: config for config in configs}
    return hash_map


def get_opportunity_data(
    opportunity: FullOpportunityInfo, matching_config: Optional[FullOpportunityConfig]
) -> List[List[str]]:
    """
    Helper for get_state_code_opportunities. Returns a list of rows containing all
    important aspects of an opportunity, such as system type, completion event,
    criteria, and denial information.
    """
    result = []
    top_row = [
        str(opportunity.system_type.name),
        str(opportunity.homepage_position),
        opportunity.opportunity_type,
        ((opportunity.completion_event).split("."))[1],
        opportunity.gating_feature_variant
        if opportunity.gating_feature_variant
        else "",
    ]

    header_and_opportunity_page_explainer = get_heading_and_opportunity_page_explainer(
        matching_config
    )
    workflows_caseload_tabs = get_workflows_caseload_tabs(matching_config)
    criteria_rows = get_eligible_and_ineligble_criteria_rows(matching_config)
    denial_rows = get_denials(matching_config)

    result.append(top_row)
    result.extend(header_and_opportunity_page_explainer)
    result.extend(workflows_caseload_tabs)
    result.extend(criteria_rows)
    result.extend(denial_rows)

    return result


def get_state_code_opportunities(
    state_code: str, schema_type: SchemaType
) -> List[List[List[str]]]:
    """
    Helper for write_to_workflows_sheet. Returns all opportunity information
    for the given state code, grouped by opportunity.

    An example of a return value can be opportunity_data_blocks = (
        [ [ ["INCARCERATION", "1", "usArInstitutionalWorkerStatus"], [] ], [ ["INCARCERATION", "2", "usArWorkRelease"] ] ]
    )
    where len(opportunity_data_blocks) = 2, since there 2 opportunites in this state, and len(opportunity_data_blocks[1]) = 1, since there
    is one row of data for the "usArWorkRelease" opportunity, and opportunity_data_blocks[0][1] means that the second row for the
    "usArInstitutionalWorkerStatus" opportunity is just an empty row of data. This setup makes calculating the border_indices easier in a later
    helper function possible.
    """
    Querier = WorkflowsQuerier(
        refine_state_code(state_code),
        database_manager=StateSegmentedDatabaseManager(
            get_workflows_enabled_states(), schema_type, using_proxy=True
        ),
    )
    opportunities = Querier.get_opportunities()
    opportunity_types = [opportunity.opportunity_type for opportunity in opportunities]
    configs = Querier.get_active_configs_for_opportunity_types(opportunity_types)
    opportunity_to_config = get_opportunity_to_config(configs)

    # This will be a list of lists (a list of row blocks)
    opportunity_data_blocks = []

    for opportunity in opportunities:
        matching_config = opportunity_to_config.get(opportunity.opportunity_type)
        # Get the block of rows for this single opportunity
        opportunity_data = get_opportunity_data(opportunity, matching_config)
        opportunity_data_blocks.append(opportunity_data)

    return opportunity_data_blocks


def write_to_workflows_sheet(
    spreadsheet_id: str,
    credentials: Credentials,
    logger: logging.Logger,
    state_codes: List,
) -> Dict[str, List[int]]:
    """
    Writes opportunity info for each workflows-enabled state into the Workflows
    Configuration Spreadsheet. This function makes one API call per state. It returns
    a dictionary mapping each state code to a list of row indices where borders
    should be placed, facilitating subsequent formatting.
    An example of a return value can be border_data_by_sheet_title = {"US_AR" : [20, 32, 87], "US_TX" : [8], "US_OR" : []}
    """
    header_data = [
        "System Type",
        "Home Page Position",
        "Opportunity",
        "Completion Event",
        "Feature Variant",
        "Component",
        "Component display copy",
        "Criteria",
        "Criteria copy",
        "Denied tab title",
        "Denial Reason Code",
        "Denial Reason display copy",
    ]

    border_data_by_sheet_title: Dict[str, List[int]] = {}

    for index, state_code in enumerate(state_codes):
        # 1. Get the data grouped in blocks
        opportunity_blocks = get_state_code_opportunities(
            state_code, SchemaType.WORKFLOWS
        )

        if not opportunity_blocks:
            # Handle states with no opportunities if necessary
            # For now, we can just write the header
            sheet_data_to_write = [header_data]
            border_data_by_sheet_title[state_code] = []
        else:
            # 2. Calculate border indices from the blocks
            border_indices = calculate_border_indices(
                opportunity_blocks, header_row_count=len([header_data])
            )
            border_data_by_sheet_title[state_code] = border_indices

            # 3. Flatten the blocks into a single list for writing
            flattened_opportunities = [
                row for block in opportunity_blocks for row in block
            ]

            sheet_data_to_write = [header_data]
            sheet_data_to_write.extend(flattened_opportunities)

        write_data_to_spreadsheet(
            credentials,
            spreadsheet_id,
            logger,
            state_code,
            index,
            data_to_write=sheet_data_to_write,
            overwrite_sheets=True,
        )

    return border_data_by_sheet_title


def get_workflows_sheet_format_requests(
    spreadsheet_id: str,
    google_spreadsheet_service: Resource,
    border_data_by_sheet_title: Dict[str, List[int]],
) -> List[Dict[str, Any]]:
    """
    Returns all format requests needed for the Workflows Spreadsheet. This function
    makes one call to the google spreadsheets api.
    """
    format_requests = []
    rsp = (
        google_spreadsheet_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets/properties(sheetId,title)")
        .execute()
    )

    for sheet in rsp["sheets"]:
        sheet_id = sheet["properties"]["sheetId"]
        sheet_title = sheet["properties"]["title"]
        format_requests.extend(get_format_requests_for_sheet(sheet_id))
        # Look up the border indices for this specific sheet by its title
        border_indices_for_sheet = border_data_by_sheet_title.get(sheet_title, [])
        format_requests.extend(
            get_all_border_requests_for_a_sheet(sheet_id, border_indices_for_sheet)
        )
    return format_requests


def format_workflows_spreadsheet(
    google_credentials: Credentials,
    spreadsheet_id: str,
    logger: logging.Logger,
    border_data_by_sheet_title: Dict[str, List[int]],
) -> None:
    """
    Applies all necessary format changes to the Workflows Spreadsheet.
    This function makes a single batch API call, as format_requests contains
    all format requests for every state.
    """
    google_spreadsheet_service = build("sheets", "v4", credentials=google_credentials)
    format_requests = get_workflows_sheet_format_requests(
        spreadsheet_id, google_spreadsheet_service, border_data_by_sheet_title
    )
    google_spreadsheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": format_requests,
        },
    ).execute()
    logger.info("Workflows sheet has been formated")


def main() -> None:
    """Populates and formats the Workflows Spreadsheet."""
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

            border_data = write_to_workflows_sheet(
                WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
                google_credentials,
                logger,
                state_codes,
            )
            format_workflows_spreadsheet(
                google_credentials,
                WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
                logger,
                border_data,
            )


if __name__ == "__main__":
    main()
