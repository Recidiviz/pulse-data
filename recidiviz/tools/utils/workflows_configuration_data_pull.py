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

To run the script using the recidiviz-staging database, use the following command:
python -m recidiviz.tools.utils.workflows_configuration_data_pull --project-id="recidiviz-staging" --credentials=path=<path>

To run the script using the recidiviz-123 database, use this command:
python -m recidiviz.tools.utils.workflows_configuration_data_pull --project-id="recidiviz-123" --credentials=path=<path>
"""
import argparse
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import google.auth
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
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    in_gcp,
)
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
    parser.add_argument(
        "--project-id",
        help="Which database we use in the script. Can either be 'recidiviz-staging' or 'recidiviz-123'.",
        required=True,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
    )
    return parser


def get_google_credentials(credentials_path: str) -> Credentials:
    """Returns credentials for accessing the Workflows Configuration Google Sheet."""
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    if not credentials_path:
        # When running via Cloud Run Job, google.auth.default()
        # will use the service account assigned to the Cloud Run Job.
        # The service account has access to the spreadsheet with editor permissions.
        credentials, _ = google.auth.default()
    else:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )

    return credentials


def get_format_requests_for_sheet(
    sheet_id: int, new_num_rows: int
) -> List[Dict[str, Any]]:
    """
    Returns the necessary format requests for a sheet in the Workflows Spreadsheet.
    The num_rows parameter specifies how many new rows were added to the top of the
    sheet, ensuring that formatting is only applied to this new data.
    """
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
                "endRowIndex": new_num_rows,
                "startColumnIndex": 1,
                "endColumnIndex": 7,
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
                # there's no way to only have this apply to certain rows
            }
        }
    }

    request_homepage_position_right_align = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": new_num_rows,
                "startColumnIndex": 2,
                "endColumnIndex": 3,
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
    opportunity_data_blocks: List[List[List[str]]],
) -> List[int]:
    """
    Helper function for write_to_workflows_sheet. It returns a list of the rows where
    each border in a state's sheet should be placed at for better readibility.
    """
    border_indices = []
    current_row_index = 1

    # We need a border after each block.
    for block in opportunity_data_blocks:
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
    heading_row = ["" for _ in range(8)]
    heading_row[6] = "Heading"
    heading_row[7] = (
        opportunity_config.initial_header
        if opportunity_config and opportunity_config.initial_header
        else "null"
    )
    opportunity_page_explainer_row = ["" for _ in range(8)]
    opportunity_page_explainer_row[6] = "OpportunityPageExplainer"
    opportunity_page_explainer_row[7] = (
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
    workflows_caseload_tabs_header = ["" for _ in range(7)]
    workflows_caseload_tabs_header[6] = "WorkflowsCaseloadTabs:"
    result.append(workflows_caseload_tabs_header)
    # for spacing to seperate the workflows caseload tabs rows with whatever are the next rows
    empty_row: List[str] = []
    # if no config or no .tab_groups for an existing config
    if not opportunity_config or not opportunity_config.tab_groups:
        null_row = ["" for _ in range(7)]
        null_row[6] = "null"
        result.append(null_row)
        result.append(empty_row)
        return result
    # there are tabs
    workflows_caseload_tabs = opportunity_config.tab_groups
    for workflows_caseload_tab_option in workflows_caseload_tabs:
        workflow_tab_row = ["" for _ in range(8)]
        # ELIGIBILITY_STATUS, GENDER, GENDER - Transgender Only, etc
        workflow_tab_row[6] = workflows_caseload_tab_option["key"]
        workflow_tab_row[7] = str(workflows_caseload_tab_option["tabs"])
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
    eligible_row = ["" for _ in range(8)]
    eligible_row[6] = "CriteriaList__eligible_criteria"
    result.append(eligible_row)
    eligible_criteria_rows = []

    if opportunity_config:
        for criteria in opportunity_config.eligible_criteria_copy:
            new_row = ["" for _ in range(10)]
            # criteria["key"] and criteria["text"] can be None, we assign the string "null" if that's the case
            new_row[8] = criteria["key"] if criteria["key"] else "null"
            new_row[9] = criteria["text"] if criteria["text"] else "null"
            eligible_criteria_rows.append(new_row)
    result.extend(eligible_criteria_rows)

    non_eligible_row = ["" for _ in range(8)]
    non_eligible_row[6] = "CriteriaList__non_eligible_criteria"
    result.append(non_eligible_row)
    non_eligible_criteria_rows = []
    if opportunity_config:
        for criteria in opportunity_config.ineligible_criteria_copy:
            new_row = ["" for _ in range(10)]
            # criteria["key"] and criteria["text"] can be None, we assign the string "null" if that's the case
            new_row[8] = criteria["key"] if criteria["key"] else "null"
            new_row[9] = criteria["text"] if criteria["text"] else "null"
            non_eligible_criteria_rows.append(new_row)
    result.extend(non_eligible_criteria_rows)
    return result


def get_denials(opportunity_config: Optional[FullOpportunityConfig]) -> List[List[str]]:
    """
    Helper for get_opportunity_data. Returns data for the Denied tab title,
    denial reason code, and denial reason display copy for the given opportunity config.
    """
    result = []
    denied_tab_title_row = ["" for _ in range(11)]
    denied_tab_title_row[10] = (
        opportunity_config.denied_tab_title
        if opportunity_config and opportunity_config.denied_tab_title
        else "null"
    )
    result.append(denied_tab_title_row)
    denial_reason_codes_rows = []
    if opportunity_config:
        for denial_reason in opportunity_config.denial_reasons:
            new_row = ["" for _ in range(13)]
            new_row[11] = denial_reason["key"]
            new_row[12] = denial_reason["text"]
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
    opportunity_type_to_config = {config.opportunity_type: config for config in configs}
    return opportunity_type_to_config


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
        "",  # Placeholder for "Date Exported" value, added later
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
    state_code: str, schema_type: SchemaType, use_proxy: bool
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
            get_workflows_enabled_states(), schema_type, using_proxy=use_proxy
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


def get_state_code_to_sheet_id(
    google_spreadsheet_service: Resource, spreadsheet_id: str
) -> Dict[str, int]:
    """
    Returns a dictionary mapping each state code (sheet title) to its corresponding sheet ID.
    This is used to quickly look up a sheet's ID when its title is known.
    This function calls the google sheets api once everytime it's used.
    """
    state_code_to_sheet_id_map = {}
    rsp = (
        google_spreadsheet_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets/properties(sheetId,title)")
        .execute()
    )

    for sheet in rsp["sheets"]:
        sheet_id = sheet["properties"]["sheetId"]
        sheet_title = sheet["properties"]["title"]
        state_code_to_sheet_id_map[sheet_title] = sheet_id
    return state_code_to_sheet_id_map


def get_new_rows_request(sheet_id: Optional[int], num_rows: int) -> List[Any]:
    """
    Returns the API request body for inserting new rows at the top of a specified sheet.
    The number of rows to insert is based on the number of new rows to add.
    """
    if not sheet_id:
        return []
    requests = [
        {
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": num_rows + 1,
                },
                "inheritFromBefore": False,
            }
        },
    ]
    return requests


def add_rows_to_top_of_sheet(
    google_spreadsheet_service: Resource,
    spreadsheet_id: str,
    sheet_id: Optional[int],
    state_code: str,
    logger: logging.Logger,
    num_rows: int,
) -> None:
    """
    Adds a specified number of new rows to the top of an existing sheet after the header row.
    This is a helper function used before writing new data to the spreadsheet.
    This function calls the google sheets api once everytime it's used.
    """
    add_new_rows_request = get_new_rows_request(sheet_id, num_rows)
    google_spreadsheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": add_new_rows_request,
        },
    ).execute()
    logger.info(
        f"{num_rows} have been added to the top of {state_code}'s sheet after the header row"
    )


def write_to_workflows_sheet(
    spreadsheet_id: str,
    credentials: Credentials,
    logger: logging.Logger,
    state_codes: List,
    use_proxy: bool,
) -> Tuple[Dict[str, List[int]], Dict[str, int]]:
    """
    Writes opportunity info for each workflows-enabled state into the Workflows
    Configuration Spreadsheet.

    This function returns two dictionaries:
    1. A mapping of each state code to a list of row indices where borders should be
       placed for readability.
       Example: `border_data_by_sheet_title = {"US_AR": [20, 32, 87], "US_TX": [8]}`
    2. A mapping of each state code to the total number of new rows written for that
       state, which is used for targeted formatting.
       Example: `state_to_new_row_count = {"US_AR": 52, "US_TX": 18}`
    """
    header_data = [
        "Date Exported",
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
    state_to_new_row_count: Dict[str, int] = {}
    # We calculate datetime here once, instead of once for each opportunity in get_opportunity_data for readibility reasons
    # Since it's more clean to get the datetime once and apply it to all the opportunities in the state than to get it once
    # for each opportunity in that state.
    export_date = datetime.now().strftime("%Y-%m-%d")

    google_spreadsheet_service = build("sheets", "v4", credentials=credentials)
    state_code_to_sheet_id = get_state_code_to_sheet_id(
        google_spreadsheet_service, spreadsheet_id
    )

    for index, state_code in enumerate(state_codes):
        existing_sheet = bool(state_code_to_sheet_id.get(state_code))
        # 1. Get the data grouped in blocks
        opportunity_blocks = get_state_code_opportunities(
            state_code, SchemaType.WORKFLOWS, use_proxy
        )

        if not opportunity_blocks:
            # Handle states with no opportunities by writing a single row with the date and "null".
            no_opp_row = [export_date, "null"]
            sheet_data_to_write = (
                [header_data, no_opp_row] if not existing_sheet else [no_opp_row]
            )
            border_data_by_sheet_title[state_code] = [
                2
            ]  # seperate the no_opp_row with everything afterwards if there is already a sheet
        else:
            # 2. Add the export date to the first row of each opportunity block.
            for block in opportunity_blocks:
                # Ensure the block and its first row exist before modifying.
                if block and block[0]:
                    block[0][0] = export_date

            # 3. Calculate border indices from the blocks
            border_indices = calculate_border_indices(opportunity_blocks)

            border_data_by_sheet_title[state_code] = border_indices

            # 4. Flatten the blocks into a single list for writing
            flattened_opportunities = [
                row for block in opportunity_blocks for row in block
            ]

            sheet_data_to_write = [header_data] if not existing_sheet else []
            sheet_data_to_write.extend(flattened_opportunities)

        # add rows to the beginning of the sheet so that the new opportunity data can go there
        num_rows_to_add = len(sheet_data_to_write)
        state_to_new_row_count[state_code] = num_rows_to_add

        # aka if there is already a sheet for this state
        if existing_sheet:
            sheet_id = state_code_to_sheet_id.get(state_code)
            add_rows_to_top_of_sheet(
                google_spreadsheet_service,
                spreadsheet_id,
                sheet_id,
                state_code,
                logger,
                num_rows_to_add,
            )

        write_data_to_spreadsheet(
            credentials,
            spreadsheet_id,
            logger,
            state_code,
            index,
            data_to_write=sheet_data_to_write,
            overwrite_sheets=False,
            value_input_option="USER_ENTERED",
            create_new_sheet=not existing_sheet,
            start_write_range="A1" if not existing_sheet else "A2",
        )

    return border_data_by_sheet_title, state_to_new_row_count


def get_workflows_sheet_format_requests(
    spreadsheet_id: str,
    google_spreadsheet_service: Resource,
    border_data_by_sheet_title: Dict[str, List[int]],
    state_to_new_row_count: Dict[str, int],
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
        num_rows = state_to_new_row_count.get(sheet_title, 0)
        if num_rows > 0:
            format_requests.extend(get_format_requests_for_sheet(sheet_id, num_rows))
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
    state_to_new_row_count: Dict[str, int],
) -> None:
    """
    Applies all necessary format changes to the Workflows Spreadsheet.
    This function makes a single batch API call, as format_requests contains
    all format requests for every state.
    """
    google_spreadsheet_service = build("sheets", "v4", credentials=google_credentials)
    format_requests = get_workflows_sheet_format_requests(
        spreadsheet_id,
        google_spreadsheet_service,
        border_data_by_sheet_title,
        state_to_new_row_count,
    )
    if format_requests:
        google_spreadsheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": format_requests,
            },
        ).execute()
        logger.info("Workflows sheet has been formated")
    else:
        logger.info("No formatting changes were needed for the Workflows sheet.")


def main() -> None:
    """Populates and formats the Workflows Spreadsheet."""
    # grab state codes where workflows are enabled
    state_codes = get_workflows_enabled_states()

    # Set up logging to use the default handler, which sends the logs to standard output.
    # Cloud Run will then capture these logs and they will be visible in the Google Cloud console.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s:%(message)s",
    )

    logger = logging.getLogger(__name__)
    # When running locally, point to JSON file with service account credentials.
    # The service account has access to the spreadsheet with editor permissions.
    args = create_parser().parse_args()
    credentials_path = args.credentials_path
    google_credentials = get_google_credentials(credentials_path)
    project_id = args.project_id
    schema_type = SchemaType.WORKFLOWS

    if in_gcp():
        border_data, state_to_new_row_count = write_to_workflows_sheet(
            WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
            google_credentials,
            logger,
            state_codes,
            use_proxy=False,
        )
        format_workflows_spreadsheet(
            google_credentials,
            WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
            logger,
            border_data,
            state_to_new_row_count,
        )

    else:
        with local_project_id_override(project_id):
            with cloudsql_proxy_control.connection(schema_type=schema_type):
                border_data, state_to_new_row_count = write_to_workflows_sheet(
                    WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
                    google_credentials,
                    logger,
                    state_codes,
                    use_proxy=True,
                )
                format_workflows_spreadsheet(
                    google_credentials,
                    WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
                    logger,
                    border_data,
                    state_to_new_row_count,
                )


if __name__ == "__main__":
    main()
