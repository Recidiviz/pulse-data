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
Script to be run manually from the command line.
For each state with active workflows, this script creates a new empty sheet/tab in the target
Google Spreadsheet. The new sheet will be named after the state code.

The following command is used while in the root directory to run the script:
python -m recidiviz.tools.utils.workflows_configuration_data_pull --credentials=path=<path>
"""
import argparse
import logging
from typing import List

from google.oauth2 import service_account
from google.oauth2.service_account import Credentials

from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.justice_counts.control_panel.utils import write_data_to_spreadsheet

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


def write_to_workflows_sheet(
    credentials: Credentials, logger: logging.Logger, state_codes: List
) -> None:
    """Writes the inputed date_to_write into our Workflows Configuration Spreadsheet"""

    # Create a new tab/sheet for each state code where workflows is enabled
    for index, state_code in enumerate(state_codes):
        # overwrite_sheets is True for now since we're still not passing in any data, and still aren't making new state tabs for each new day
        write_data_to_spreadsheet(
            credentials,
            WORKFLOWS_CONFIGURATION_SPREADSHEET_ID,
            logger,
            state_code,
            index,
            overwrite_sheets=True,
        )


def main() -> None:
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
    write_to_workflows_sheet(
        credentials=google_credentials, logger=logger, state_codes=state_codes
    )


if __name__ == "__main__":
    main()
