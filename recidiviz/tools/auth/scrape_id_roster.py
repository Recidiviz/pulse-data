#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#   =============================================================================
"""A script to scrape the current contents of the Idaho DOC Azure Active Directory
for district office employees into a local csv file named "id_roster_YYY-MM-DD.csv".

This can be run on-demand locally with the following command:
    python -m recidiviz.tools.auth.scrape_id_roster
"""
import csv
import logging
from typing import Dict, List, Set, TypedDict

import attr
import msal  # type: ignore
import requests
from progress.spinner import Spinner  # type: ignore

from recidiviz.common.date import today_in_iso
from recidiviz.utils import secrets
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FIELD_EMPLOYEE_NAME = "employee_name"
FIELD_JOB_TITLE = "job_title"
FIELD_DISTRICT = "district"
FIELD_EMAIL_ADDRESS = "email_address"
FIELD_EXTERNAL_ID = "external_id"

ROSTER_FIELD_NAMES = [
    FIELD_EMPLOYEE_NAME,
    FIELD_JOB_TITLE,
    FIELD_DISTRICT,
    FIELD_EMAIL_ADDRESS,
    FIELD_EXTERNAL_ID,
]

DISTRICT_NAME_MAP: Dict[str, str] = {
    "DIST1": "DISTRICT OFFICE 1, COEUR D'ALENE",
    "DIST2": "DISTRICT OFFICE 2, LEWISTON",
    "DIST3": "DISTRICT OFFICE 3, CALDWELL",
    "DIST4": "DISTRICT OFFICE 4, BOISE",
    "DIST4W": "DISTRICT OFFICE 4, BOISE",
    "DIST4E": "DISTRICT OFFICE 4, BOISE",
    "DIST4R": "DISTRICT OFFICE 4, BOISE",
    "DIST5": "DISTRICT OFFICE 5, TWIN FALLS",
    "DIST6": "DISTRICT OFFICE 6, POCATELLO",
    "DIST7": "DISTRICT OFFICE 7, IDAHO FALLS",
}

ALLOWED_JOB_TITLE_KEYWORDS: Set[str] = {
    "ADMINISTRATIVE ASSISTANT",
    "CLINICIAN",
    "MANAGER",
    "REHAB SPECIALIST",
    "OFFICE SPECIALIST",
    "PRE-SENTENCE INVESTIGATOR",
    "OFFICER",
    "RE-ENTRY SPECIALIST",
    "REENTRY SPECIALIST",
    "SUPERVISOR",
    "TECHNICAL RECORDS",
}


DISALLOWED_JOB_TITLE_KEYWORDS: Set[str] = {
    "INTERN",
    "AMERICORPS",
}


@attr.s
class AzureConfig:
    authority: str = attr.ib()
    client_id: str = attr.ib()
    secret: str = attr.ib()
    endpoint: str = attr.ib()
    scope: List[str] = attr.ib()


# TODO(#10190): Migrate this function to secrets.py
def get_secret(secret_id: str) -> str:
    secret_value = secrets.get_secret(secret_id)

    if secret_value is None:
        raise RuntimeError(f'Secret_id "{secret_id}" not found.')

    return secret_value


def get_config() -> AzureConfig:
    tenant_id = get_secret("us_id_ad_tenant_id")
    client_id = get_secret("us_id_ad_client_id")
    client_secret = get_secret("us_id_ad_client_secret")

    return AzureConfig(
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_id=client_id,
        secret=client_secret,
        endpoint="https://graph.microsoft.com/v1.0/users",
        scope=["https://graph.microsoft.com/.default"],
    )


def is_eligible_employee(entry: Dict[str, str]) -> bool:
    if entry["jobTitle"] is None:
        return False

    job_title = entry["jobTitle"].upper()

    if any(keyword in job_title for keyword in DISALLOWED_JOB_TITLE_KEYWORDS):
        return False

    if not any(keyword in job_title for keyword in ALLOWED_JOB_TITLE_KEYWORDS):
        return False

    if (location := entry["officeLocation"]) is None:
        return False

    return "DIST" in location


def parse_entry(entry: Dict[str, str]) -> Dict[str, str]:
    district_name = DISTRICT_NAME_MAP.get(
        entry["officeLocation"].upper(), entry["officeLocation"]
    )

    if district_name is None:
        raise RuntimeError(f'Unexpected district id: "{entry["officeLocation"]}"')

    return {
        FIELD_EMPLOYEE_NAME: entry["displayName"],
        FIELD_JOB_TITLE: entry["jobTitle"],
        FIELD_DISTRICT: district_name,
        FIELD_EMAIL_ADDRESS: entry["userPrincipalName"],
        FIELD_EXTERNAL_ID: entry["userPrincipalName"].split("@")[0],
    }


# Authenticate with the Azure AD server to get an access token
# used for querying the graph endpoint for user information
def get_access_token(config: AzureConfig) -> str:
    # Create a preferably long-lived app instance which maintains a token cache.
    app = msal.ConfidentialClientApplication(
        config.client_id,
        authority=config.authority,
        client_credential=config.secret,
    )

    # Firstly, looks up a token from cache
    # Since we are looking for token for the current app, NOT for an end user,
    # notice we give account parameter as None.
    result = app.acquire_token_silent(config.scope, account=None)

    if not result:
        # No suitable token exists in cache. Let's get a new one from AAD
        result = app.acquire_token_for_client(scopes=config.scope)

    if "access_token" not in result:
        raise RuntimeError(
            f"{result.get('error')}: {result.get('error_description')}. Correlation_id: {result.get('correlation_id')}"
        )

    return result.get("access_token")


def scrape_azure_active_directory() -> None:
    config = get_config()

    access_token = get_access_token(config)

    next_link = config.endpoint

    total_entries = 0
    filename = f"id_roster_{today_in_iso()}.csv"

    with open(filename, "w", newline="") as roster_file:
        writer = csv.DictWriter(roster_file, ROSTER_FIELD_NAMES)

        writer.writeheader()

        spinner = Spinner("Scraping directory ")
        while next_link is not None:
            # Calling graph using the access token
            graph_data = requests.get(  # Use token to call downstream service
                next_link,
                headers={"Authorization": "Bearer " + access_token},
            ).json()
            entries = graph_data["value"]
            next_link = graph_data.get("@odata.nextLink")

            spinner.next()

            for entry in entries:
                if is_eligible_employee(entry):
                    writer.writerow(parse_entry(entry))
                    total_entries += 1

    # print needed to clear line after spinner finishes
    print("")
    logging.info("%d entries written to %s", total_entries, filename)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    with local_project_id_override(GCP_PROJECT_STAGING):
        scrape_azure_active_directory()
