#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Script for generating PDF packets for JII in Maine explaining their SCCP eligibility.

The script takes as input:
- Name of a BigQuery view to query for the raw data. This view must contain the following columns:
    - external_id
    - person_name
    - release_date
    - case_manager_name
    - ineligible_criteria
    - array_reasons
- Path to Google credentials

The script will create a folder of PDF packets to this folder in Google Drive:
https://drive.google.com/drive/folders/1DW1ICTq5XjXyz3jbcZ3hJR7h55-qHhOS

Usage: python -m recidiviz.tools.jii.generate_me_sccp_packets \
  --bigquery-view=recidiviz-staging.xxx_scratch.me_sccp_jii_unit_2_query \
  --credentials-directory=/Users/xxx/.config/gcloud
"""

import argparse
import datetime
import io
from ast import literal_eval
from enum import Enum
from typing import Any, Dict, List, Set

from googleapiclient.http import MediaIoBaseUpload

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.google_drive import (
    get_credentials,
    get_docs_service,
    get_drive_service,
)
from recidiviz.utils.metadata import local_project_id_override

# https://drive.google.com/drive/folders/1DW1ICTq5XjXyz3jbcZ3hJR7h55-qHhOS
GENERATED_TEMPLATES_FOLDER_ID = "1nNIImRxeDERno-dgROODWNIv6RaEHF2S"

US_ME_SERVED_X_PORTION_OF_SENTENCE = "US_ME_SERVED_X_PORTION_OF_SENTENCE"
US_ME_X_MONTHS_REMAINING_ON_SENTENCE = "US_ME_X_MONTHS_REMAINING_ON_SENTENCE"
US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS = (
    "US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS"
)
US_ME_CUSTODY_LEVEL_IS_MINIMUM_OR_COMMUNITY = (
    "US_ME_CUSTODY_LEVEL_IS_MINIMUM_OR_COMMUNITY"
)
US_ME_NO_DETAINERS_WARRANTS_OR_OTHER = "US_ME_NO_DETAINERS_WARRANTS_OR_OTHER"


# Each possible combination of ineligible reasons gets its own Google Docs template
class Templates(Enum):
    ELIGIBLE = 1
    ONE_REASON_TIME_FRACTION = 2
    ONE_REASON_MONTHS_REMAINING = 3
    ONE_REASON_VIOLATIONS = 4
    TWO_REASONS_TIME_FRACTION_MONTHS_REMAINING = 5
    TWO_REASONS_TIME_FRACTION_VIOLATIONS = 6
    TWO_REASONS_MONTHS_REMAINING_VIOLATIONS = 7
    THREE_REASONS_TIME_FRACTION_MONTHS_REMAINING_VIOLATIONS = 8


# The values of this dictionary are Google Docs file ids
TEMPLATE_TO_FILE_ID = {
    Templates.ELIGIBLE: "1_3jd-LMreqI26FU77QSH7kKVDY9iFTlvsSUyBn6B0GA",
    Templates.ONE_REASON_TIME_FRACTION: "1jeQ9OzYN1A2yL7wc2af6HGWck7j_YHOpS04G0SMjLfM",
    Templates.TWO_REASONS_TIME_FRACTION_MONTHS_REMAINING: "1lAupNW03B7-XjHgnpJC_I7HpmJJX4jYZogSKzycpIwk",
}

# Input BigQuery view must contain the following columns
REQUIRED_COLUMNS = {
    "external_id",
    "person_name",
    "release_date",
    "case_manager_name",
    "ineligible_criteria",
    "array_reasons",
}

# Google doc template will have the following fields in double brackets
# e.g. {{external id}}
TEMPLATE_COLUMNS = [
    "external id",
    "full name",
    "first name",
    "date",
    "release date",
    "1/2 or 2/3",
    "1/2 or 2/3 date",
    "months remaining date",
    "case manager name",
    "custody level",
    "eligibility date",
]


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bigquery-view",
        help="Name of the BigQuery view that the script will query to get the raw data.",
        required=True,
    )
    parser.add_argument(
        "--credentials-directory",
        required=False,
        default=".",
        help="Directory where the Google API 'credentials.json' files lives. See recidiviz.utils.google_drive for instructions on downloading and storing credentials.",
    )
    return parser


def get_template_file_id(ineligible_set: Set[str]) -> str:
    """Given a combination of ineligible reasons, return the file ID
    of the corresponding Google Docs template.
    """
    template = None
    if ineligible_set == set():
        template = Templates.ELIGIBLE
    elif ineligible_set == {US_ME_SERVED_X_PORTION_OF_SENTENCE}:
        template = Templates.ONE_REASON_TIME_FRACTION
    elif ineligible_set == {US_ME_X_MONTHS_REMAINING_ON_SENTENCE}:
        template = Templates.ONE_REASON_MONTHS_REMAINING
    elif ineligible_set == {US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS}:
        template = Templates.ONE_REASON_VIOLATIONS
    elif ineligible_set == {
        US_ME_SERVED_X_PORTION_OF_SENTENCE,
        US_ME_X_MONTHS_REMAINING_ON_SENTENCE,
    }:
        template = Templates.TWO_REASONS_TIME_FRACTION_MONTHS_REMAINING
    elif ineligible_set == {
        US_ME_SERVED_X_PORTION_OF_SENTENCE,
        US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS,
    }:
        template = Templates.TWO_REASONS_TIME_FRACTION_VIOLATIONS
    elif ineligible_set == {
        US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS,
        US_ME_X_MONTHS_REMAINING_ON_SENTENCE,
    }:
        template = Templates.TWO_REASONS_MONTHS_REMAINING_VIOLATIONS
    elif ineligible_set == {
        US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS,
        US_ME_X_MONTHS_REMAINING_ON_SENTENCE,
        US_ME_SERVED_X_PORTION_OF_SENTENCE,
    }:
        template = Templates.THREE_REASONS_TIME_FRACTION_MONTHS_REMAINING_VIOLATIONS
    else:
        raise ValueError(
            f"No matching template for ineligible reasons {ineligible_set}."
        )

    file_id = TEMPLATE_TO_FILE_ID.get(template)
    if not file_id:
        raise ValueError(f"No matching Google Docs file ID for template {template}")

    return file_id


def str_to_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, "%Y-%m-%d")


def generate_me_sccp_data(bigquery_view: str) -> List[Dict[str, Any]]:
    """
    This method does the following:

    Queries the given BigQuery view, which must include the following columns:
        - external_id
        - person_name
        - release_date
        - case_manager_name
        - ineligible_criteria
        - array_reasons

    Parses this data -- in particular the `ineligible_criteria` blob and `array_reasons` blob --
    into a format that can be used to autofill a Google Doc template.

    Returns a list of dictionaries, each corresponding to a packet.
    """
    query = f"SELECT * FROM {bigquery_view}"
    query_job = BigQueryClientImpl().run_query_async(
        query_str=query, use_query_cache=True
    )

    new_rows = []
    today = datetime.datetime.now()

    for row in query_job:  # pylint: disable=too-many-nested-blocks
        row_columns = set(row.keys())
        missing_columns = REQUIRED_COLUMNS.difference(row_columns)
        if missing_columns:
            raise ValueError(
                "The following required columns were missing from the BQ view: {missing_columns}"
            )

    for row in query_job:  # pylint: disable=too-many-nested-blocks
        name_blob = literal_eval(row["person_name"])
        first_name = name_blob["given_names"].title()
        full_name = first_name + " " + name_blob["surname"].title()

        ineligible_criterias = set(row["ineligible_criteria"])
        criteria_array = [
            literal_eval(r.replace("null", "None")) for r in row["array_reasons"]
        ]

        eligible_reasons = []
        ineligible_reasons = []
        ineligible_set = set()
        one_half_or_two_thirds = None
        one_half_or_two_thirds_date = None
        months_remaining_date = None
        custody_level = None
        eligibility_dates = set()

        for criteria in criteria_array:
            criteria_name = criteria["criteria_name"]

            ### PORTION OF SENTENCE ###
            if criteria_name == US_ME_SERVED_X_PORTION_OF_SENTENCE:
                one_half_or_two_thirds = criteria["reason"]["x_portion_served"]
                if one_half_or_two_thirds not in {"1/2", "2/3"}:
                    raise ValueError(
                        f"Invalid x_portion_served value: {one_half_or_two_thirds}"
                    )

                date_obj = str_to_date(criteria["reason"]["eligible_date"])
                one_half_or_two_thirds_date = date_obj.strftime("%B %-d, %Y")

                # Don't rely on ineligible_criterias to determine if this criteria
                # is eligible or not, because that array won't include this criteria
                # if they're eligible within three months. We want to include this criteria
                # if they're not eligible now, regardless of when they will be.
                if date_obj > today:
                    ineligible_reasons.append(
                        f'{one_half_or_two_thirds} time date on {date_obj.strftime("%-m/%-d/%Y")}'
                    )
                    ineligible_set.add(criteria_name)
                    eligibility_dates.add(date_obj)
                else:
                    eligible_reasons.append(
                        f"Served {one_half_or_two_thirds} of sentence"
                    )

            ### CUSTODY LEVEL ###
            elif criteria_name == US_ME_CUSTODY_LEVEL_IS_MINIMUM_OR_COMMUNITY:
                custody_level = criteria["reason"]["custody_level"]
                if criteria_name in ineligible_criterias:
                    ineligible_reasons.append(f"Custody level: {custody_level}")
                    ineligible_set.add(criteria_name)
                else:
                    eligible_reasons.append(f"Custody level: {custody_level}")

            ### VIOLATIONS ###
            elif criteria_name == US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS:
                if criteria["reason"]:
                    violation_eligible_date = criteria["reason"]["eligible_date"]
                    if violation_eligible_date:
                        date_obj = str_to_date(criteria["reason"]["eligible_date"])
                        # Same as above -- don't rely on ineligible_criterias.
                        if date_obj > today:
                            ineligible_set.add(criteria_name)
                            ineligible_reasons.append(
                                f'Will be violation-free for 90 days on {date_obj.strftime("%-m/%-d/%Y")}'
                            )
                            eligibility_dates.add(date_obj)
                        else:
                            eligible_reasons.append(
                                "No Class A or B violations for 90 days"
                            )
                    else:
                        # If there's no eligibility date given, we can assume this is pending.
                        ineligible_set.add(criteria_name)
                        ineligible_reasons.append("Pending violation")
                else:
                    eligible_reasons.append("No Class A or B violations for 90 days")

            ### DETAINERS ###
            elif criteria_name == US_ME_NO_DETAINERS_WARRANTS_OR_OTHER:
                if criteria_name in ineligible_criterias:
                    ineligible_set.add(criteria_name)
                    ineligible_reasons.append(
                        f'Detainers or warrants: {criteria["reason"]}'
                    )
                else:
                    eligible_reasons.append("No detainers or warrants")

            ### MONTHS REMAINING ###
            elif criteria_name == US_ME_X_MONTHS_REMAINING_ON_SENTENCE:
                date_obj = str_to_date(criteria["reason"]["eligible_date"])
                months_remaining_date = date_obj.strftime("%B %-d, %Y")
                months_based_on_caseload = criteria["reason"][
                    "months_remaining_based_on_caseload"
                ]

                # Same as above -- don't rely on ineligible_criterias.
                if date_obj > today:
                    ineligible_set.add(criteria_name)
                    ineligible_reasons.append(
                        f'{months_based_on_caseload} months remaining on {date_obj.strftime("%-m/%-d/%Y")}'
                    )
                    eligibility_dates.add(date_obj)
                else:
                    eligible_reasons.append(
                        f"Fewer than {months_based_on_caseload} months remaining"
                    )

        new_row = {
            "external id": row["external_id"],
            "full name": full_name,
            "first name": first_name,
            "date": today.strftime("%B %-d, %Y"),
            "release date": row["release_date"].strftime("%B %-d, %Y"),
            "1/2 or 2/3": one_half_or_two_thirds,
            "1/2 or 2/3 date": one_half_or_two_thirds_date,
            "months remaining date": months_remaining_date,
            "case manager name": row["case_manager_name"].title(),
            "custody level": custody_level,
            "eligibility date": max(eligibility_dates).strftime("%B %-d, %Y")
            if eligibility_dates
            else None,
            "ineligible reasons": "\n".join(ineligible_reasons),
            "eligible reasons": "\n".join(eligible_reasons),
            "template_file_id": get_template_file_id(
                ineligible_set=ineligible_criterias
            ),
        }
        new_rows.append(new_row)

    return new_rows


if __name__ == "__main__":
    args = create_parser().parse_args()

    creds = get_credentials(
        directory=args.credentials_directory,
        readonly=False,
        scopes=[
            "https://www.googleapis.com/auth/documents",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    docs_service = get_docs_service(creds=creds)
    drive_service = get_drive_service(creds=creds)

    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        data = generate_me_sccp_data(bigquery_view=args.bigquery_view)

    # Create a top-level folder (named for the current date) to put the packets in
    base_folder = (
        drive_service.files()
        .create(
            body={
                "name": datetime.date.today().strftime("%m-%d-%Y"),
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [GENERATED_TEMPLATES_FOLDER_ID],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    base_folder_id = base_folder.get("id")

    # Create separate folders for docs and pdfs within the top-level folder
    doc_folder = (
        drive_service.files()
        .create(
            body={
                "name": "docs",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [base_folder_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    doc_folder_id = doc_folder.get("id")

    pdf_folder = (
        drive_service.files()
        .create(
            body={
                "name": "pdfs",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [base_folder_id],
            },
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    pdf_folder_id = pdf_folder.get("id")

    for data_row in data:
        file_name = f"[{data_row['full name']}] SCCP Eligibility Details"

        # Create a copy of the template file and put it in the docs folder
        doc_file = (
            drive_service.files()
            .copy(
                fileId=data_row["template_file_id"],
                body={"name": file_name, "parents": [doc_folder_id]},
                supportsAllDrives=True,
            )
            .execute()
        )
        doc_file_id = doc_file.get("id")

        # Run find/replace on all fields in the template file
        requests = [
            {
                "replaceAllText": {
                    "replaceText": data_row[field],
                    "containsText": {
                        "text": f"{{{{{field}}}}}",
                        "matchCase": True,
                    },
                }
            }
            for field in TEMPLATE_COLUMNS
        ]

        # Execute the changes
        docs_service.documents().batchUpdate(
            documentId=doc_file_id, body={"requests": requests}
        ).execute()

        # Export file as a pdf
        pdf_media = MediaIoBaseUpload(
            io.BytesIO(
                drive_service.files()
                .export(fileId=doc_file_id, mimeType="application/pdf")
                .execute()
            ),
            mimetype="application/pdf",
            resumable=True,
        )

        # Write the pdf to the pdf folder
        pdf_file = (
            drive_service.files()
            .create(
                body={
                    "name": file_name,
                    "parents": [pdf_folder_id],
                    "mimeType": "application/pdf",
                },
                media_body=pdf_media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
