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
"""Script for generating data for JII in Maine explaining their SCCP eligibility.

The script takes as input:
- Name of a BigQuery view to query for the raw data. This view must contain the following columns:
    - external_id
    - person_name
    - release_date
    - case_manager_name
    - ineligible_criteria
    - array_reasons
- Path to directory where the script will write the generated spreadsheet, which can be used 
  to autofill Google Docs templates.

Usage: python -m recidiviz.tools.jii.generate_me_sccp_data \
  --bigquery-view=recidiviz-staging.xxx_scratch.me_sccp_jii_unit_2_query \
  --output-path=/Users/xxx/data.csv
"""

import argparse
import csv
import datetime
from ast import literal_eval
from enum import Enum
from typing import Any, Dict, List, Set

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

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


# Input BigQuery view must contain the following columns
REQUIRED_COLUMNS = {
    "external_id",
    "person_name",
    "release_date",
    "case_manager_name",
    "ineligible_criteria",
    "array_reasons",
}

# Output spreadsheet will have the following columns
FIELD_NAMES = [
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
    "template",
    "ineligible reasons",
    "eligible reasons",
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
        "--output-path",
        help="Path to directory where the script will write the generated spreadsheet.",
        required=True,
    )
    return parser


def get_template(ineligible_set: Set[str]) -> int:
    """Given a combination of ineligible reasons, return the corresponding template."""
    if ineligible_set == set():
        return Templates.ELIGIBLE.value
    if ineligible_set == {US_ME_SERVED_X_PORTION_OF_SENTENCE}:
        return Templates.ONE_REASON_TIME_FRACTION.value
    if ineligible_set == {US_ME_X_MONTHS_REMAINING_ON_SENTENCE}:
        return Templates.ONE_REASON_MONTHS_REMAINING.value
    if ineligible_set == {US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS}:
        return Templates.ONE_REASON_VIOLATIONS.value
    if ineligible_set == {
        US_ME_SERVED_X_PORTION_OF_SENTENCE,
        US_ME_X_MONTHS_REMAINING_ON_SENTENCE,
    }:
        return Templates.TWO_REASONS_TIME_FRACTION_MONTHS_REMAINING.value
    if ineligible_set == {
        US_ME_SERVED_X_PORTION_OF_SENTENCE,
        US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS,
    }:
        return Templates.TWO_REASONS_TIME_FRACTION_VIOLATIONS.value
    if ineligible_set == {
        US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS,
        US_ME_X_MONTHS_REMAINING_ON_SENTENCE,
    }:
        return Templates.TWO_REASONS_MONTHS_REMAINING_VIOLATIONS.value
    if ineligible_set == {
        US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS,
        US_ME_X_MONTHS_REMAINING_ON_SENTENCE,
        US_ME_SERVED_X_PORTION_OF_SENTENCE,
    }:
        return Templates.THREE_REASONS_TIME_FRACTION_MONTHS_REMAINING_VIOLATIONS.value

    raise ValueError(f"No matching template for ineligible reasons {ineligible_set}.")


def str_to_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, "%Y-%m-%d")


def generate_me_sccp_data(bigquery_view: str) -> List[Dict[str, Any]]:
    """
    This method does the following:
        1) Queries the given BigQuery view for the raw data.
        2) Parses this data -- in particular the `ineligible_criteria` blob and `array_reasons` blob --
           into a format that can be used to autofill a Google Doc template.
        3) Returns a list of dictionaries, each corresponding to a row to write to the output spreadsheet.
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
            "template": get_template(ineligible_set=ineligible_criterias),
        }
        new_rows.append(new_row)

    return new_rows


if __name__ == "__main__":
    args = create_parser().parse_args()
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        data = generate_me_sccp_data(bigquery_view=args.bigquery_view)
        with open(args.output_path, "w", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELD_NAMES)
            writer.writeheader()
            writer.writerows(data)
