#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
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
#  =============================================================================
"""
Local script for converting North Dakota mental health and substance abuse community opportunity CSV files
into the standard format expected by downstream processing. The output is a set of new CSV files, 
one per district, each of which contains reformatted mental health and substance abuse opportunities.

Example Usage:
    python -m recidiviz.tools.ingest.one_offs.convert_nd_community_opportunities \
        --mental_health_file_path mental_health.csv \
        --substance_use_file_path substance_use.csv \
        --output_file_directory nd_community_opportunities \
"""

import argparse
import csv
import logging
import sys
from collections import defaultdict
from typing import Dict, List

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def get_county_to_district_mapping() -> Dict[str, str]:
    query = "SELECT * FROM recidiviz-123.sentencing_views.sentencing_counties_and_districts where state_code = 'US_ND'"
    query_job = BigQueryClientImpl().run_query_async(
        query_str=query, use_query_cache=True
    )
    return {row["county"]: row["district"] for row in query_job}


def process_mental_health_data(
    mental_health_data: List[Dict[str, str]],
    county_to_district: Dict[str, str],
    district_to_opps: Dict[str, List[Dict[str, str]]],
) -> None:
    """
    Processes opportunies from the mental health CSV and adds them to the district_to_opps mapping.

    Args:
        mental_health_data: Data from the mental health CSV.
        county_to_district: Mapping of ND counties to districts.
        district_to_opps: Mapping of districts to list of opportunities, formatted in the standard schema.
            This is modified in place.

    Returns:
        None (modifies district_to_opps in place)
    """
    for row in mental_health_data:
        if row["State"] != "North Dakota":
            continue

        county = row["County"]
        if county not in county_to_district:
            logging.info("County Not Found: %s", county)
            continue

        district = county_to_district[county]

        address = f'{row["Street Address"]}\n{row["City"]}, {row["State"]} United States, {row["Zip Code"]}'

        insurance = ""
        if row["Insurance Medicaid"] == "Yes":
            insurance = "Medicaid"
        elif row["Insurance Medicare"] == "Yes":
            insurance = "Medicare"
        elif row["Insurance Other"] == "Yes":
            insurance = "Private insurance"

        mental_health_diagnoses = []
        if row["Adult Spec Bipolar Disorders"] == "Yes":
            mental_health_diagnoses.append("Bipolar Disorder")
        if row["Adult Spec Depression"] == "Yes":
            mental_health_diagnoses.append(
                "Major Depressive Disorder (severe and recurrent)"
            )
        if row["Adult Spec Personality Disorders"] == "Yes":
            mental_health_diagnoses.append("Borderline Personality Disorder")
        if row["Adult Spec Schizophrenia Other Psychotic Disorders"] == "Yes":
            mental_health_diagnoses.append("Delusional Disorder")
            mental_health_diagnoses.append("Psychotic Disorder")
            mental_health_diagnoses.append("Schizophrenia")
            mental_health_diagnoses.append("Schizoaffective Disorder")
            mental_health_diagnoses.append("Schizoid Disorder")

        adults = row["Adult Population Served"] == "Yes"
        children = row["Child Population Served"] == "Yes"
        min_age = ""
        max_age = ""
        if adults and not children:
            min_age = "18"
        if children and not adults:
            max_age = "17"

        new_row = {
            "Opportunity Name": row["Location Name"],
            "Description": row["Program Description"],
            "Provider Name": row["Location Name"],
            "Phone Number": row["Location Phone Number"],
            "Website": row["Website"],
            "Address": address,
            "Needs Addressed": "Mental health",
            "Insurance Accepted": insurance,
            "Available Virtually": row["Telehealth"],
            "Eligibility Criteria": "",
            "Counties Served": row["County"],
            "Min Age": min_age,
            "Max Age": max_age,
            "Gender": "",
            "Mental Health Diagnoses": ", ".join(mental_health_diagnoses),
            "Substance Use Disorder": "",
            "Additional Notes": "",
            "Status": row["Status"],
            "Generic Description": "",
            "District": district,
        }
        district_to_opps[district].append(new_row)


def process_substance_use_data(
    substance_use_data: List[Dict[str, str]],
    county_to_district: Dict[str, str],
    district_to_opps: Dict[str, List[Dict[str, str]]],
) -> None:
    """
    Processes opportunities from the substance use CSV and adds them to the district_to_opps mapping.

    Args:
        substance_use_data: Data from the substance use CSV.
        county_to_district: Mapping of ND counties to districts.
        district_to_opps: Mapping of districts to list of opportunities, formatted in the standard schema.
            This is modified in place.

    Returns:
        None (modifies district_to_opps in place)
    """
    for row in substance_use_data:
        county = row["County"]
        if county not in county_to_district:
            print("County Not Found: %s", county)
            continue

        district = county_to_district[county]

        address = f'{row["Physical Address"]}\n{row["Physical City"]}, {row["Physical State"]} United States, {row["Physical Postal Code"]}'

        sud = ""
        levels_of_care = row["Levels of Care"]
        if "Level 4" in levels_of_care or "Level 3" in levels_of_care:
            sud = "Severe"
        elif "Level 2" in levels_of_care:
            sud = "Moderate"
        elif "Level 1" in levels_of_care:
            sud = "Mild"

        adolescent_cols = [
            "Level 1 – Adolescent",
            "Level 2_1 – Adolescent",
            "Level 2_5 – Adolescent",
            "Level 3_1 - Adolescent",
            "Level 3_5 Adolescent",
            "Level 3_7 Adolescent",
        ]

        adult_cols = [
            "Level 1 – Adult",
            "Level 2_1 – Adult",
            "Level 2_5 – Adult",
            "Level 3_1 Adult",
            "Level 3_5 Adult",
            "Level 3_7 Adult",
        ]

        combined_cols = [
            "Level 1 – Adolescent Adult Combined",
            "Level 2_1 – Adolescent Adult Combined",
            "Level 2_5 - Adolescent Adult Combined",
            "Level 3_1 Adolescent Adult Combined",
            "Level 3_5 Adolescent Adult Combined",
        ]

        adults = False
        children = False
        for col in adolescent_cols + combined_cols:
            if row[col] == "TRUE":
                children = True
        for col in adult_cols + combined_cols:
            if row[col] == "TRUE":
                adults = True

        min_age = ""
        max_age = ""
        if adults and not children:
            min_age = "18"
        if children and not adults:
            max_age = "17"

        status = "Active" if row["Closed Permanently"] == "FALSE" else "Inactive"

        new_row = {
            "Opportunity Name": row["Provider Name"],
            "Description": "",
            "Provider Name": row["Provider Name"],
            "Phone Number": row["Phone"],
            "Website": row["Website"],
            "Address": address,
            "Needs Addressed": "Substance use",
            "Insurance Accepted": "",
            "Available Virtually": "",
            "Eligibility Criteria": "",
            "Counties Served": row["County"],
            "Min Age": min_age,
            "Max Age": max_age,
            "Gender": "",
            "Mental Health Diagnoses": "",
            "Substance Use Disorder": sud,
            "Additional Notes": "",
            "Status": status,
            "Generic Description": "",
            "District": district,
        }
        district_to_opps[district].append(new_row)


def convert_nd_community_opportunities(
    mental_health_file_path: str,
    substance_use_file_path: str,
    output_file_directory: str,
) -> None:
    """
    Processes data from the mental health and subsstance use file paths, and generates multiple CSV files,
    one per district, with opportunities from both input files, in the standard community opportunity format.
    """
    county_to_district_mapping = get_county_to_district_mapping()

    with open(mental_health_file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        mental_health_data = list(reader)

    with open(substance_use_file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        substance_use_data = list(reader)

    # Map of district to list of opportunities (both mental health and substance use) in that district
    district_to_opps: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    process_mental_health_data(
        mental_health_data=mental_health_data,
        county_to_district=county_to_district_mapping,
        district_to_opps=district_to_opps,
    )
    process_substance_use_data(
        substance_use_data=substance_use_data,
        county_to_district=county_to_district_mapping,
        district_to_opps=district_to_opps,
    )

    for district, opps in district_to_opps.items():
        # Write out one file per district
        with open(
            f"{output_file_directory}/{district}.csv", mode="w", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=list(opps[0].keys()))
            writer.writeheader()
            writer.writerows(opps)


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mental_health_file_path",
        required=True,
        type=str,
        help="The filepath of the input mental health CSV file.",
    )

    parser.add_argument(
        "--substance_use_file_path",
        required=True,
        type=str,
        help="The filepath of the input substance use CSV file.",
    )

    parser.add_argument(
        "--output_file_directory",
        required=True,
        type=str,
        help="The name of the directory to which to write output CSV files.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = parse_arguments()

    with local_project_id_override(GCP_PROJECT_STAGING):
        convert_nd_community_opportunities(
            mental_health_file_path=args.mental_health_file_path,
            substance_use_file_path=args.substance_use_file_path,
            output_file_directory=args.output_file_directory,
        )
