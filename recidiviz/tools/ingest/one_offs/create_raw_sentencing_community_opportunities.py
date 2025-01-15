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
Local script for taking individual CSV files for sentencing community opportunities by district and combining them into a single CSV file with a new delimiter.

Example Usage:
    python -m recidiviz.tools.ingest.one_offs.create_raw_sentencing_community_opportunities --input_file_names district-1.csv district-2.csv --output_file_name output.csv
"""

import argparse
import csv
import logging
import re
import sys
from datetime import datetime
from typing import List

COLUMN_NAME_MAPPING = {
    "Opportunity Name": "OpportunityName",
    "Description": "Description",
    "Provider Name": "ProviderName",
    "Provider Phone Number": "ProviderPhoneNumber",
    "Provider Website": "ProviderWebsite",
    "Provider Address": "ProviderAddress",
    "Needs Addressed (multi-select)": "NeedsAddressed",
    "Eligibility Criteria (multiselect)": "EligibilityCriteria",
    "Additional Notes": "AdditionalNotes",
    "ASAM Level of Care": "ASAMLevelCriteria",
    "Mental Health Diagnoses": "mentalHealthDisorderCriteria",
    "Substance Use Disorder": "substanceUseDisorderCriteria",
    "Min LSI-R": "minLSIRScore",
    "Max LSI-R": "maxLSIRScore",
    "Min Age": "minAge",
    "Max Age": "maxAge",
    "Gender": "genders",
    "Counties Served": "countiesServed",
    "Status": "status",
    "Generic Description (do not edit please!)": "genericDescription",
}

ORIGINAL_DELIMITER = ","
NEW_DELIMITER = "†"
NEW_LINE_DELIMITER = "‡"


def create_raw_sentencing_community_opportunities(
    input_file_paths: List[str], output_file_path: str
) -> None:
    """Creates a new CSV file with the specified delimiter for the provided files."""

    date = datetime.today().strftime("%Y-%m-%d")

    for idx, input_file_path in enumerate(input_file_paths):
        # Infer the district name from the file name
        district = "D" + re.findall(r"\d+", input_file_path)[0]

        # Read the entire content of the input file
        with open(
            input_file_path, mode="r", newline="", encoding="utf-8"
        ) as input_file:
            reader = csv.reader(input_file, delimiter=ORIGINAL_DELIMITER)

            # Include the header line for the first file, ignore it for the rest
            # The first two lines are extra headers, the third line is the real header
            lines_to_skip = 2 if idx == 0 else 3
            for _ in range(lines_to_skip):
                next(reader)

            # Collect the rows and modify as needed
            modified_rows = []

            # For the first line in the first file, map the header names
            if idx == 0:
                # Map the column names to the expected names
                mapped_columns = [
                    COLUMN_NAME_MAPPING.get(col, col) for col in next(reader)
                ]

                # Manually add these columns since they won't be present in the input file
                mapped_columns.extend(
                    [
                        "District",
                        "CapacityTotal",
                        "CapacityAvailable",
                        "lastUpdatedDate",
                    ]
                )
                modified_rows.append(mapped_columns)

            for row in reader:
                cleaned_row = [field.replace("\n", ",") for field in row]

                # Add the district, capacity total, capacity available, and last updated date
                cleaned_row.extend([district, "", "", date])
                modified_rows.append(cleaned_row)

        # Write the modified content to the output file
        with open(
            output_file_path, mode="a", newline="", encoding="utf-8"
        ) as output_file:
            writer = csv.writer(
                output_file, delimiter=NEW_DELIMITER, lineterminator=NEW_LINE_DELIMITER
            )
            writer.writerows(modified_rows)


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_file_paths",
        required=True,
        nargs="+",
        type=str,
        help="The filepath of the input CSV file.",
    )

    parser.add_argument(
        "--output_file_path",
        required=True,
        type=str,
        help="The filepath of the output CSV file.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = parse_arguments()

    create_raw_sentencing_community_opportunities(
        args.input_file_paths,
        args.output_file_path,
    )
