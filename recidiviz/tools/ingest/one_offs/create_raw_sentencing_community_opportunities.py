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
    python -m recidiviz.tools.ingest.one_offs.create_raw_sentencing_community_opportunities --input_file_paths district-1.csv district-2.csv --output_file_name RECIDIVIZ_REFERENCE_community_opportunities.csv
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import List

import pandas as pd

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
    "District (do not edit please!)": "District",
}

LAST_UPDATED_COLUMN_NAME = "lastUpdatedDate"

ORIGINAL_DELIMITER = ","
NEW_DELIMITER = "†"
NEW_LINE_DELIMITER = "‡"


def create_raw_sentencing_community_opportunities(
    input_file_paths: List[str],
    output_file_path: str,
) -> None:
    """Combines all of the input csvs, filters and maps the column names, and write them out to a new file with the correct delimiters."""

    # Reads all of the input files, concatenates them, and filters the columns
    df = pd.concat(
        (
            # Skip the first two rows because they are extra headers
            pd.read_csv(f, delimiter=ORIGINAL_DELIMITER, skiprows=2)
            for f in input_file_paths
        ),
        ignore_index=True,
    )
    df = df[[col for col in df.columns if col in COLUMN_NAME_MAPPING]]
    df = df.rename(mapper=COLUMN_NAME_MAPPING, axis=1)

    # Add a new column with the current date
    df[LAST_UPDATED_COLUMN_NAME] = datetime.today().strftime("%Y-%m-%d")

    df.to_csv(
        path_or_buf=output_file_path,
        sep=NEW_DELIMITER,
        index=False,
        lineterminator=NEW_LINE_DELIMITER,
    )


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
        args.input_file_paths, args.output_file_path
    )
