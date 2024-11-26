# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
This script processes race/ethnicity and biological sex CSV files to generate a combined
CSV file containing jurisdiction-level census data. It takes two input folders as arguments:
one containing race/ethnicity data and the other containing biological sex data.

The output CSV file combines these datasets, ensuring that all columns match the expected
format for upload to CSG
"""
import argparse
import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--race_ethnicity_folder",
        required=True,
        help="Path to the folder containing race/ethnicity files.",
    )
    parser.add_argument(
        "--biological_sex_folder",
        required=True,
        help="Path to the folder containing biological sex files.",
    )
    return parser


def create_file(race_ethnicity_folder: str, biological_sex_folder: str) -> None:
    """Processes files from the provided folders and combines them into a single CSV."""
    # Initialize an empty DataFrame to collect data
    final_df = pd.DataFrame()
    total_rows_expected = 0  # Counter to track the total rows from all individual files

    # Process race/ethnicity files
    for re_file in os.listdir(race_ethnicity_folder):
        file_path = os.path.join(race_ethnicity_folder, re_file)
        if file_path.endswith(".csv"):
            try:
                df = pd.read_csv(file_path)
                if {"geoid", "year", "race_eth", "n"}.issubset(df.columns):
                    total_rows_expected += len(df)  # Add the row count to the total
                    df["sex"] = ""  # Add empty sex column
                    df["population"] = df["n"]  # Rename column n to be population
                    df["race_ethnicity"] = df[
                        "race_eth"
                    ]  # Rename column race_eth to be race_ethnicity
                    df = df[
                        ["geoid", "year", "race_ethnicity", "sex", "population"]
                    ]  # Reorder columns
                    final_df = pd.concat([final_df, df], ignore_index=True)
                else:
                    logger.warning("File %s is missing required columns.", file_path)
            except Exception as e:
                logger.error("Failed to process file %s: %s", file_path, str(e))

    # Process biological sex files
    for sex_file in os.listdir(biological_sex_folder):
        file_path = os.path.join(biological_sex_folder, sex_file)
        if file_path.endswith(".csv"):
            try:
                df = pd.read_csv(file_path)
                if {"geoid", "year", "sex", "n"}.issubset(df.columns):
                    total_rows_expected += len(df)  # Add the row count to the total
                    df["race_ethnicity"] = ""  # Add empty race_ethnicity column
                    df["population"] = df["n"]  # Rename column n to be population
                    df = df[
                        ["geoid", "year", "race_ethnicity", "sex", "population"]
                    ]  # Reorder columns
                    final_df = pd.concat([final_df, df], ignore_index=True)
                else:
                    logger.warning("File %s is missing required columns.", file_path)
            except Exception as e:
                logger.error("Failed to process file %s: %s", file_path, str(e))

    # Check if the combined row count matches the expected total
    total_rows_actual = len(final_df)
    if total_rows_actual != total_rows_expected:
        logger.error(
            "Row count mismatch! Expected %d rows, but combined dataset has %d rows.",
            total_rows_expected,
            total_rows_actual,
        )
    else:
        logger.info(
            "Row count verification successful: Combined dataset has %d rows.",
            total_rows_actual,
        )

    # Ensure all numerical columns are integers in the final dataset
    final_df["population"] = final_df["population"].astype(int)
    final_df["year"] = final_df["year"].astype(int)
    # Add an 'id' column as the first column
    final_df.insert(0, "id", range(1, len(final_df) + 1))
    # Save the combined data to the output file
    final_df.to_csv("census_jurisdiction_data.csv", index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()

    create_file(
        race_ethnicity_folder=args.race_ethnicity_folder,
        biological_sex_folder=args.biological_sex_folder,
    )
