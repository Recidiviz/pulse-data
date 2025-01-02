# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Fetches current state resident population data and writes it locally.

python -m recidiviz.tools.datasets.refresh_state_resident_populations
"""

import io

import pandas as pd
import requests

from recidiviz.tools.datasets.static_data_utils import make_output_path

CDC_NOTES_COL = "Notes"
CDC_STATES_COL = "States"
CDC_STATES_CODE_COL = "States Code"
CDC_AGE_GROUP_COL = "Five-Year Age Groups"
CDC_AGE_GROUP_CODE_COL = "Five-Year Age Groups Code"
CDC_RACE_COL = "Race"
CDC_RACE_CODE_COL = "Race Code"
CDC_ETHNICITY_COL = "Ethnicity"
CDC_ETHNICITY_CODE_COL = "Ethnicity Code"
CDC_GENDER_COL = "Gender"
CDC_GENDER_CODE_COL = "Gender Code"
CDC_POPULATION_COL = "Population"

STATE_COL = "state"
AGE_GROUP_COL = "age_group"
RACE_COL = "race"
ETHNICITY_COL = "ethnicity"
GENDER_COL = "gender"
POPULATION_COL = "population"


def fetch_population_csv(year: str) -> io.StringIO:
    """Request the population CSV from the CDC website."""
    response = requests.post(
        "https://wonder.cdc.gov/controller/datarequest/D184",
        timeout=600,
        data=[
            ("saved_id", ""),
            ("dataset_code", "D184"),
            (
                "dataset_label",
                # Note: I think this label will change when they release 2022 estimates.
                "Single-Race Population Estimates 2020-2021 by State and Single-Year Age",
            ),
            ("dataset_vintage_latest", "Single Race,Single Year"),
            ("stage", "request"),
            ("O_javascript", "on"),
            ("M_1", "D184.M1"),
            ("B_1", "D184.V2"),
            ("B_2", "D184.V8"),
            ("B_3", "D184.V4"),
            ("B_4", "D184.V5"),
            ("B_5", "D184.V6"),
            ("O_title", ""),
            ("O_location", "D184.V2"),
            ("V_D184.V2", "*All*"),
            ("finder-stage-D184.V9", "codeset"),
            ("O_V9_fmode", "freg"),
            ("V_D184.V9", ""),
            ("F_D184.V9", "*All*"),
            ("I_D184.V9", "*All* (The United States)"),
            ("O_age", "D184.V8"),
            ("V_D184.V10", "*All*"),
            ("V_D184.V8", "*All*"),
            ("V_D184.V7", "*All*"),
            ("V_D184.V4", "*All*"),
            ("V_D184.V6", "*All*"),
            ("V_D184.V1", year),
            ("V_D184.V5", "*All*"),
            ("O_change_action-Send-Export Results", "Export Results"),
            ("O_show_zeros", "true"),
            ("O_precision", "0"),
            ("O_timeout", "600"),
            # ("O_datatable", "default"),
            ("action-Send", "Send"),
        ],
    )
    response.raise_for_status()
    return io.StringIO(response.text)


def transform_population_df(
    csv_contents: io.TextIOWrapper | io.StringIO,
) -> pd.DataFrame:
    """Pull the CSV into pandas to clean it up."""
    df = pd.read_csv(
        csv_contents,
        sep="\t",
        # Read all as str to avoid N/A issues
        dtype={
            CDC_NOTES_COL: str,
            CDC_STATES_COL: str,
            CDC_STATES_CODE_COL: str,
            CDC_AGE_GROUP_COL: str,
            CDC_AGE_GROUP_CODE_COL: str,
            CDC_RACE_COL: str,
            CDC_RACE_CODE_COL: str,
            CDC_ETHNICITY_COL: str,
            CDC_ETHNICITY_CODE_COL: str,
            CDC_GENDER_COL: str,
            CDC_GENDER_CODE_COL: str,
            CDC_POPULATION_COL: str,
        },
    )

    # Remove methodology from the bottom
    notes_start = df[~df[CDC_NOTES_COL].isna()].iloc[0]
    df = df.iloc[: notes_start.name]

    df = df[
        [
            CDC_STATES_COL,
            CDC_AGE_GROUP_COL,
            CDC_RACE_COL,
            CDC_ETHNICITY_COL,
            CDC_GENDER_COL,
            CDC_POPULATION_COL,
        ]
    ]
    df = df.astype({CDC_POPULATION_COL: int})
    df = df.rename(
        {
            CDC_STATES_COL: STATE_COL,
            CDC_AGE_GROUP_COL: AGE_GROUP_COL,
            CDC_RACE_COL: RACE_COL,
            CDC_ETHNICITY_COL: ETHNICITY_COL,
            CDC_GENDER_COL: GENDER_COL,
            CDC_POPULATION_COL: POPULATION_COL,
        },
        axis="columns",
    )

    return df


def main() -> None:
    """Fetches county population data and writes it locally

    It transforms the location to add fips information, and unpivots the year columns
    into a single year column."""

    YEAR = "2021"
    csv = fetch_population_csv(YEAR)
    df = transform_population_df(csv)
    df.to_csv(make_output_path("state_resident_populations.csv"), index=False)


if __name__ == "__main__":
    main()
