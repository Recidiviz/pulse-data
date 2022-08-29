# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Generates a CSV file of FIPS codes and county names.

Usage: `python -m recidiviz.tools.datasets.refresh_county_fips`
"""
import os
from typing import List

import pandas as pd
from us import states

from recidiviz.common import data_sets, fips
from recidiviz.datasets import static_data

FIPS_2018_URL = "https://www2.census.gov/programs-surveys/popest/geographies/2018/all-geocodes-v2018.xlsx"

FIPS_2018_AREA_NAME_COL = "Area Name (including legal/statistical area description)"
FIPS_2018_CITY_COL = (
    "Consolidtated City Code (FIPS)"  # Misspelling is present in source
)
FIPS_2018_COUNTY_COL = "County Code (FIPS)"
FIPS_2018_COUNTY_SUBDIVISION_COL = "County Subdivision Code (FIPS)"
FIPS_2018_PLACE_COL = "Place Code (FIPS)"
FIPS_2018_STATE_COL = "State Code (FIPS)"
FIPS_2018_SUMMARY_COL = "Summary Level"

STATE_ABBREV_COL = "state_abbrev"
STATE_CODE_COL = "state_code"
COUNTY_CODE_COL = "county_code"
COUNTY_NAME_COL = "county_name"
FIPS_COL = "fips"


def generate_2018_fips_df(path: str = FIPS_2018_URL) -> pd.DataFrame:
    """Downloads raw FIPS data from the 2018 source and transforms to the proper format."""
    fips_df = pd.read_excel(path, dtype=str, engine="openpyxl", skiprows=range(4))

    fips_df = fips_df.drop([FIPS_2018_SUMMARY_COL], axis="columns")

    # Filter to counties only
    fips_df = fips_df[
        (
            # Filter out sub-county level fips
            (fips_df[FIPS_2018_COUNTY_SUBDIVISION_COL] == "00000")
            & (fips_df[FIPS_2018_PLACE_COL] == "00000")
            & (fips_df[FIPS_2018_CITY_COL] == "00000")
            # Filter out state fips
            & (fips_df[FIPS_2018_COUNTY_COL] != "000")
        )
    ]
    fips_df = fips_df.reset_index(drop=True)

    # Drop unnecessary columns
    fips_df = fips_df.drop(
        [FIPS_2018_COUNTY_SUBDIVISION_COL, FIPS_2018_PLACE_COL, FIPS_2018_CITY_COL],
        axis="columns",
    )

    # Rename columns
    fips_df = fips_df.rename(
        columns={
            FIPS_2018_STATE_COL: STATE_CODE_COL,
            FIPS_2018_COUNTY_COL: COUNTY_CODE_COL,
            FIPS_2018_AREA_NAME_COL: COUNTY_NAME_COL,
        }
    )

    # Add column with state abbreviation
    abbrev_col = fips_df.state_code.apply(
        lambda code: states.lookup(code, field="fips").abbr
    )
    fips_df.insert(loc=0, column=STATE_ABBREV_COL, value=abbrev_col)

    # Add columns with concatenated fips
    fips_df[FIPS_COL] = fips_df[STATE_CODE_COL] + fips_df[COUNTY_CODE_COL]

    return fips_df


FIPS_2014_URL = (
    "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"
)

FIPS_2014_CLASS_CODE_COL = "class_code"

FIPS_2014_FIELDS = [
    STATE_ABBREV_COL,
    STATE_CODE_COL,
    COUNTY_CODE_COL,
    COUNTY_NAME_COL,
    FIPS_2014_CLASS_CODE_COL,
]

FIPS_2014_DTYPES = {field: str for field in FIPS_2014_FIELDS}


def generate_2014_fips_df(path: str = FIPS_2014_URL) -> pd.DataFrame:
    """Downloads raw FIPS data, removes unnecessary fields, adds FIPS 6-4 code."""
    fips_df = pd.read_csv(path, names=FIPS_2014_FIELDS, dtype=FIPS_2014_DTYPES)

    fips_df = fips_df.drop(FIPS_2014_CLASS_CODE_COL, axis="columns")

    fips_df[FIPS_COL] = fips_df[STATE_CODE_COL] + fips_df[COUNTY_CODE_COL]

    return fips_df


def union_fips_dfs(fips_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Unions fips dataframes.

    If the same fips shows up in multiple dataframes, the row from the last dataframe wins.

    Expects dataframes to have the following columns:
    'fips', 'state_code', 'county_code', 'state_abbrev', 'county_name'
    """
    # Set the index to 'fips'
    for fips_df in fips_dfs:
        fips_df.index = fips_df[FIPS_COL]

    # Reverse, so that later dataframes take priority
    first, *others = reversed(fips_dfs)

    combined = first
    for to_combine in others:
        combined = combined.combine_first(to_combine)

    return combined.reset_index(drop=True)


def add_unknowns(fips_df: pd.DataFrame) -> pd.DataFrame:
    """Adds a catch-all FIPS 6-4 code of the form 'XX999' for each state, where XX is the state code."""
    unique_states = fips_df.drop_duplicates(STATE_CODE_COL)

    unknown_county_code = "999"
    unknown_codes = unique_states.apply(
        lambda row: {
            STATE_ABBREV_COL: row[STATE_ABBREV_COL],
            STATE_CODE_COL: row[STATE_CODE_COL],
            COUNTY_CODE_COL: unknown_county_code,
            COUNTY_NAME_COL: f"{row[STATE_ABBREV_COL]} Unknown",
            FIPS_COL: f"{row[STATE_CODE_COL]}{unknown_county_code}",
        },
        axis="columns",
    )
    unknown_codes = pd.DataFrame(list(unknown_codes))

    fips_df_with_unknowns = fips_df.append(
        unknown_codes[fips_df.columns], ignore_index=True
    )

    return fips_df_with_unknowns


def write_bigquery_csv(fips_df: pd.DataFrame) -> None:
    # Switch state and county code columns from fips to our standardized codes, e.g.
    # US_XX and US_XX_ALPHA
    fips_df[STATE_CODE_COL] = fips_df[STATE_ABBREV_COL].apply(
        fips.standardize_raw_state
    )
    fips_df[COUNTY_CODE_COL] = (
        fips_df[STATE_CODE_COL]
        + "_"
        + fips_df[COUNTY_NAME_COL].apply(
            lambda name: fips.sanitize_county_name(name).upper()
        )
    )

    if not fips_df[COUNTY_CODE_COL].is_unique:
        duplicate_rows = fips_df[fips_df[COUNTY_CODE_COL].duplicated(keep=False)]
        raise ValueError(f"Dataframe contains duplicate fips:\n{duplicate_rows}")

    # Select relevant columns in preferred order.
    fips_df = fips_df[[FIPS_COL, STATE_CODE_COL, COUNTY_CODE_COL, COUNTY_NAME_COL]]

    output_path = os.path.join(os.path.dirname(static_data.__file__), "county_fips.csv")
    fips_df.to_csv(output_path, index=False)


def write_common_csv(fips_df: pd.DataFrame) -> None:
    output_path = os.path.join(os.path.dirname(data_sets.__file__), "fips.csv")
    fips_df.to_csv(output_path, index=False)


def main() -> None:
    """Pulls fips from multiple sources, merges them, and stores the mappings locally.

    We use multiple sources because some county(-equivalents) have been added and
    removed over time. We want all of them including those that are no longer
    active."""
    # See https://www.census.gov/programs-surveys/geography/technical-documentation/county-changes.html

    # This source has codes as of 2014.
    fips_2014 = generate_2014_fips_df()
    # This source has codes as of 2018.
    fips_2018 = generate_2018_fips_df()

    fips_union = union_fips_dfs([fips_2014, fips_2018])

    write_bigquery_csv(fips_union.copy())

    fips_with_unknowns = add_unknowns(fips_union)
    write_common_csv(fips_with_unknowns)


if __name__ == "__main__":
    main()
