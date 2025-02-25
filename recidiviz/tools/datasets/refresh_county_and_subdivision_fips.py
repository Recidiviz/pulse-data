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
"""Generates a CSV file of FIPS codes for all states, counties, and county subdivisions.
This differs from the refresh_county_fips.py file in the following ways:
    - uses 2021 census geocodes (rather than 2014 and 2018 geocodes)
    - includes states, counties, and county subdivisions (rather than just counties)

Usage: `python -m recidiviz.tools.datasets.refresh_county_and_subdivision_fips`
"""
import os

import numpy as np
import pandas as pd
from us import states

from recidiviz.common import data_sets
from recidiviz.tools.datasets.refresh_county_fips import (
    COUNTY_CODE_COL,
    FIPS_2018_AREA_NAME_COL,
    FIPS_2018_CITY_COL,
    FIPS_2018_COUNTY_COL,
    FIPS_2018_COUNTY_SUBDIVISION_COL,
    FIPS_2018_PLACE_COL,
    FIPS_2018_STATE_COL,
    FIPS_2018_SUMMARY_COL,
    STATE_ABBREV_COL,
    STATE_CODE_COL,
)

FIPS_2021_URL = "https://www2.census.gov/programs-surveys/popest/geographies/2021/all-geocodes-v2021.xlsx"

AREA_NAME_COL = FIPS_2018_AREA_NAME_COL
CITY_COL = FIPS_2018_CITY_COL
COUNTY_COL = FIPS_2018_COUNTY_COL
COUNTY_SUBDIVISION_COL = FIPS_2018_COUNTY_SUBDIVISION_COL
PLACE_COL = FIPS_2018_PLACE_COL
STATE_COL = FIPS_2018_STATE_COL
SUMMARY_COL = FIPS_2018_SUMMARY_COL

COUNTY_SUBDIVISION_NAME_COL = "county_subdivision"
AREA_NAME_COL_NEW = "area_name"


def generate_fips_df(path: str = FIPS_2021_URL) -> pd.DataFrame:
    """Downloads raw FIPS data from the 2021 source and transforms to the proper format."""
    fips_df = pd.read_excel(path, dtype=str, engine="openpyxl", skiprows=range(4))

    fips_df = fips_df.drop([SUMMARY_COL], axis="columns")

    # Filter to states, counties and county subdivisions only
    fips_df = fips_df[
        (
            # Filter out sub-county-subdivision level fips
            (fips_df[PLACE_COL] == "00000")
            & (fips_df[CITY_COL] == "00000")
            # Filter out United States
            & (fips_df[AREA_NAME_COL] != "United States")
        )
    ]

    # Filter out D.C. county row (only keep D.C. state row)
    dc_county_index = fips_df[
        (
            (fips_df[AREA_NAME_COL] == "District of Columbia")
            & (fips_df[COUNTY_COL] == "001")
        )
    ].index
    fips_df = fips_df.drop(dc_county_index)
    fips_df = fips_df.reset_index(drop=True)

    # Drop unnecessary columns
    fips_df = fips_df.drop(
        [PLACE_COL, CITY_COL],
        axis="columns",
    )

    # Rename columns
    fips_df = fips_df.rename(
        columns={
            STATE_COL: STATE_CODE_COL,
            COUNTY_COL: COUNTY_CODE_COL,
            COUNTY_SUBDIVISION_COL: COUNTY_SUBDIVISION_NAME_COL,
            AREA_NAME_COL: AREA_NAME_COL_NEW,
        }
    )

    state_obj = fips_df.state_code.apply(lambda code: states.lookup(code, field="fips"))
    abbrev_col = state_obj.apply(lambda state: state.abbr)
    full_state_name_col = state_obj.apply(lambda state: state.name)
    # Add column with state abbreviation
    fips_df.insert(loc=0, column=STATE_ABBREV_COL, value=abbrev_col)
    # Add column with full state name
    fips_df.insert(loc=0, column="state_name", value=full_state_name_col)

    # Make new name, county_name, county_subdivision_name, and type columns

    # STATE
    # State name is just 'state' (same as area_name)
    fips_df.loc[fips_df["county_code"] == "000", "name"] = fips_df.loc[
        fips_df["county_code"] == "000", "area_name"
    ]
    # State county_name is None
    fips_df.loc[fips_df["county_code"] == "000", "county_name"] = np.nan
    # State county_sub_division_name is None
    fips_df.loc[fips_df["county_code"] == "000", "county_subdivision_name"] = np.nan
    # State type is 'state'
    fips_df.loc[fips_df["county_code"] == "000", "type"] = "state"

    # COUNTY
    # County name is 'county, state' (same as area_name, state_name)
    fips_df.loc[
        (fips_df["county_code"] != "000") & (fips_df["county_subdivision"] == "00000"),
        "name",
    ] = (
        fips_df.loc[
            (fips_df["county_code"] != "000")
            & (fips_df["county_subdivision"] == "00000"),
            "area_name",
        ]
        + ", "
        + fips_df.loc[
            (fips_df["county_code"] != "000")
            & (fips_df["county_subdivision"] == "00000"),
            "state_name",
        ]
    )
    # County county_name is the same as area_name
    fips_df.loc[
        (fips_df["county_code"] != "000") & (fips_df["county_subdivision"] == "00000"),
        "county_name",
    ] = fips_df.loc[
        (fips_df["county_code"] != "000") & (fips_df["county_subdivision"] == "00000"),
        "area_name",
    ]
    # County county_subdivision_name is None
    fips_df.loc[
        (fips_df["county_code"] != "000") & (fips_df["county_subdivision"] == "00000"),
        "county_subdivision_name",
    ] = np.nan
    # County type is 'county'
    fips_df.loc[
        (fips_df["county_code"] != "000") & (fips_df["county_subdivision"] == "00000"),
        "type",
    ] = "county"

    # COUNTY SUBDIVISIONS
    # County subdivision name is 'county_subdivision, county, state'
    # County subdivision county_name is the county
    # County subdivision county_subdivision_name is the area_name
    # County subdivision type is 'county_subdivision'
    for idx, row in fips_df.iterrows():
        if row[4] != "00000":
            state_code = row[2]
            county_code = row[3]
            county = fips_df[
                (fips_df["state_code"] == state_code)
                & (fips_df["county_code"] == county_code)
                & (fips_df["county_subdivision"] == "00000")
            ].iloc[0, 5]
            fips_df.iloc[idx, 6] = f"{row[5]}, {county}, {row[0]}"
            fips_df.iloc[idx, 7] = county
            fips_df.iloc[idx, 8] = row[5]
            fips_df.iloc[idx, 9] = "county_subdivision"

    # D.C. and Puerto Rico are special cases
    fips_df.loc[fips_df["state_name"] == "District of Columbia", "type"] = "district"
    fips_df.loc[fips_df["state_name"] == "Puerto Rico", "type"] = "territory"

    # Create unique id (state_code + county_code + county_subdivision)
    fips_df["id"] = (
        fips_df["state_code"] + fips_df["county_code"] + fips_df["county_subdivision"]
    )

    # Drop area_name, state_code, county_code, county_subdivision_code
    fips_df = fips_df.drop(
        ["state_code", "county_code", "area_name", "county_subdivision"],
        axis="columns",
    )

    return fips_df


def write_common_csv(fips_df: pd.DataFrame) -> None:
    output_path = os.path.join(
        os.path.dirname(data_sets.__file__), "fips_with_county_subdivisions.csv"
    )
    fips_df.to_csv(output_path, index=False)


def main() -> None:
    """
    Call function to download raw FIPS data from the 2021 source and transform to the
    proper format. Once we have a dataframe in the proper format, write to csv (will
    later be converted to json for the frontend).

    The purpose of this is to generate a csv of all jurisdcitions (states, counties, and
    county subdivisions).
    """

    fips_df = generate_fips_df()
    write_common_csv(fips_df)


if __name__ == "__main__":
    main()
