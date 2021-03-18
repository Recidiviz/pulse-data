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
"""Fetches resident population data from census.gov and writes it locally"""

import io
import os
from typing import AnyStr, IO, Union
import pandas as pd
import requests
from us import states

from recidiviz.common import fips
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets import static_data

COUNTY_POPULATIONS_2010_2019 = "https://www2.census.gov/programs-surveys/popest/tables/2010-2019/counties/totals/co-est2019-annres.xlsx"

TEMP_LOCATION_COL = "location"
TEMP_COUNTY_NAME_COL = "county_name"
TEMP_STATE_NAME_COL = "state_name"
TEMP_STATE_CODE_COL = "state_code"

FIPS_COL = "fips"
YEAR_COL = "year"
POPULATION_COL = "population"


def add_fips_to_state_df(df: pd.DataFrame) -> pd.DataFrame:
    state_code = StateCode(df.name)
    df = df.copy()
    return fips.add_column_to_df(df, df[TEMP_COUNTY_NAME_COL], state_code.get_state())


def fetch_population_df(path: str) -> pd.DataFrame:
    """Fetches county population data."""
    return pd.read_excel(path, engine="openpyxl", skiprows=range(3), skipfooter=6)


def transform_population_df(pops_df: pd.DataFrame) -> pd.DataFrame:
    """Transforms the population data int to a more usable format.

    Adds fips information and unpivots the year columns into a single year column."""
    # First column is missing name, name it "location"
    pops_df = pops_df.rename(columns={pops_df.columns[0]: TEMP_LOCATION_COL})

    # Just keep the 2010 to 2019 estimated columns
    pops_df = pops_df.drop(["Census", "Estimates Base"], axis="columns")

    # County rows start with ".", remove it
    pops_df = pops_df[pops_df[TEMP_LOCATION_COL].str.startswith(".")]
    pops_df[TEMP_LOCATION_COL] = pops_df[TEMP_LOCATION_COL].str[1:]

    # Location is of form "county name, state name", pull these into their own columns
    location_col = pops_df.pop(TEMP_LOCATION_COL).str.split(",", expand=True)
    pops_df[TEMP_COUNTY_NAME_COL] = location_col[0].str.strip()
    pops_df[TEMP_STATE_NAME_COL] = location_col[1].str.strip()

    # Get the state code to group by
    pops_df[TEMP_STATE_CODE_COL] = pops_df[TEMP_STATE_NAME_COL].apply(
        lambda state_name: "US_" + states.lookup(state_name, field="name").abbr
    )

    # Group by state, add fips to each row in each group
    pops_df = pops_df.groupby([TEMP_STATE_CODE_COL]).apply(add_fips_to_state_df)

    # Only keep fips id column
    if not pops_df[FIPS_COL].is_unique:
        duplicate_rows = pops_df[pops_df[FIPS_COL].duplicated(keep=False)]
        raise ValueError(
            f"Dataframe contains duplicate fips:\n{duplicate_rows}",
        )
    pops_df = pops_df.drop(
        [TEMP_COUNTY_NAME_COL, TEMP_STATE_NAME_COL, TEMP_STATE_CODE_COL], axis="columns"
    )

    # Unpivot 2010 to 2019 columns into single year column
    pops_df = pops_df.melt(
        id_vars=[FIPS_COL], var_name=YEAR_COL, value_name=POPULATION_COL
    )
    pops_df[YEAR_COL] = pops_df[YEAR_COL].astype(int)

    return pops_df


CDC_NOTES_COL = "Notes"
CDC_YEAR_COL = "Yearly July 1st Estimates"
CDC_YEAR_CODE_COL = "Yearly July 1st Estimates Code"
CDC_COUNTY_COL = "County"
CDC_COUNTY_CODE_COL = "County Code"
CDC_POPULATION_COL = "Population"


def fetch_adult_population_csv() -> pd.DataFrame:
    """Fetchs a csv continaing populations of adults (15 to 64) per county"""
    response = requests.post(
        "https://wonder.cdc.gov/controller/datarequest/D163",
        data=[
            ("saved_id", ""),
            ("dataset_code", "D163"),
            ("dataset_label", "Bridged-Race Population Estimates 1990-2019"),
            ("dataset_vintage_latest", "Bridged Race"),
            ("stage", "request"),
            ("O_javascript", "on"),
            ("M_1", "D163.M1"),
            ("B_1", "D163.V1"),
            ("B_2", "D163.V2-level2"),
            ("B_3", "*None*"),
            ("B_4", "*None*"),
            ("B_5", "*None*"),
            ("O_title", ""),
            ("O_location", "D163.V2"),
            ("finder-stage-D163.V2", "codeset"),
            ("O_V2_fmode", "freg"),
            ("V_D163.V2", ""),
            ("F_D163.V2", "*All*"),
            ("I_D163.V2", "*All* (The United States)"),
            ("finder-stage-D163.V9", "codeset"),
            ("O_V9_fmode", "freg"),
            ("V_D163.V9", ""),
            ("F_D163.V9", "*All*"),
            ("I_D163.V9", "*All* (The United States)"),
            ("O_age", "D163.V8"),
            ("V_D163.V3", "*All*"),
            # Include all persons from 15-64 years old.
            ("V_D163.V8", "15-19"),
            ("V_D163.V8", "20-24"),
            ("V_D163.V8", "25-29"),
            ("V_D163.V8", "30-34"),
            ("V_D163.V8", "35-39"),
            ("V_D163.V8", "40-44"),
            ("V_D163.V8", "45-49"),
            ("V_D163.V8", "50-54"),
            ("V_D163.V8", "55-59"),
            ("V_D163.V8", "60-64"),
            ("V_D163.V4", "*All*"),
            ("V_D163.V6", "*All*"),
            # Fetching all years creates too large of a response, so we only fetch 2010
            # to 2019. If more years are needed they can be requested separately and
            # concatenated with these results.
            ("V_D163.V1", "2010"),
            ("V_D163.V1", "2011"),
            ("V_D163.V1", "2012"),
            ("V_D163.V1", "2013"),
            ("V_D163.V1", "2014"),
            ("V_D163.V1", "2015"),
            ("V_D163.V1", "2016"),
            ("V_D163.V1", "2017"),
            ("V_D163.V1", "2018"),
            ("V_D163.V1", "2019"),
            ("V_D163.V5", "*All*"),
            ("O_change_action-Send-Export Results", "Export Results"),
            ("O_show_zeros", "true"),
            ("O_precision", "0"),
            ("O_timeout", "600"),
            ("O_datatable", "default"),
            ("action-Send", "Send"),
        ],
    )
    return io.StringIO(response.text)


def transform_adult_population_df(
    filepath_or_buffer: Union[str, IO[AnyStr]]
) -> pd.DataFrame:
    """Transforms the csv into a dataframe in the population format"""
    df = pd.read_csv(
        filepath_or_buffer,
        sep="\t",
        # Read all as str to avoid N/A issues
        dtype={
            CDC_NOTES_COL: str,
            CDC_YEAR_COL: str,
            CDC_YEAR_CODE_COL: str,
            CDC_COUNTY_COL: str,
            CDC_COUNTY_CODE_COL: str,
            CDC_POPULATION_COL: str,
        },
    )

    # Remove notes from the bottom
    notes_start = df[~df[CDC_NOTES_COL].isna()].iloc[0]
    df = df.iloc[: notes_start.name]

    # Omit missing values
    df = df[df[CDC_POPULATION_COL] != "Missing"]

    df = df[[CDC_COUNTY_CODE_COL, CDC_YEAR_COL, CDC_POPULATION_COL]]
    df = df.astype({CDC_YEAR_COL: int, CDC_POPULATION_COL: int})
    df = df.rename(
        {
            CDC_YEAR_COL: YEAR_COL,
            CDC_COUNTY_CODE_COL: FIPS_COL,
            CDC_POPULATION_COL: POPULATION_COL,
        },
        axis="columns",
    )

    return df


def make_output_path(name: str) -> str:
    return os.path.join(os.path.dirname(static_data.__file__), name)


def main() -> None:
    """Fetches county population data and writes it locally

    It transforms the location to add fips information, and unpivots the year columns
    into a single year column."""
    pops_df = fetch_population_df(COUNTY_POPULATIONS_2010_2019)
    pops_df = transform_population_df(pops_df)
    pops_df.to_csv(make_output_path("county_resident_populations.csv"), index=False)

    adult_pops_csv = fetch_adult_population_csv()
    adult_pops_df = transform_adult_population_df(adult_pops_csv)
    adult_pops_df.to_csv(
        make_output_path("county_resident_adult_populations.csv"), index=False
    )


if __name__ == "__main__":
    main()
