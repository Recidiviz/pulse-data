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

Usage: `python -m recidiviz.ingest.aggregate.data_sets.generate_fips`
"""
import os

import pandas as pd

FIPS_URL = "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"

FIPS_FIELDS = ["state_abbrev", "state_code", "county_code", "county_name", "class_code"]

FIPS_DTYPES = {field: str for field in FIPS_FIELDS}


def generate_fips_df() -> pd.DataFrame:
    """Downloads raw FIPS data, removes unncessary fields, adds FIPS 6-4 code.

    Additionally, a catch-all FIPS 6-4 code of the form 'XX999',
    where XX is the state code, is generated for each state.
    """
    fips_df = pd.read_csv(FIPS_URL, names=FIPS_FIELDS, dtype=FIPS_DTYPES)

    fips_df = fips_df.drop("class_code", axis="columns")

    fips_df["fips"] = fips_df["state_code"] + fips_df["county_code"]

    unique_states = fips_df.drop_duplicates("state_code")

    unknown_county_code = "999"
    unknown_codes = unique_states.apply(
        lambda row: {
            "state_abbrev": row["state_abbrev"],
            "state_code": row["state_code"],
            "county_code": unknown_county_code,
            "county_name": "{} Unknown".format(row["state_abbrev"]),
            "fips": "{}{}".format(row["state_code"], unknown_county_code),
        },
        axis="columns",
    )
    unknown_codes = pd.DataFrame(list(unknown_codes))

    fips_df_with_unknowns = fips_df.append(unknown_codes[fips_df.columns])

    return fips_df_with_unknowns


if __name__ == "__main__":
    FIPS = generate_fips_df()

    OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "fips.csv")
    FIPS.to_csv(OUTPUT_PATH, index=False)
