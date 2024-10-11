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
"""
The purpose of this file is to convert the CSV files 'county_sub_fips_geoid.csv',
'county_fips_geoid.csv', and 'state_fips_geoid.csv' to a dictionary, mapping
fips code to geoid.
"""
from typing import Dict

import pandas as pd


def get_fips_code_to_geoid() -> Dict[str, str]:
    """Reads incounty_sub_fips_geoid.csv and converts to a dictionary mapping FIPS code to GEOID"""
    subdivision_df = pd.read_csv(
        "./recidiviz/common/data_sets/county_sub_fips_geoid.csv",
        dtype={
            "county_sub_fips": str,
            "geoid": str,
        },
    )
    county_df = pd.read_csv(
        "./recidiviz/common/data_sets/county_fips_geoid.csv",
        dtype={
            "county_fips": str,
            "geoid": str,
        },
    )
    state_df = pd.read_csv(
        "./recidiviz/common/data_sets/state_fips_geoid.csv",
        dtype={
            "state_fips": str,
            "geoid": str,
        },
    )
    fips_code_to_geoid = {}
    for row in subdivision_df.itertuples(index=False, name=None):
        fips_code_to_geoid[row[0]] = row[1]
    for row in county_df.itertuples(index=False, name=None):
        fips_code_to_geoid[row[0]] = row[1]
    for row in state_df.itertuples(index=False, name=None):
        fips_code_to_geoid[row[0]] = row[1]

    return fips_code_to_geoid
