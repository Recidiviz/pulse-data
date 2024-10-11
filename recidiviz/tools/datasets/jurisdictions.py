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
"""The purpose of this file is to convert the CSV file 'fips_with_county_subdivisions'
to a list of dictionaries, which will later be converted to a json representation for the frontend."""
from typing import Any, Dict

import pandas as pd


def get_fips_code_to_jurisdiction_metadata() -> Dict[str, Dict[str, Any]]:
    """Reads in fips_with_county_subdivisions.csv and converts to a dictionary of dictionaries
    of all 24,329 jurisdictions.
    """
    jurisdictions_df = pd.read_csv(
        "./recidiviz/common/data_sets/fips_with_county_subdivisions.csv",
        dtype={
            "state_name": str,
            "state_abbrev": str,
            "name": str,
            "county_name": str,
            "county_subdivision_name": str,
            "type": str,
            "id": str,
        },
    )
    all_jurisdictions = {}
    for jurisdiction in jurisdictions_df.itertuples(index=False, name=None):
        all_jurisdictions[jurisdiction[6]] = {
            "id": jurisdiction[6],
            "state_abbrev": jurisdiction[1],
            "state_name": jurisdiction[0],
            "county_name": (
                jurisdiction[3] if pd.isna(jurisdiction[3]) is False else None
            ),
            "county_subdivision_name": (
                jurisdiction[4] if pd.isna(jurisdiction[4]) is False else None
            ),
            "name": jurisdiction[2],
            "type": jurisdiction[5],
        }
    return all_jurisdictions
