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
from typing import Any, Dict, List

import pandas as pd


def get_all_jurisdictions() -> List[Dict[str, Any]]:
    """Reads in fips_with_county_subdivisions.csv and converts to a list of dictionaries
    of all 24,329 jurisdictions.
    """
    jurisdictions_df = pd.read_csv(
        "./recidiviz/common/data_sets/fips_with_county_subdivisions.csv",
        dtype={
            "state_name": str,
            "state_abbrev": str,
            "state_code": str,
            "county_code": str,
            "county_subdivision": str,
            "area_name": str,
            "fips": str,
        },
    )
    jurisdiction_lst = []
    for _, jurisdiction in jurisdictions_df.iterrows():
        # unique id is state_code + county_code + county_subdivision
        jurisdiction_json = {
            "id": jurisdiction[2] + jurisdiction[3] + jurisdiction[4],
            "area_name": jurisdiction[5],
            "state_name": jurisdiction[0],
            "state_abbrev": jurisdiction[1],
            "state_code": jurisdiction[2],
            "fips": jurisdiction[6],
        }
        # D.C. special case (1)
        if jurisdiction[0] == "District of Columbia":
            state_json = {
                "county_name": None,
                "county_code": None,
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "district",
            }
            jurisdiction_json.update(state_json)
        # Puerto Rico special case (1)
        elif jurisdiction[3] == "000" and jurisdiction[0] == "Puerto Rico":
            state_json = {
                "county_name": None,
                "county_code": None,
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "territory",
            }
            jurisdiction_json.update(state_json)
        # states (50)
        elif jurisdiction[3] == "000":
            state_json = {
                "county_name": None,
                "county_code": None,
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "state",
            }
            jurisdiction_json.update(state_json)
        # counties (3220 counties)
        elif jurisdiction[4] == "00000":
            county_json = {
                "county_name": jurisdiction[5],
                "county_code": jurisdiction[3],
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "county",
            }
            jurisdiction_json.update(county_json)
        # county subdivisions (21057 county subdivisions)
        else:
            county_subdiv_json = {
                "county_name": None,
                "county_code": None,
                "county_subdivision": jurisdiction[5],
                "county_subdivision_code": jurisdiction[4],
                "type": "county_subdivision",
            }
            jurisdiction_json.update(county_subdiv_json)
        jurisdiction_lst.append(jurisdiction_json)
    return jurisdiction_lst
