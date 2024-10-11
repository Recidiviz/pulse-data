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
"""The purpose of this file is to test that the conversion from CSV to json representation
of all jurisdictions works as expected."""
from unittest import TestCase

import pandas as pd

from recidiviz.tests.justice_counts.utils.utils import JusticeCountsSchemaTestObjects
from recidiviz.tools.datasets.jurisdictions import (
    get_fips_code_to_jurisdiction_metadata,
)


class TestJurisdictions(TestCase):
    """Test that get_fips_code_to_jurisdiction_metadata() converts csv to json as expected"""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.maxDiff = None

    def test_jurisdictions(self) -> None:
        all_jurisdictions = get_fips_code_to_jurisdiction_metadata()
        jurisdictions_csv = pd.read_csv(
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
        self.assertEqual(len(all_jurisdictions), len(jurisdictions_csv))

        # check state record
        alabama_state = all_jurisdictions["0100000000"]
        self.assertEqual(
            alabama_state,
            {
                "id": "0100000000",
                "state_abbrev": "AL",
                "state_name": "Alabama",
                "county_name": None,
                "county_subdivision_name": None,
                "name": "Alabama",
                "type": "state",
            },
        )
        # check unique ids
        ids = set(all_jurisdictions.keys())
        self.assertEqual(len(ids), len(all_jurisdictions))
        # check county record
        alabama_county = all_jurisdictions["0100100000"]
        self.assertEqual(
            alabama_county,
            {
                "id": "0100100000",
                "state_abbrev": "AL",
                "state_name": "Alabama",
                "county_name": "Autauga County",
                "county_subdivision_name": None,
                "name": "Autauga County, Alabama",
                "type": "county",
            },
        )
        # check county subdivision record
        ct_county_subdivision = all_jurisdictions["0900104720"]
        self.assertEqual(
            ct_county_subdivision,
            {
                "id": "0900104720",
                "state_abbrev": "CT",
                "state_name": "Connecticut",
                "county_name": "Fairfield County",
                "county_subdivision_name": "Bethel town",
                "name": "Bethel town, Fairfield County, Connecticut",
                "type": "county_subdivision",
            },
        )
        # check D.C.
        dc = all_jurisdictions["1100000000"]
        self.assertEqual(
            dc,
            {
                "id": "1100000000",
                "state_abbrev": "DC",
                "state_name": "District of Columbia",
                "county_name": None,
                "county_subdivision_name": None,
                "name": "District of Columbia",
                "type": "district",
            },
        )

        # check Puerto Rico
        pr = all_jurisdictions["7200000000"]
        self.assertEqual(
            pr,
            {
                "id": "7200000000",
                "state_abbrev": "PR",
                "state_name": "Puerto Rico",
                "county_name": None,
                "county_subdivision_name": None,
                "name": "Puerto Rico",
                "type": "territory",
            },
        )
