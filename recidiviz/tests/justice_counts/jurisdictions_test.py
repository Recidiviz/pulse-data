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

from recidiviz.tests.justice_counts.utils import JusticeCountsSchemaTestObjects
from recidiviz.tools.datasets.jurisdictions import get_all_jurisdictions


class TestJurisdictions(TestCase):
    """Test that get_all_jurisdictions() converts csv to json as expected"""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.maxDiff = None

    def test_jurisdictions(self) -> None:
        all_jurisdictions_lst = get_all_jurisdictions()
        jurisdictions_csv = pd.read_csv(
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
        self.assertEqual(len(all_jurisdictions_lst), len(jurisdictions_csv))

        # check state record
        alabama_state = all_jurisdictions_lst[0]
        self.assertEqual(
            alabama_state,
            {
                "id": "0100000000",
                "area_name": "Alabama",
                "state_name": "Alabama",
                "state_abbrev": "AL",
                "state_code": "01",
                "fips": "01000",
                "county_name": None,
                "county_code": None,
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "state",
            },
        )
        # check unique ids
        ids = set(jurisdiction["id"] for jurisdiction in all_jurisdictions_lst)
        self.assertEqual(len(ids), len(all_jurisdictions_lst))
        # check county record
        alabama_county = all_jurisdictions_lst[1]
        self.assertEqual(
            alabama_county,
            {
                "id": "0100100000",
                "area_name": "Autauga County",
                "state_name": "Alabama",
                "state_abbrev": "AL",
                "state_code": "01",
                "fips": "01001",
                "county_name": "Autauga County",
                "county_code": "001",
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "county",
            },
        )
        # check county subdivision record
        ct_county_subdivision = all_jurisdictions_lst[324]
        self.assertEqual(
            ct_county_subdivision,
            {
                "id": "0900104720",
                "area_name": "Bethel town",
                "state_name": "Connecticut",
                "state_abbrev": "CT",
                "state_code": "09",
                "fips": "09001",
                "county_name": None,
                "county_code": None,
                "county_subdivision": "Bethel town",
                "county_subdivision_code": "04720",
                "type": "county_subdivision",
            },
        )
        # check D.C.
        dc = all_jurisdictions_lst[497]
        self.assertEqual(
            dc,
            {
                "id": "1100000000",
                "area_name": "District of Columbia",
                "state_name": "District of Columbia",
                "state_abbrev": "DC",
                "state_code": "11",
                "fips": "11000",
                "county_name": None,
                "county_code": None,
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "district",
            },
        )

        # check Puerto Rico
        pr = all_jurisdictions_lst[24250]
        self.assertEqual(
            pr,
            {
                "id": "7200000000",
                "area_name": "Puerto Rico",
                "state_name": "Puerto Rico",
                "state_abbrev": "PR",
                "state_code": "72",
                "fips": "72000",
                "county_name": None,
                "county_code": None,
                "county_subdivision": None,
                "county_subdivision_code": None,
                "type": "territory",
            },
        )
