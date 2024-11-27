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
import pandas as pd

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.agency_jurisdictions import AgencyJurisdictionInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.datasets.jurisdictions import (
    get_fips_code_to_jurisdiction_metadata,
)


class TestJurisdictions(JusticeCountsDatabaseTestCase):
    """Test that get_fips_code_to_jurisdiction_metadata() converts csv to json as expected"""

    def setUp(self) -> None:
        super().setUp()
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

    def test_get_agency_population(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency_A = AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Alpha",
                systems=[schema.System.PRISONS],
                state_code="us_ny",
                fips_county_code=None,
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            session.commit()
            session.refresh(agency_A)

            race_eth_categories = [
                "American Indian or Alaska Native",
                "Asian",
                "Black",
                "Hispanic or Latino",
                "More than one race",
                "Native Hawaiian or Pacific Islander",
                "White",
            ]

            bio_sex_categories = ["Male", "Female"]
            session.add(
                schema.AgencyJurisdiction(
                    source_id=agency_A.id,
                    geoid=1234,
                    membership=schema.AgencyJurisdictionType.INCLUDE.value,
                )
            )
            for race_eth in race_eth_categories:
                session.add(
                    schema.JurisdictionPopulation(
                        geoid=1234, year=2022, race_ethnicity=race_eth, population=10
                    )
                )

            for sex in bio_sex_categories:
                session.add(
                    schema.JurisdictionPopulation(
                        geoid=1234, year=2022, sex=sex, population=10
                    )
                )
            session.commit()

            populations_dict = AgencyJurisdictionInterface.get_agency_population(
                agency=agency_A, session=session
            )
            self.assertEqual(
                set(populations_dict.keys()), {"biological_sex", "race_and_ethnicity"}
            )

            self.assertEqual(
                set(populations_dict["biological_sex"].keys()), {"Female", "Male"}
            )

            self.assertEqual(
                set(populations_dict["race_and_ethnicity"].keys()),
                {
                    "American Indian or Alaska Native",
                    "Asian",
                    "Black",
                    "Hispanic or Latino",
                    "More than one race",
                    "Native Hawaiian or Pacific Islander",
                    "White",
                },
            )

            self.assertEqual(
                populations_dict["biological_sex"]["Female"],
                {
                    2022: 10,
                },
            )

            self.assertEqual(
                populations_dict["race_and_ethnicity"]["Hispanic or Latino"],
                {
                    2022: 10,
                },
            )
