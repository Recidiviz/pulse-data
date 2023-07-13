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
"""This class implements tests for the Justice Counts AgencyInterface."""

from sqlalchemy.exc import IntegrityError

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import JusticeCountsDatabaseTestCase


class TestAgencyInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the AgencyInterface."""

    def setUp(self) -> None:
        super().setUp()
        with SessionFactory.using_database(self.database_key) as session:
            user_id = UserAccountInterface.create_or_update_user(
                session=session, auth0_user_id="test_auth0_user"
            ).id
            AgencyInterface.create_agency(
                session=session,
                name="Agency Alpha",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                user_account_id=user_id,
            )
            AgencyInterface.create_agency(
                session=session,
                name="Beta Initiative",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ak",
                fips_county_code="us_ak_anchorage",
                user_account_id=user_id,
            )

    def test_get_agencies(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            agency_1 = AgencyInterface.get_agency_by_name(
                session=session, name="Agency Alpha"
            )
            self.assertEqual(agency_1.name, "Agency Alpha")

            allAgencies = AgencyInterface.get_agencies(session=session)
            self.assertEqual(
                {a.name for a in allAgencies}, {"Agency Alpha", "Beta Initiative"}
            )

            allAgencyIds = AgencyInterface.get_agency_ids(session=session)
            self.assertEqual(sorted(allAgencyIds), sorted(a.id for a in allAgencies))

            agenciesByName = AgencyInterface.get_agencies_by_name(
                session=session, names=["Agency Alpha", "Beta Initiative"]
            )
            self.assertEqual(
                {a.name for a in agenciesByName}, {"Agency Alpha", "Beta Initiative"}
            )

            agenciesById = AgencyInterface.get_agencies_by_id(
                session=session,
                agency_ids=[agency.id for agency in agenciesByName],
            )
            self.assertEqual(
                {a.name for a in agenciesById}, {"Agency Alpha", "Beta Initiative"}
            )

            # Raise error if one of the agencies not found
            with self.assertRaisesRegex(
                ValueError, "Could not find the following agencies: {'Agency Beta'}"
            ):
                AgencyInterface.get_agencies_by_name(
                    session=session,
                    names=["Agency Alpha", "Agency Beta"],
                )

    def test_create_agency(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id="test_auth0_user",
                email="test@email.com",
            )
            gamma_agency = AgencyInterface.create_agency(
                session=session,
                name="Agency Gamma",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                user_account_id=user.id,
            )
            delta_agency = AgencyInterface.create_agency(
                session=session,
                name="Agency Delta",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ak",
                fips_county_code="us_ak_anchorage",
                user_account_id=user.id,
            )
            UserAccountInterface.add_or_update_user_agency_association(
                session=session, user=user, agencies=[delta_agency, gamma_agency]
            )
        session.add_all([user, gamma_agency, delta_agency])
        session.flush()
        agencies = AgencyInterface.get_agencies(session=session)
        self.assertEqual(
            {a.name for a in agencies},
            {"Agency Alpha", "Beta Initiative", "Agency Gamma", "Agency Delta"},
        )

        # Test to_json
        self.assertEqual(
            {a.to_json()["state"] for a in agencies},
            {"California", "Alaska"},
        )

        self.assertEqual(
            gamma_agency.to_json()["team"][0]["name"],
            user.name,
        )
        self.assertEqual(
            gamma_agency.to_json()["team"][0]["auth0_user_id"],
            user.auth0_user_id,
        )

        # Raise error if agency of that name already exists
        with self.assertRaisesRegex(
            IntegrityError,
            "psycopg2.errors.UniqueViolation",
        ):
            AgencyInterface.create_agency(
                session=session,
                name="Agency Alpha",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                user_account_id=user.id,
            )

    def test_get_child_agencies(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id="test_auth0_user",
                email="test@email.com",
            )
            super_agency = AgencyInterface.create_agency(
                session=session,
                name="Super Agency",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                user_account_id=user.id,
                is_superagency=True,
            )
            child_agency = AgencyInterface.create_agency(
                session=session,
                name="Child Agency",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                user_account_id=user.id,
                super_agency_id=super_agency.id,
            )
            UserAccountInterface.add_or_update_user_agency_association(
                session=session, user=user, agencies=[super_agency, child_agency]
            )
        session.add_all([user, super_agency, child_agency])
        session.flush()
        # The super agency will have the child agency in its list of child agencies.
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=session, agency=super_agency
        )
        self.assertEqual(child_agencies, [child_agency])

        # The child agency will have no agencies in its list of child agencies.
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=session, agency=child_agency
        )
        self.assertEqual(child_agencies, [])
