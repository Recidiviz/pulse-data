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
"""This class implements tests for the Justice Counts UserInterface."""

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import JusticeCountsDatabaseTestCase


class TestUserInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the UserAccountInterface."""

    def setUp(self) -> None:
        super().setUp()
        with SessionFactory.using_database(self.database_key) as session:
            for agency in [
                {
                    "name": "Agency Alpha",
                    "system": "LAW_ENFORCEMENT",
                    "state_code": "us_ca",
                    "fips_county_code": "us_ca_sacramento",
                },
                {
                    "name": "Agency Beta",
                    "system": "LAW_ENFORCEMENT",
                    "state_code": "us_ak",
                    "fips_county_code": "us_ak_anchorage",
                },
                {
                    "name": "Agency Gamma",
                    "system": "LAW_ENFORCEMENT",
                    "state_code": "us_ar",
                    "fips_county_code": "us_ar_lee",
                },
            ]:
                AgencyInterface.create_agency(
                    session=session,
                    name=agency["name"],
                    system=agency["system"],
                    state_code=agency["state_code"],
                    fips_county_code=agency["fips_county_code"],
                )

            UserAccountInterface.create_or_update_user(
                session=session,
                email_address="user@gmail.com",
                name="User",
                auth0_user_id="id0",
            )
            for agency_name in ["Agency Alpha", "Agency Beta"]:
                UserAccountInterface.add_agency_to_user(
                    session=session,
                    email_address="user@gmail.com",
                    agency_name=agency_name,
                )

    def test_create_user(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            # Can create user with just email address
            UserAccountInterface.create_user(
                session=session,
                email_address="user1@test.com",
            )

            # can create user with auth0_user_id
            UserAccountInterface.create_user(
                session=session,
                email_address="user2@test.com",
                auth0_user_id="auth0_id",
            )

            # can create user with agency_ids
            agencies = AgencyInterface.get_agencies_by_name(
                session=session, names=["Agency Beta", "Agency Gamma"]
            )

            UserAccountInterface.create_user(
                session=session,
                email_address="user3@test.com",
                agency_ids=[agency.id for agency in agencies],
            )

            user1 = UserAccountInterface.get_user_by_email_address(
                session=session, email_address="user1@test.com"
            )
            self.assertEqual(user1.email_address, "user1@test.com")

            user2 = UserAccountInterface.get_user_by_email_address(
                session=session, email_address="user2@test.com"
            )
            self.assertEqual(user2.auth0_user_id, "auth0_id")

            user3 = UserAccountInterface.get_user_by_email_address(
                session=session, email_address="user3@test.com"
            )
            self.assertEqual(user3.agencies, agencies)

            # Cannot create user with the same email address
            with self.assertRaisesRegex(
                IntegrityError, "psycopg2.errors.UniqueViolation"
            ):
                UserAccountInterface.create_user(
                    session=session,
                    email_address="user1@test.com",
                )

            session.rollback()

            # Cannot create user with invalid email address
            with self.assertRaisesRegex(ValueError, "Invalid email address"):
                UserAccountInterface.create_user(
                    session=session,
                    email_address="xyz",
                )

    def test_create_or_update_user(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            # Can create user with just email address
            UserAccountInterface.create_or_update_user(
                session=session,
                email_address="user2@gmail.com",
            )

            # can update user with auth0_user_id
            UserAccountInterface.create_or_update_user(
                session=session,
                email_address="user2@gmail.com",
                auth0_user_id="auth0_id",
            )

            # can update user with agency_ids
            agencies = AgencyInterface.get_agencies_by_name(
                session=session, names=["Agency Beta", "Agency Gamma"]
            )

            UserAccountInterface.create_or_update_user(
                session=session,
                email_address="user2@gmail.com",
                agency_ids=[agency.id for agency in agencies],
            )

            user = UserAccountInterface.get_user_by_email_address(
                session=session, email_address="user2@gmail.com"
            )
            self.assertEqual(user.auth0_user_id, "auth0_id")
            self.assertEqual(user.agencies, agencies)

            # Cannot create user with invalid email address
            with self.assertRaisesRegex(ValueError, "Invalid email address"):
                UserAccountInterface.create_or_update_user(
                    session=session,
                    email_address="xyz",
                )

    def test_get_user_by_email_address(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.get_user_by_email_address(
                session=session, email_address="user@gmail.com"
            )
            self.assertEqual(user.auth0_user_id, "id0")
            self.assertEqual(user.name, "User")
            self.assertEqual(
                {a.name for a in user.agencies}, {"Agency Alpha", "Agency Beta"}
            )

            # Raise error if no user found
            with self.assertRaises(NoResultFound):
                UserAccountInterface.get_user_by_email_address(
                    session=session, email_address="abc"
                )

    def get_user_by_auth0_user_id(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=session, auth0_user_id="id0"
            )
            self.assertEqual(user.email_address, "user@gmail.com")
            self.assertEqual(user.name, "User")
            self.assertEqual(
                {a.name for a in user.agencies}, {"Agency Alpha", "Agency Beta"}
            )

            # Raise error if no user found
            with self.assertRaises(NoResultFound):
                UserAccountInterface.get_user_by_auth0_user_id(
                    session=session, auth0_user_id="blah"
                )

    def test_add_agency_to_user(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            UserAccountInterface.add_agency_to_user(
                session=session,
                email_address="user@gmail.com",
                agency_name="Agency Gamma",
            )
            user = UserAccountInterface.get_user_by_email_address(
                session=session, email_address="user@gmail.com"
            )
            self.assertIn("Agency Gamma", {a.name for a in user.agencies})

            # Raise error if agency does not exist
            with self.assertRaises(NoResultFound):
                UserAccountInterface.add_agency_to_user(
                    session=session,
                    email_address="user@gmail.com",
                    agency_name="Agency Delta",
                )
