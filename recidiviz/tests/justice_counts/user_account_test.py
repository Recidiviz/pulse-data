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

from sqlalchemy.orm.exc import NoResultFound

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils import JusticeCountsDatabaseTestCase


class TestUserInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the UserAccountInterface."""

    def setUp(self) -> None:
        super().setUp()
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.create_or_update_user(
                session=session, auth0_user_id="test_auth0_user"
            )
            AgencyInterface.create_agency(
                session=session,
                name="Agency Alpha",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                user_account_id=user.id,
            )
            AgencyInterface.create_agency(
                session=session,
                name="Agency Beta",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ak",
                fips_county_code="us_ak_anchorage",
                user_account_id=user.id,
            )
            AgencyInterface.create_agency(
                session=session,
                name="Agency Gamma",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ar",
                fips_county_code="us_ar_lee",
                user_account_id=user.id,
            )

            UserAccountInterface.create_or_update_user(
                session=session,
                name="User",
                auth0_user_id="id0",
            )

    def test_create_or_update_user(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            # Can create user with auth0_user_id and name
            UserAccountInterface.create_or_update_user(
                session=session, auth0_user_id="auth0|user2", name="Test User 2"
            )

            # Can update name
            UserAccountInterface.create_or_update_user(
                session=session, auth0_user_id="auth0|user2", name="Test User 3"
            )

    def get_user_by_auth0_user_id(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=session, auth0_user_id="id0"
            )
            self.assertEqual(user.name, "User")
            self.assertEqual(
                {a.name for a in user.agencies}, {"Agency Alpha", "Agency Beta"}
            )

            # Raise error if no user found
            with self.assertRaises(NoResultFound):
                UserAccountInterface.get_user_by_auth0_user_id(
                    session=session, auth0_user_id="blah"
                )
