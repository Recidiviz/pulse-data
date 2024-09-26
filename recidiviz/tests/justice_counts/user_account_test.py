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
"""This class implements tests for the Justice Counts UserAccountInterface."""

from sqlalchemy.orm.exc import NoResultFound

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.justice_counts.utils.utils import JusticeCountsDatabaseTestCase


class TestUserAccountInterface(JusticeCountsDatabaseTestCase):
    """Implements tests for the UserAccountInterface."""

    def setUp(self) -> None:
        super().setUp()
        with SessionFactory.using_database(self.database_key) as session:
            AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Alpha",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ca",
                fips_county_code="us_ca_sacramento",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Beta",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ak",
                fips_county_code="us_ak_anchorage",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )
            AgencyInterface.create_or_update_agency(
                session=session,
                name="Agency Gamma",
                systems=[schema.System.LAW_ENFORCEMENT],
                state_code="us_ar",
                fips_county_code="us_ar_lee",
                agency_id=None,
                is_superagency=False,
                super_agency_id=None,
                is_dashboard_enabled=False,
            )

            UserAccountInterface.create_or_update_user(
                session=session,
                name="User",
                auth0_user_id="id0",
                email="test-email",
                auth0_client=self.test_auth0_client,
            )

    def test_create_or_update_user(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            # Can create user with auth0_user_id, name, and agencies
            user = UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id="auth0|user2",
                name="Test User 2",
                email="test-email",
                auth0_client=self.test_auth0_client,
            )
            agency = AgencyInterface.get_agency_by_name_state_and_systems(
                session=session,
                name="Agency Gamma",
                state_code="us_ar",
                systems=[schema.System.LAW_ENFORCEMENT.value],
            )
            UserAccountInterface.add_or_update_user_agency_association(
                session=session,
                user=user,
                agencies=[agency],
            )

            # Can update name
            UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id=user.auth0_user_id,
                name="Test User 3",
                auth0_client=self.test_auth0_client,
            )

            user_account_association = (
                session.query(schema.AgencyUserAccountAssociation)
                .filter(
                    schema.AgencyUserAccountAssociation.user_account_id == user.id,
                    schema.AgencyUserAccountAssociation.agency_id == agency.id,
                )
                .one()
            )
            self.assertEqual(
                user_account_association.invitation_status,
                None,
            )

    def get_user_by_auth0_user_id(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            user = UserAccountInterface.get_user_by_auth0_user_id(
                session=session, auth0_user_id="id0"
            )
            self.assertEqual(user.name, "User")
            self.assertEqual(user.email, "test-email")
            self.assertEqual(
                {assoc.agency.name for assoc in user.agency_assocs},
                {"Agency Alpha", "Agency Beta"},
            )

            # Raise error if no user found
            with self.assertRaises(NoResultFound):
                UserAccountInterface.get_user_by_auth0_user_id(
                    session=session, auth0_user_id="blah"
                )

    def test_get_users_overview(self) -> None:
        with SessionFactory.using_database(self.database_key) as session:
            UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id="auth0|user1",
                name="Test User 1",
                email="user1-email",
                auth0_client=self.test_auth0_client,
            )
            UserAccountInterface.create_or_update_user(
                session=session,
                auth0_user_id="auth0|user2",
                name="Test User 2",
                email="user2-email",
                auth0_client=self.test_auth0_client,
            )

            users_overview = UserAccountInterface.get_users_overview(session=session)

            # Dropping the user id since it is an unstable value.
            expected_overview = [
                {
                    "auth0_user_id": "auth0|1234User",
                    "name": "User",
                    "email": "test-email",
                },
                {
                    "auth0_user_id": "auth0|1234Test User 1",
                    "name": "Test User 1",
                    "email": "user1-email",
                },
                {
                    "auth0_user_id": "auth0|1234Test User 2",
                    "name": "Test User 2",
                    "email": "user2-email",
                },
            ]

            # Sort both lists for order-agnostic comparison
            sorted_users_overview = sorted(
                [
                    {
                        "auth0_user_id": user["auth0_user_id"],
                        "name": user["name"],
                        "email": user["email"],
                    }
                    for user in users_overview
                ],
                key=lambda user: user["auth0_user_id"],
            )

            sorted_expected_overview = sorted(
                expected_overview, key=lambda user: user["auth0_user_id"]
            )

            self.assertEqual(sorted_users_overview, sorted_expected_overview)
