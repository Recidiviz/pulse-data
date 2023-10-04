# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements tests for the Justice Counts Control Panel backend API."""
import pytest
from mock import patch
from sqlalchemy.engine import Engine

from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    AgencyUserAccountAssociation,
    Source,
    UserAccount,
    UserAccountInvitationStatus,
)
from recidiviz.tests.auth.utils import get_test_auth0_config
from recidiviz.tests.justice_counts.utils.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.auth.auth0 import passthrough_authorization_decorator
from recidiviz.utils.types import assert_type


@pytest.mark.uses_db
class TestJusticePublisherAdminPanelAPI(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Publisher Admin Panel backend API."""

    def setUp(self) -> None:
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        self.client_patcher = patch("recidiviz.auth.auth0_client.Auth0")
        self.test_auth0_client = self.client_patcher.start().return_value
        test_config = Config(
            DB_URL=local_postgres_helpers.on_disk_postgres_db_url(),
            WTF_CSRF_ENABLED=False,
            AUTH_DECORATOR=passthrough_authorization_decorator(),
            AUTH0_CONFIGURATION=get_test_auth0_config(),
            AUTH0_CLIENT=self.test_auth0_client,
            SEGMENT_KEY="fake_segment_key",
        )
        self.app = create_app(config=test_config)
        self.client = self.app.test_client()
        self.app.secret_key = "NOT A SECRET"
        # `flask_sqlalchemy_session` sets the `scoped_session` attribute on the app,
        # even though this is not specified in the types for `app`.
        self.session = self.app.scoped_session  # type: ignore[attr-defined]
        super().setUp()

    def get_engine(self) -> Engine:
        return self.session.get_bind()

    def load_users_and_agencies(self) -> None:
        """Loads three agency and three users to the current session."""
        agency_A = self.test_schema_objects.test_agency_A
        user_A = self.test_schema_objects.test_user_A
        agency_B = self.test_schema_objects.test_agency_B
        user_B = self.test_schema_objects.test_user_B
        agency_C = self.test_schema_objects.test_agency_C
        user_C = self.test_schema_objects.test_user_C
        self.session.add_all([agency_A, user_A, agency_B, user_B, agency_C, user_C])
        self.session.commit()
        self.session.refresh(user_A)
        self.session.refresh(user_B)
        self.session.refresh(user_C)
        self.session.refresh(agency_A)
        self.session.refresh(agency_B)
        self.session.refresh(agency_C)

        user_agency_association_A = AgencyUserAccountAssociation(
            user_account_id=user_A.id,
            agency_id=agency_A.id,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        user_agency_association_B = AgencyUserAccountAssociation(
            user_account_id=user_B.id,
            agency_id=agency_B.id,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        user_agency_association_C = AgencyUserAccountAssociation(
            user_account_id=user_C.id,
            agency_id=agency_C.id,
            invitation_status=UserAccountInvitationStatus.PENDING,
        )
        self.session.add_all(
            [
                user_agency_association_A,
                user_agency_association_B,
                user_agency_association_C,
            ]
        )
        self.session.commit()

    def test_get_all_users(self) -> None:
        self.load_users_and_agencies()
        response = self.client.get("/admin/user")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        user_json_list = response_json["users"]
        self.assertEqual(len(user_json_list), 3)
        users = self.session.query(UserAccount).all()
        user_id_to_user = {user.id: user for user in users}

        for user_json in user_json_list:
            db_user = user_id_to_user[(user_json["id"])]
            agency = db_user.agency_assocs[0].agency
            self.assertEqual(user_json["auth0_user_id"], db_user.auth0_user_id)
            self.assertEqual(user_json["name"], db_user.name)
            self.assertEqual(len(user_json["agencies"]), 1)
            self.assertEqual(user_json["agencies"][0]["id"], agency.id)
            self.assertEqual(user_json["agencies"][0]["name"], agency.name)

    def test_get_all_agencies(self) -> None:
        self.load_users_and_agencies()
        response = self.client.get("/admin/agency")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(
            response_json["systems"], [enum.value for enum in schema.System]
        )
        agency_json_list = response_json["agencies"]
        self.assertEqual(len(agency_json_list), 3)
        agencies = self.session.query(Source).all()
        agency_id_to_agency = {a.id: a for a in agencies}

        for agency_json in agency_json_list:
            db_agency = agency_id_to_agency[(agency_json["id"])]
            user = db_agency.user_account_assocs[0].user_account
            self.assertEqual(agency_json["name"], db_agency.name)
            self.assertEqual(len(agency_json["team"]), 1)
            self.assertEqual(agency_json["team"][0]["name"], user.name)

    def test_get_agency(self) -> None:
        self.load_users_and_agencies()
        agency = (
            self.session.query(schema.Agency)
            .filter(schema.Agency.name == "Agency Alpha")
            .one()
        )
        response = self.client.get(f"/admin/agency/{agency.id}")
        self.assertEqual(response.status_code, 200)
        response_json = assert_type(response.json, dict)
        self.assertEqual(
            response_json["roles"], [enum.value for enum in schema.UserAccountRole]
        )
        agency_json = response_json["agency"]
        user = agency.user_account_assocs[0].user_account
        self.assertEqual(agency_json["name"], agency.name)
        self.assertEqual(agency_json["id"], agency.id)
        self.assertEqual(len(agency_json["team"]), 1)
        self.assertEqual(agency_json["team"][0]["name"], user.name)
