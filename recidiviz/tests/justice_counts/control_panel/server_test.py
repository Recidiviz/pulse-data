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
"""Implements tests for the Justice Counts Control Panel backend API."""
import pytest
from flask import session
from sqlalchemy.engine import Engine

from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.server import create_app
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Agency,
    Source,
    UserAccount,
)
from recidiviz.tests.auth.utils import get_test_auth0_config
from recidiviz.tests.justice_counts.utils import (
    JusticeCountsDatabaseTestCase,
    JusticeCountsSchemaTestObjects,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.auth.auth0 import passthrough_authorization_decorator


@pytest.mark.uses_db
class TestJusticeCountsControlPanelAPI(JusticeCountsDatabaseTestCase):
    """Implements tests for the Justice Counts Control Panel backend API."""

    def setUp(self) -> None:
        test_config = Config(
            DB_URL=local_postgres_helpers.on_disk_postgres_db_url(),
            WTF_CSRF_ENABLED=False,
            AUTH_DECORATOR=passthrough_authorization_decorator(),
            AUTH0_CONFIGURATION=get_test_auth0_config(),
        )
        self.app = create_app(config=test_config)
        self.client = self.app.test_client()
        self.app.secret_key = "NOT A SECRET"
        # `flask_sqlalchemy_session` sets the `scoped_session` attribute on the app,
        # even though this is not specified in the types for `app`.
        self.session = self.app.scoped_session  # type: ignore[attr-defined]
        self.test_schema_objects = JusticeCountsSchemaTestObjects()
        super().setUp()

    def get_engine(self) -> Engine:
        return self.session.get_bind()

    def test_logout(self) -> None:
        with self.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["session_data"] = {"foo": "bar"}

            response = client.post("/auth/logout")
            self.assertEqual(response.status_code, 200)
            with self.app.test_request_context():
                self.assertEqual(0, len(session.keys()))

    def test_auth0_config(self) -> None:
        response = self.client.get("/auth0_public_config.js")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.data,
            b"window.AUTH0_CONFIG = {'audience': 'http://localhost', 'clientId': 'test_client_id', 'domain': 'auth0.localhost'};",
        )

    def test_get_reports(self) -> None:
        user_A = self.test_schema_objects.test_user_A
        report = self.test_schema_objects.test_report_monthly
        self.session.add_all([report, user_A])
        self.session.commit()
        response = self.client.get(
            f"/api/reports?user_id={user_A.id}&agency_id={report.source_id}"
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json,
            [
                {
                    "editors": [],
                    "frequency": "MONTHLY",
                    "id": 1,
                    "last_modified_at": None,
                    "month": 6,
                    "status": "NOT_STARTED",
                    "year": 2022,
                }
            ],
        )

    def test_create_user(self) -> None:
        email_address = "user@gmail.com"
        agency_name = "Agency Alpha"
        name = "John Doe"
        self.session.add(Agency(name=agency_name, id=1))
        admin_response = self.client.post(
            "/api/users",
            json={
                "email_address": email_address,
                "agency_ids": [1],
                "name": name,
            },
        )
        self.assertEqual(admin_response.status_code, 200)
        self.assertEqual(
            admin_response.json,
            {
                "agencies": [{"id": 1, "name": agency_name}],
                "auth0_user_id": None,
                "email_address": email_address,
                "id": 1,
                "name": name,
            },
        )
        db_item = self.session.query(UserAccount).one_or_none()
        self.assertEqual(db_item.to_json(), admin_response.json)

    def test_update_user(self) -> None:
        agency_name = "Agency Alpha"
        email_address = "user@gmail.com"
        agency = Agency(name=agency_name, id=1)
        user_account = UserAccount(
            id=1, name="Jane Doe", agencies=[agency], email_address=email_address
        )
        self.session.add_all([agency, user_account])
        self.session.commit()
        auth0_user_id = "12345abc"
        user_response = self.client.post(
            "/api/users",
            json={"email_address": email_address, "auth0_user_id": auth0_user_id},
        )
        self.assertEqual(user_response.status_code, 200)
        self.assertEqual(
            user_response.json,
            {
                "agencies": [{"id": 1, "name": agency_name}],
                "auth0_user_id": auth0_user_id,
                "email_address": email_address,
                "id": 1,
                "name": "Jane Doe",
            },
        )
        db_items = self.session.query(UserAccount).all()
        self.assertEqual(len(db_items), 1)

    def test_session(self) -> None:
        # Add data
        name = "Agency Alpha"
        self.session.add(Source(name=name))
        self.session.commit()

        # Query data
        source = self.session.query(Source).one_or_none()

        self.assertEqual(source.name, name)
