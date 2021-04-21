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
"""Implements tests to enforce that demo users work."""
import json
import os
from http import HTTPStatus
from typing import Optional
from unittest import TestCase

import pytest
from flask import Flask

import recidiviz.case_triage
from recidiviz.case_triage.api_routes import create_api_blueprint
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.scoped_sessions import setup_scoped_sessions
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.api_test_helpers import (
    CaseTriageTestHelpers,
)
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class TestDemoUser(TestCase):
    """Implements tests to enforce that demo users work."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.test_app = Flask(__name__)
        register_error_handlers(self.test_app)
        api = create_api_blueprint()
        self.test_app.register_blueprint(api)
        self.test_client = self.test_app.test_client()
        self.helpers = CaseTriageTestHelpers.from_test(self, self.test_app)

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        db_url = local_postgres_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.test_app, db_url)
        # Auto-generate all tables that exist in our schema in this database
        database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        database_key.declarative_meta.metadata.create_all(engine)

        demo_fixture_path = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.realpath(recidiviz.case_triage.__file__)),
                "./fixtures/dummy_clients.json",
            )
        )

        with open(demo_fixture_path) as f:
            self.demo_data = json.load(f)

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.teardown_on_disk_postgresql_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_get_clients(self) -> None:
        with self.helpers.as_demo_user():
            self.assertEqual(len(self.helpers.get_clients()), len(self.demo_data))

    def test_create_case_updates_action(self) -> None:
        with self.helpers.as_demo_user():
            client_to_modify = self.helpers.get_clients()[0]

            response = self.test_client.post(
                "/case_updates",
                json={
                    "personExternalId": client_to_modify["personExternalId"],
                    "actionType": "FILED_REVOCATION_OR_VIOLATION",
                    "comment": "",
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)

            new_client = self.helpers.find_client_in_api_response(
                client_to_modify["personExternalId"]
            )
            self.assertIn(
                "FILED_REVOCATION_OR_VIOLATION",
                new_client["caseUpdates"],
            )
