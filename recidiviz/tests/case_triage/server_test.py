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
"""Implements tests for the Case Triage Flask server."""
from datetime import date
from http import HTTPStatus
from typing import Optional
from unittest import mock, TestCase

import pytest
from flask import Flask, Response, g, jsonify, session

from recidiviz.case_triage.api_routes import api
from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.case_updates.interface import CaseUpdatesInterface
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.impersonate_users import (
    IMPERSONATED_EMAIL_KEY,
    ImpersonateUser,
)
from recidiviz.case_triage.scoped_sessions import setup_scoped_sessions
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_officer,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.flask_exception import FlaskException


@pytest.mark.uses_db
class TestCaseTriageAPIRoutes(TestCase):
    """Implements tests for the Case Triage Flask server."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.test_app = Flask(__name__)
        self.test_app.register_blueprint(api)
        self.test_client = self.test_app.test_client()

        @self.test_app.errorhandler(FlaskException)
        def _handle_auth_error(ex: FlaskException) -> Response:
            response = jsonify(
                {
                    "code": ex.code,
                    "description": ex.description,
                }
            )
            response.status_code = ex.status_code
            return response

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        db_url = local_postgres_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.test_app, db_url)
        # Auto-generate all tables that exist in our schema in this database
        database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        database_key.declarative_meta.metadata.create_all(engine)

        # Add seed data
        self.officer_1 = generate_fake_officer("officer_id_1")
        self.officer_2 = generate_fake_officer("officer_id_2")
        self.client_1 = generate_fake_client(
            client_id="client_1",
            supervising_officer_id=self.officer_1.external_id,
        )
        self.client_2 = generate_fake_client(
            client_id="client_2",
            supervising_officer_id=self.officer_1.external_id,
            last_assessment_date=date(2021, 2, 2),
        )
        self.case_update_1 = CaseUpdate(
            person_external_id=self.client_1.person_external_id,
            officer_external_id=self.client_1.supervising_officer_external_id,
            state_code=self.client_1.state_code,
            update_metadata={
                "actions": CaseUpdatesInterface.serialize_actions(
                    self.client_1,
                    [CaseUpdateActionType.COMPLETED_ASSESSMENT],
                ),
            },
        )
        self.case_update_2 = CaseUpdate(
            person_external_id=self.client_2.person_external_id,
            officer_external_id=self.client_2.supervising_officer_external_id,
            state_code=self.client_2.state_code,
            update_metadata={
                "actions": CaseUpdatesInterface.serialize_actions(
                    self.client_2,
                    [CaseUpdateActionType.COMPLETED_ASSESSMENT],
                ),
            },
        )
        self.client_2.most_recent_assessment_date = date(2022, 2, 2)

        sess = SessionFactory.for_database(self.database_key)
        sess.add(self.officer_1)
        sess.add(self.client_1)
        sess.add(self.client_2)
        sess.add(self.case_update_1)
        sess.add(self.case_update_2)
        sess.commit()

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

    def test_no_clients(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_2

            response = self.test_client.get("/clients")
            self.assertEqual(response.status_code, HTTPStatus.OK)

            client_json = response.get_json()
            self.assertEqual(client_json, [])

    def test_get_clients(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_1

            response = self.test_client.get("/clients")
            self.assertEqual(response.status_code, HTTPStatus.OK)

            client_json = response.get_json()
            self.assertEqual(len(client_json), 2)

            self.assertEqual(
                client_json[0]["inProgressActions"],
                [CaseUpdateActionType.COMPLETED_ASSESSMENT.value],
            )
            self.assertTrue("inProgressActions" not in client_json[1])

    def test_record_client_action_malformed_input(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_1

            # GET instead of POST
            response = self.test_client.get("/record_client_action")
            self.assertEqual(response.status_code, HTTPStatus.METHOD_NOT_ALLOWED)

            # No body
            response = self.test_client.post("/record_client_action")
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "missing_body")

            # Missing `personExternalId`
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "person_external_id": self.client_1.person_external_id,
                    "actions": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "missing_arg")

            # Missing `actions`
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "action": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "missing_arg")

            # `actions` is not a list
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "actions": CaseUpdateActionType.OTHER_DISMISSAL.value,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "improper_type")

            # `actions` not a list of CaseUpdateActionTypes
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "actions": ["imaginary-action"],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "improper_type")

            # `personExternalId` doesn't map to a real person
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": "nonexistent-person",
                    "actions": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "invalid_arg")

    def test_record_client_action_success(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_1

            # Record new user action
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_2.person_external_id,
                    "actions": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)

            # Verify user action is persisted
            response = self.test_client.get("/clients")
            self.assertEqual(response.status_code, HTTPStatus.OK)

            client_json = response.get_json()
            self.assertEqual(len(client_json), 2)

            self.assertEqual(
                client_json[1]["inProgressActions"],
                [CaseUpdateActionType.OTHER_DISMISSAL.value],
            )


class TestUserImpersonation(TestCase):
    """Implements tests for user impersonation.

    Note: The ignored types in here are due to a mismatch between Flask's docs and the
    typeshed definitions. A task has been filed on the typeshed repo that looks into this
    https://github.com/python/typeshed/issues/5016
    """

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.metadata_patcher.start().return_value = "recidiviz-456"

        self.auth_store = AuthorizationStore()

        def no_op() -> str:
            return ""

        self.test_app = Flask(__name__)
        self.test_app.secret_key = "NOT-A-SECRET"
        self.test_app.add_url_rule("/", view_func=no_op)
        self.test_app.add_url_rule(
            "/impersonate_user",
            view_func=ImpersonateUser.as_view(
                "impersonate_user",
                redirect_url="/",
                authorization_store=self.auth_store,
            ),
        )
        self.test_client = self.test_app.test_client()

        @self.test_app.errorhandler(FlaskException)
        def _handle_auth_error(ex: FlaskException) -> Response:
            response = jsonify(
                {
                    "code": ex.code,
                    "description": ex.description,
                }
            )
            response.status_code = ex.status_code
            return response

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_non_admin(self) -> None:
        with self.test_app.test_request_context():
            response = self.test_client.get("/impersonate_user")
            self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {
                    "email": "non-admin@recidiviz.org",
                }

            response = self.test_client.get("/impersonate_user")
            self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

    def test_no_query_params(self) -> None:
        with self.test_app.test_request_context():
            self.auth_store.admin_users = ["admin@recidiviz.org"]
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {
                    "email": "admin@recidiviz.org",
                }

            response = self.test_client.get("/impersonate_user")
            self.assertEqual(response.status_code, HTTPStatus.FOUND)

            with self.test_client.session_transaction() as sess:  # type: ignore
                self.assertTrue(IMPERSONATED_EMAIL_KEY not in session)

    def test_happy_path(self) -> None:
        with self.test_app.test_request_context():
            self.auth_store.admin_users = ["admin@recidiviz.org"]
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {
                    "email": "admin@recidiviz.org",
                }

            response = self.test_client.get(
                f"/impersonate_user?{IMPERSONATED_EMAIL_KEY}=non-admin%40recidiviz.org"
            )
            self.assertEqual(response.status_code, HTTPStatus.FOUND)

            with self.test_client.session_transaction() as sess:  # type: ignore
                self.assertEqual(
                    sess[IMPERSONATED_EMAIL_KEY], "non-admin@recidiviz.org"
                )

    def test_remove_impersonation(self) -> None:
        with self.test_app.test_request_context():
            self.auth_store.admin_users = ["admin@recidiviz.org"]
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {
                    "email": "admin@recidiviz.org",
                }

            # Perform initial impersonation
            self.test_client.get(
                f"/impersonate_user?{IMPERSONATED_EMAIL_KEY}=non-admin%40recidiviz.org"
            )

            # Undo impersonation
            response = self.test_client.get("/impersonate_user")
            self.assertEqual(response.status_code, HTTPStatus.FOUND)
            with self.test_client.session_transaction() as sess:  # type: ignore
                self.assertTrue(IMPERSONATED_EMAIL_KEY not in sess)
