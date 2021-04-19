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
from datetime import date, datetime, timedelta
from http import HTTPStatus
from typing import Optional
from unittest import mock, TestCase

import pytest
from flask import Flask, Response, g, jsonify, session

from recidiviz.case_triage.api_routes import create_api_blueprint
from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.case_updates.serializers import serialize_last_version_info
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.impersonate_users import (
    IMPERSONATED_EMAIL_KEY,
    ImpersonateUser,
)
from recidiviz.case_triage.scoped_sessions import setup_scoped_sessions
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    OpportunityDeferral,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_officer,
    generate_fake_opportunity,
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
        register_error_handlers(self.test_app)
        api = create_api_blueprint()
        self.test_app.register_blueprint(api)

        self.test_client = self.test_app.test_client()

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
        now = datetime.now()
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
        self.client_3 = generate_fake_client(
            client_id="client_3",
            supervising_officer_id=self.officer_1.external_id,
        )
        self.case_update_1 = CaseUpdate(
            person_external_id=self.client_1.person_external_id,
            officer_external_id=self.client_1.supervising_officer_external_id,
            state_code=self.client_1.state_code,
            action_type=CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
            action_ts=now,
            last_version=serialize_last_version_info(
                CaseUpdateActionType.COMPLETED_ASSESSMENT, self.client_1
            ).to_json(),
        )
        self.case_update_2 = CaseUpdate(
            person_external_id=self.client_2.person_external_id,
            officer_external_id=self.client_2.supervising_officer_external_id,
            state_code=self.client_2.state_code,
            action_type=CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
            action_ts=now,
            last_version=serialize_last_version_info(
                CaseUpdateActionType.COMPLETED_ASSESSMENT, self.client_2
            ).to_json(),
        )
        self.case_update_3 = CaseUpdate(
            person_external_id=self.client_1.person_external_id,
            officer_external_id=self.client_1.supervising_officer_external_id,
            state_code=self.client_1.state_code,
            action_type=CaseUpdateActionType.OTHER_DISMISSAL.value,
            action_ts=now,
            last_version=serialize_last_version_info(
                CaseUpdateActionType.OTHER_DISMISSAL, self.client_1
            ).to_json(),
        )
        self.client_2.most_recent_assessment_date = date(2022, 2, 2)

        self.opportunity_1 = generate_fake_opportunity(
            officer_id=self.officer_1.external_id,
            person_external_id=self.client_1.person_external_id,
        )
        tomorrow = datetime.now() + timedelta(days=1)
        self.deferral_1 = OpportunityDeferral.from_etl_opportunity(
            self.opportunity_1, tomorrow, True
        )
        self.opportunity_2 = generate_fake_opportunity(
            officer_id=self.officer_1.external_id,
            person_external_id=self.client_2.person_external_id,
        )

        sess = SessionFactory.for_database(self.database_key)
        sess.expire_on_commit = False
        sess.add(self.officer_1)
        sess.add(self.client_1)
        sess.add(self.client_2)
        sess.add(self.client_3)
        sess.add(self.case_update_1)
        sess.add(self.case_update_2)
        sess.add(self.case_update_3)
        sess.add(self.opportunity_1)
        sess.add(self.deferral_1)
        sess.add(self.opportunity_2)
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
            self.assertEqual(len(client_json), 3)

            client_1_response = None
            for data in client_json:
                if data["personExternalId"] == self.client_1.person_external_id:
                    client_1_response = data
                    break
            assert client_1_response is not None

            self.assertCountEqual(
                client_1_response["inProgressActions"],
                [
                    CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
                    CaseUpdateActionType.OTHER_DISMISSAL.value,
                ],
            )

    def test_no_opportunities(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_2

            response = self.test_client.get("/opportunities")
            self.assertEqual(response.status_code, HTTPStatus.OK)

            client_json = response.get_json()
            self.assertEqual(client_json, [])

    def test_get_opportunities(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_1

            response = self.test_client.get("/opportunities")
            self.assertEqual(response.status_code, HTTPStatus.OK)

            opportunity_json = response.get_json()
            # Only one of the opportunities should be active
            self.assertEqual(len(opportunity_json), 1)

    def test_record_client_action_malformed_input(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_1

            # GET instead of POST
            response = self.test_client.get("/record_client_action")
            self.assertEqual(response.status_code, HTTPStatus.METHOD_NOT_ALLOWED)

            # No body
            response = self.test_client.post("/record_client_action")
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "bad_request")

            # Missing `personExternalId`
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "person_external_id": self.client_1.person_external_id,
                    "actions": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "bad_request")

            # Missing `actionType`
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "action": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "bad_request")
            self.assertEqual(
                "Missing data for required field.",
                response.get_json()["description"]["actions"][0],
            )

            # `actions` is not a list
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "actions": False,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "bad_request")
            self.assertEqual(
                "Not a valid list.",
                response.get_json()["description"]["actions"][0],
            )

            # `actions` not a list of CaseUpdateActionTypes
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "actions": ["imaginary-action"],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "bad_request")
            self.assertIn(
                "Must be one of: ",
                response.get_json()["description"]["actions"]["0"][0],
            )

            # `personExternalId` doesn't map to a real person
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": "nonexistent-person",
                    "actions": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "bad_request")
            self.assertIn(
                "does not correspond to a known person",
                response.get_json()["description"]["personExternalId"],
            )

    def test_record_client_action_success(self) -> None:
        with self.test_app.test_request_context():
            g.current_user = self.officer_1

            # Record new user action
            response = self.test_client.post(
                "/record_client_action",
                json={
                    "personExternalId": self.client_2.person_external_id,
                    "actions": [CaseUpdateActionType.OTHER_DISMISSAL.value],
                    "otherText": "Notes",
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)

            # Verify user action is persisted
            response = self.test_client.get("/clients")
            self.assertEqual(response.status_code, HTTPStatus.OK)

            client_json = response.get_json()
            self.assertEqual(len(client_json), 3)

            client_2_response = None
            for data in client_json:
                if data["personExternalId"] == self.client_2.person_external_id:
                    client_2_response = data
                    break
            assert client_2_response is not None

            self.assertEqual(
                client_2_response["inProgressActions"],
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
