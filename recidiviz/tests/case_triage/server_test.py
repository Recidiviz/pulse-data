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
from unittest.mock import MagicMock

import pytest
from flask import Flask, Response, jsonify, session

from recidiviz.case_triage.api_routes import create_api_blueprint
from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.case_updates.serializers import serialize_last_version_info
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.impersonate_users import (
    IMPERSONATED_EMAIL_KEY,
    ImpersonateUser,
)
from recidiviz.case_triage.querier.case_update_presenter import CaseUpdateStatus
from recidiviz.case_triage.scoped_sessions import setup_scoped_sessions
from recidiviz.persistence.database.schema.case_triage.schema import (
    OpportunityDeferral,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.api_test_helpers import (
    CaseTriageTestHelpers,
)
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_client,
    generate_fake_officer,
    generate_fake_opportunity,
    generate_fake_case_update,
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
        self.mock_segment_client = MagicMock()
        api = create_api_blueprint(self.mock_segment_client)
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
        self.database_key.declarative_meta.metadata.create_all(engine)
        self.session = SessionFactory.for_database(self.database_key)

        # Add seed data
        self.officer = generate_fake_officer("officer_id_1", "officer1@recidiviz.org")
        self.officer_without_clients = generate_fake_officer(
            "officer_id_2", "officer2@recidiviz.org"
        )
        self.client_1 = generate_fake_client(
            client_id="client_1",
            supervising_officer_id=self.officer.external_id,
        )
        self.client_2 = generate_fake_client(
            client_id="client_2",
            supervising_officer_id=self.officer.external_id,
            last_assessment_date=date(2021, 2, 2),
        )
        self.client_3 = generate_fake_client(
            client_id="client_3",
            supervising_officer_id=self.officer.external_id,
        )
        self.case_update_1 = generate_fake_case_update(
            self.client_1,
            self.officer.external_id,
            action_type=CaseUpdateActionType.COMPLETED_ASSESSMENT,
            last_version=serialize_last_version_info(
                CaseUpdateActionType.COMPLETED_ASSESSMENT, self.client_1
            ).to_json(),
        )
        self.case_update_2 = generate_fake_case_update(
            self.client_2,
            self.officer_without_clients.external_id,
            action_type=CaseUpdateActionType.COMPLETED_ASSESSMENT,
            last_version=serialize_last_version_info(
                CaseUpdateActionType.COMPLETED_ASSESSMENT, self.client_2
            ).to_json(),
        )
        self.case_update_3 = generate_fake_case_update(
            self.client_1,
            self.officer.external_id,
            action_type=CaseUpdateActionType.OTHER_DISMISSAL,
            last_version=serialize_last_version_info(
                CaseUpdateActionType.OTHER_DISMISSAL, self.client_1
            ).to_json(),
        )
        self.client_2.most_recent_assessment_date = date(2022, 2, 2)

        self.opportunity_1 = generate_fake_opportunity(
            officer_id=self.officer.external_id,
            person_external_id=self.client_1.person_external_id,
        )
        tomorrow = datetime.now() + timedelta(days=1)
        self.deferral_1 = OpportunityDeferral.from_etl_opportunity(
            self.opportunity_1, tomorrow, True
        )
        self.opportunity_2 = generate_fake_opportunity(
            officer_id=self.officer.external_id,
            person_external_id=self.client_2.person_external_id,
        )
        self.session.expire_on_commit = False

        self.session.add_all(
            [
                self.officer,
                self.officer_without_clients,
                self.client_1,
                self.client_2,
                self.client_3,
                self.case_update_1,
                self.case_update_2,
                self.case_update_3,
                self.opportunity_1,
                self.deferral_1,
                self.opportunity_2,
            ]
        )
        self.session.commit()

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
        with self.helpers.as_officer(self.officer_without_clients):
            self.assertEqual(self.helpers.get_clients(), [])

    def test_get_clients(self) -> None:
        with self.helpers.as_officer(self.officer):
            client_1_response = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )

            self.assertCountEqual(
                client_1_response["caseUpdates"],
                [
                    CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
                    CaseUpdateActionType.OTHER_DISMISSAL.value,
                ],
            )

    def test_no_opportunities(self) -> None:
        with self.helpers.as_officer(self.officer_without_clients):
            self.assertEqual([], self.helpers.get_opportunities())

    def test_get_opportunities(self) -> None:
        with self.helpers.as_officer(self.officer):
            # Only one of the opportunities should be active
            self.assertEqual(len(self.helpers.get_opportunities()), 1)

    def test_case_updates_malformed_input(self) -> None:
        with self.helpers.as_officer(self.officer):
            # GET instead of POST
            response = self.test_client.get("/case_updates")
            self.assertEqual(response.status_code, HTTPStatus.METHOD_NOT_ALLOWED)

            # `personExternalId` doesn't map to a real person
            response = self.test_client.post(
                "/case_updates",
                json={
                    "personExternalId": "nonexistent-person",
                    "actionType": CaseUpdateActionType.OTHER_DISMISSAL.value,
                },
            )

            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "bad_request")
            self.assertIn(
                "does not correspond to a known person",
                response.get_json()["description"]["personExternalId"],
            )

    def test_case_updates_success(self) -> None:
        with self.helpers.as_officer(self.officer):
            # Record new user action
            response = self.test_client.post(
                "/case_updates",
                json={
                    "personExternalId": self.client_2.person_external_id,
                    "actionType": CaseUpdateActionType.OTHER_DISMISSAL.value,
                    "comment": "Not on my caseload!",
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)
            self.mock_segment_client.track_person_action_taken.assert_called()

            # Verify user action is persisted
            client = self.helpers.find_client_in_api_response(
                self.client_2.person_external_id
            )
            self.assertIn(
                CaseUpdateActionType.OTHER_DISMISSAL.value, client["caseUpdates"]
            )

            update = client["caseUpdates"][CaseUpdateActionType.OTHER_DISMISSAL.value]
            self.assertEqual(update["status"], CaseUpdateStatus.IN_PROGRESS.value)
            self.assertEqual(
                update["comment"],
                "Not on my caseload!",
            )
            self.assertEqual(len(client["caseUpdates"].keys()), 2)

    def test_delete_case_update(self) -> None:
        with self.helpers.as_officer(self.officer):

            # Record new user action
            case_update = generate_fake_case_update(
                self.client_2,
                self.officer.external_id,
                action_type=CaseUpdateActionType.OTHER_DISMISSAL,
                comment="They are currently in jail.",
            )
            self.session.add(case_update)
            self.session.commit()

            # Verify user action is persisted
            client = self.helpers.find_client_in_api_response(
                self.client_2.person_external_id
            )
            self.assertIn(
                CaseUpdateActionType.OTHER_DISMISSAL.value, client["caseUpdates"]
            )

            response = self.test_client.delete(f"/case_updates/{case_update.update_id}")
            self.assertEqual(response.status_code, 200)
            self.mock_segment_client.track_person_action_removed.assert_called()

            client = self.helpers.find_client_in_api_response(
                self.client_2.person_external_id
            )
            self.assertNotIn(
                CaseUpdateActionType.OTHER_DISMISSAL.value, client["caseUpdates"]
            )

    def test_delete_case_update_errors(self) -> None:
        # Record new user action for officer 2
        case_update = self.case_update_2

        # No signed in user
        with self.helpers.as_demo_user():
            response = self.test_client.delete(f"/case_updates/{case_update.update_id}")
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

        # Officer trying to delete another officer's update
        with self.helpers.as_officer(self.officer_without_clients):
            response = self.test_client.delete(f"/case_updates/{case_update.update_id}")
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_post_state_policy(self) -> None:
        with self.helpers.as_officer(self.officer):
            response = self.test_client.post(
                "/policy_requirements_for_state", json={"state": "US_ID"}
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)


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
