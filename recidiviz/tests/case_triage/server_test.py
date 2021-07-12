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
from unittest import TestCase, mock
from unittest.mock import MagicMock

import pytest
from flask import Flask, Response, jsonify, session

from recidiviz.case_triage.admin_flask_views import (
    IMPERSONATED_EMAIL_KEY,
    ImpersonateUser,
)
from recidiviz.case_triage.api_routes import create_api_blueprint
from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.case_updates.serializers import serialize_client_case_version
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityType,
)
from recidiviz.case_triage.querier.case_update_presenter import CaseUpdateStatus
from recidiviz.case_triage.scoped_sessions import setup_scoped_sessions
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.api_test_helpers import CaseTriageTestHelpers
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_case_update,
    generate_fake_client,
    generate_fake_client_info,
    generate_fake_etl_opportunity,
    generate_fake_officer,
    generate_fake_reminder,
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
        self.session = self.test_app.scoped_session

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
        self.client_info_3 = generate_fake_client_info(
            client=self.client_3,
            preferred_name="Alex",
        )
        self.case_update_1 = generate_fake_case_update(
            self.client_1,
            self.officer.external_id,
            action_type=CaseUpdateActionType.COMPLETED_ASSESSMENT,
            last_version=serialize_client_case_version(
                CaseUpdateActionType.COMPLETED_ASSESSMENT, self.client_1
            ).to_json(),
        )
        self.case_update_2 = generate_fake_case_update(
            self.client_2,
            self.officer_without_clients.external_id,
            action_type=CaseUpdateActionType.COMPLETED_ASSESSMENT,
            last_version=serialize_client_case_version(
                CaseUpdateActionType.COMPLETED_ASSESSMENT, self.client_2
            ).to_json(),
        )
        self.case_update_3 = generate_fake_case_update(
            self.client_1,
            self.officer.external_id,
            action_type=CaseUpdateActionType.NOT_ON_CASELOAD,
            last_version=serialize_client_case_version(
                CaseUpdateActionType.NOT_ON_CASELOAD, self.client_1
            ).to_json(),
        )
        self.client_2.most_recent_assessment_date = date(2022, 2, 2)

        self.opportunity_1 = generate_fake_etl_opportunity(
            officer_id=self.officer.external_id,
            person_external_id=self.client_1.person_external_id,
        )
        tomorrow = datetime.now() + timedelta(days=1)
        self.deferral_1 = generate_fake_reminder(self.opportunity_1, tomorrow)
        self.opportunity_2 = generate_fake_etl_opportunity(
            officer_id=self.officer.external_id,
            person_external_id=self.client_2.person_external_id,
        )
        # all generated fake clients have no employer
        self.num_unemployed_opportunities = 3

        self.session.add_all(
            [
                self.officer,
                self.officer_without_clients,
                self.client_1,
                self.client_2,
                self.client_3,
                self.client_info_3,
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
                    CaseUpdateActionType.NOT_ON_CASELOAD.value,
                ],
            )

    def test_no_opportunities(self) -> None:
        with self.helpers.as_officer(self.officer_without_clients):
            self.assertEqual([], self.helpers.get_opportunities())

    def test_get_opportunities(self) -> None:

        with self.helpers.as_officer(self.officer):
            # Two ETL opportunities exist but one is inactive
            self.assertEqual(
                len(self.helpers.get_opportunities()),
                2 + self.num_unemployed_opportunities,
            )
            self.assertEqual(
                len(self.helpers.get_undeferred_opportunities()),
                1 + self.num_unemployed_opportunities,
            )

    def test_defer_opportunity_malformed_input(self) -> None:
        with self.helpers.as_officer(self.officer):
            # GET instead of POST
            response = self.test_client.get("/opportunity_deferrals")
            self.assertEqual(response.status_code, HTTPStatus.METHOD_NOT_ALLOWED)

            # `personExternalId` doesn't map to a real person
            response = self.test_client.post(
                "/opportunity_deferrals",
                json={
                    "personExternalId": "nonexistent-person",
                    "opportunityType": OpportunityType.EARLY_DISCHARGE.value,
                    "deferralType": OpportunityDeferralType.REMINDER.value,
                    "deferUntil": str(datetime.now()),
                    "requestReminder": True,
                },
            )

            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "not_found")
            self.assertIn(
                "does not correspond to a known person",
                response.get_json()["description"]["personExternalId"],
            )

    def test_defer_opportunity_successful(self) -> None:
        with self.helpers.as_officer(self.officer):
            opportunity_type = self.opportunity_2.opportunity_type
            person_external_id = self.client_2.person_external_id

            # Submit API request
            self.helpers.defer_opportunity(person_external_id, opportunity_type)
            self.mock_segment_client.track_opportunity_deferred.assert_called()

            # Verify that the opportunity is deferred
            self.assertEqual(
                len(
                    [
                        o
                        for o in self.helpers.get_undeferred_opportunities()
                        if o["opportunityType"] == opportunity_type
                        and o["personExternalId"] == person_external_id
                    ]
                ),
                0,
            )

    def test_delete_opportunity_deferral(self) -> None:
        with self.helpers.as_officer(self.officer):
            # Submit API request
            self.helpers.delete_opportunity_deferral(self.deferral_1.deferral_id)
            self.mock_segment_client.track_opportunity_deferral_deleted.assert_called()

            # Verify that all opportunities are undeferred
            self.assertEqual(
                len(self.helpers.get_undeferred_opportunities()),
                2 + self.num_unemployed_opportunities,
            )

    def test_cannot_delete_anothers_opportunity_deferral(self) -> None:
        # Fetch original deferral id
        with self.helpers.as_officer(self.officer_without_clients):
            response = self.test_client.delete(
                f"/opportunity_deferrals/{self.deferral_1.deferral_id}"
            )
            self.assertEqual(
                response.status_code, HTTPStatus.BAD_REQUEST, response.get_json()
            )

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
                    "actionType": CaseUpdateActionType.NOT_ON_CASELOAD.value,
                },
            )

            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(response.get_json()["code"], "not_found")
            self.assertIn(
                "does not correspond to a known person",
                response.get_json()["description"]["personExternalId"],
            )

    def test_case_updates_success(self) -> None:
        with self.helpers.as_officer(self.officer):
            # Record new user action
            action = CaseUpdateActionType.INCORRECT_ASSESSMENT_DATA.value
            person_external_id = self.client_2.person_external_id
            comment = "I recently completed a risk assessment"

            # Submit API request
            self.helpers.create_case_update(person_external_id, action, comment)

            self.mock_segment_client.track_person_action_taken.assert_called()

            # Verify user action is persisted
            client = self.helpers.find_client_in_api_response(person_external_id)
            self.assertIn(action, client["caseUpdates"])

            update = client["caseUpdates"][action]
            self.assertEqual(update["status"], CaseUpdateStatus.IN_PROGRESS.value)
            self.assertEqual(
                update["comment"],
                comment,
            )
            self.assertEqual(len(client["caseUpdates"].keys()), 2)

            # Assessment completed in CIS
            self.client_2.most_recent_assessment_date += timedelta(days=1)
            self.session.commit()

            client = self.helpers.find_client_in_api_response(person_external_id)
            self.assertIn(action, client["caseUpdates"])

            update = client["caseUpdates"][action]
            self.assertEqual(CaseUpdateStatus.UPDATED_IN_CIS.value, update["status"])

    def test_delete_case_update(self) -> None:
        with self.helpers.as_officer(self.officer):

            # Record new user action
            case_update = generate_fake_case_update(
                self.client_2,
                self.officer.external_id,
                action_type=CaseUpdateActionType.NOT_ON_CASELOAD,
                comment="They are currently in jail.",
            )
            self.session.add(case_update)
            self.session.commit()

            # Verify user action is persisted
            client = self.helpers.find_client_in_api_response(
                self.client_2.person_external_id
            )
            self.assertIn(
                CaseUpdateActionType.NOT_ON_CASELOAD.value,
                client["caseUpdates"],
            )

            response = self.test_client.delete(f"/case_updates/{case_update.update_id}")
            self.assertEqual(response.status_code, 200)
            self.mock_segment_client.track_person_action_removed.assert_called()

            client = self.helpers.find_client_in_api_response(
                self.client_2.person_external_id
            )
            self.assertNotIn(
                CaseUpdateActionType.NOT_ON_CASELOAD.value,
                client["caseUpdates"],
            )

    def test_delete_case_update_errors(self) -> None:
        case_update = self.case_update_2

        # Officer trying to delete another officer's update
        with self.helpers.as_officer(self.officer_without_clients):
            response = self.test_client.delete(f"/case_updates/{case_update.update_id}")
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

        # No signed in user
        with self.helpers.as_demo_user():
            response = self.test_client.delete(f"/case_updates/{case_update.update_id}")
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_post_state_policy(self) -> None:
        with self.helpers.as_officer(self.officer):
            response = self.test_client.post(
                "/policy_requirements_for_state", json={"state": "US_ID"}
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)

    def test_set_preferred_name(self) -> None:
        with self.helpers.as_officer(self.officer):
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertTrue("preferredName" not in client_info)

            # Set preferred name
            self.helpers.set_preferred_name(
                self.client_1.person_external_id, "Preferred"
            )
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertEqual(client_info["preferredName"], "Preferred")

            # Unset preferred name
            self.helpers.set_preferred_name(self.client_1.person_external_id, None)
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertTrue("preferredName" not in client_info)

    def test_set_preferred_contact(self) -> None:
        with self.helpers.as_officer(self.officer):
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertTrue("preferredContactMethod" not in client_info)

            # Set preferred contact method
            self.helpers.set_preferred_contact_method(
                self.client_1.person_external_id, PreferredContactMethod.Call
            )
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertEqual(client_info["preferredContactMethod"], "CALL")

            # Unset preferred contact fails
            response = self.test_client.post(
                "/set_preferred_contact_method",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "contactMethod": None,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_set_receiving_ssi_or_disability_income(self) -> None:
        with self.helpers.as_officer(self.officer):
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertFalse(client_info["receivingSSIOrDisabilityIncome"])

            # Set receiving SSI/disability income
            self.helpers.set_receiving_ssi_or_disability_income(
                self.client_1.person_external_id, True
            )
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertTrue(client_info["receivingSSIOrDisabilityIncome"])

            # Unset receiving SSI/disability income
            self.helpers.set_receiving_ssi_or_disability_income(
                self.client_1.person_external_id, False
            )
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertFalse(client_info["receivingSSIOrDisabilityIncome"])

    def test_cannot_set_preferred_values_for_non_clients(self) -> None:
        with self.helpers.as_officer(self.officer_without_clients):
            # Test that you cannot set preferred contact
            response = self.test_client.post(
                "/set_preferred_contact_method",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "contactMethod": PreferredContactMethod.Call.value,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

            # Test that you cannot set preferred name
            response = self.test_client.post(
                "/set_preferred_name",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "name": "Preferred",
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

            # Test that you cannot mark as receiving SSI/disability income
            response = self.test_client.post(
                "/set_receiving_ssi_or_disability_income",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "markReceiving": True,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_ssi_disability_satisfies_employment_opportunity(self) -> None:
        with self.helpers.as_officer(self.officer):
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertFalse(client_info["receivingSSIOrDisabilityIncome"])

            # Check that an employment opportunity exists
            opportunities = self.helpers.get_opportunities()
            employment_opportunity = None
            for opportunity in opportunities:
                if (
                    opportunity["personExternalId"] == self.client_1.person_external_id
                    and opportunity["opportunityType"]
                    == OpportunityType.EMPLOYMENT.value
                ):
                    employment_opportunity = opportunity
                    break
            self.assertIsNotNone(employment_opportunity)

            # Set receiving SSI/disability income
            self.helpers.set_receiving_ssi_or_disability_income(
                self.client_1.person_external_id, True
            )

            # Check that employment opportunity does not exist
            opportunities = self.helpers.get_opportunities()
            employment_opportunity = None
            for opportunity in opportunities:
                if (
                    opportunity["personExternalId"] == self.client_1.person_external_id
                    and opportunity["opportunityType"]
                    == OpportunityType.EMPLOYMENT.value
                ):
                    employment_opportunity = opportunity
                    break
            self.assertIsNone(employment_opportunity)

    def test_create_note(self) -> None:
        with self.helpers.as_officer(self.officer):
            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertEqual(client_info["notes"], [])

            # Test note creation
            note_id = self.helpers.create_note(
                self.client_1.person_external_id, "Demo user note."
            )

            client_info = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertEqual(len(client_info["notes"]), 1)
            self.assertEqual(client_info["notes"][0]["noteId"], note_id)

    def test_resolve_note(self) -> None:
        with self.helpers.as_officer(self.officer):
            note_id = self.helpers.create_note(
                self.client_1.person_external_id, "Demo user note."
            )

            # Not resolved initially
            note = self.helpers.find_note_for_person(
                self.client_1.person_external_id, note_id
            )
            self.assertFalse(note["resolved"])

            # Becomes resolved
            self.helpers.resolve_note(note_id, True)
            note = self.helpers.find_note_for_person(
                self.client_1.person_external_id, note_id
            )
            self.assertTrue(note["resolved"])

            # Un-resolve it
            self.helpers.resolve_note(note_id, False)
            note = self.helpers.find_note_for_person(
                self.client_1.person_external_id, note_id
            )
            self.assertFalse(note["resolved"])

    def test_update_note(self) -> None:
        with self.helpers.as_officer(self.officer):
            note_id = self.helpers.create_note(
                self.client_1.person_external_id, "Demo user note."
            )

            # Has initial text
            note = self.helpers.find_note_for_person(
                self.client_1.person_external_id, note_id
            )
            self.assertEqual(note["text"], "Demo user note.")

            # Text is updated
            self.helpers.update_note(note_id, "Updated!")
            note = self.helpers.find_note_for_person(
                self.client_1.person_external_id, note_id
            )
            self.assertEqual(note["text"], "Updated!")

    def test_no_empty_notes(self) -> None:
        with self.helpers.as_officer(self.officer):
            # Check that creating an empty note fails
            response = self.test_client.post(
                "/create_note",
                json={
                    "personExternalId": self.officer.external_id,
                    "text": "",
                },
            )
            self.assertEqual(
                response.status_code, HTTPStatus.BAD_REQUEST, response.get_json()
            )

            # Check that updating to empty fails
            note_id = self.helpers.create_note(
                self.client_1.person_external_id, "Demo user note."
            )

            response = self.test_client.post(
                "/update_note",
                json={
                    "noteId": note_id,
                    "text": "",
                },
            )
            self.assertEqual(
                response.status_code, HTTPStatus.BAD_REQUEST, response.get_json()
            )

    def test_notes_sql_injection_gut_check(self) -> None:
        with self.helpers.as_officer(self.officer):
            attempted_injection = "foo'; DROP TABLE officer_notes; SELECT * FROM etl_clients WHERE state_code != 'foo"
            note_id = self.helpers.create_note(
                self.client_1.person_external_id,
                attempted_injection,
            )

            # Make sure we can still access the raw note
            note = self.helpers.find_note_for_person(
                self.client_1.person_external_id, note_id
            )
            self.assertEqual(note["text"], attempted_injection)


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
