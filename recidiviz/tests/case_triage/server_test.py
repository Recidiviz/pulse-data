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
from typing import Any, Dict, List, Optional, Tuple
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch
from urllib import parse

import pytest
from flask import Flask, Response, g, jsonify, session
from freezegun import freeze_time
from sqlalchemy.orm import Session

from recidiviz.case_triage.api_routes import IMPERSONATED_EMAIL_KEY
from recidiviz.case_triage.authorization import AccessPermissions, AuthorizationStore
from recidiviz.case_triage.case_updates.serializers import serialize_client_case_version
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.client_event.types import ClientEventType
from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityType,
)
from recidiviz.case_triage.querier.case_update_presenter import CaseUpdateStatus
from recidiviz.case_triage.querier.querier import OfficerDoesNotExistError
from recidiviz.case_triage.user_context import REGISTRATION_DATE_CLAIM, UserContext
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClientEvent,
    ETLOfficer,
    OfficerMetadata,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.tests.case_triage.api_test_helpers import CaseTriageTestHelpers
from recidiviz.tests.case_triage.case_triage_helpers import (
    generate_fake_case_update,
    generate_fake_client,
    generate_fake_client_info,
    generate_fake_etl_opportunity,
    generate_fake_officer,
    generate_fake_reminder,
    hash_email,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.types import assert_type


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
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "test-project"

        self.helpers = CaseTriageTestHelpers.from_test(self, self.test_app)
        self.test_client = self.helpers.test_client
        self.mock_segment_client = self.helpers.mock_segment_client

        schema_type = SchemaType.CASE_TRIAGE
        self.database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
        self.overridden_env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        db_url = local_postgres_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.test_app, schema_type, db_url)
        # Auto-generate all tables that exist in our schema in this database
        self.database_key.declarative_meta.metadata.create_all(engine)
        # `flask_sqlalchemy_session` sets the `scoped_session` attribute on the app,
        # even though this is not specified in the types for `app`.
        self.session = self.test_app.scoped_session  # type: ignore[attr-defined]

        # Add seed data
        self.officer = generate_fake_officer(
            "officer_id_1", "officer1@not-recidiviz.org"
        )
        self.officer_without_clients = generate_fake_officer(
            "officer_id_2", "officer2@not-recidiviz.org"
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

        self.client_events = [
            ETLClientEvent(
                state_code=self.client_1.state_code,
                person_external_id=self.client_1.person_external_id,
                # note that these are not in proper descending date order
                event_date=date.today() - timedelta(days=2),
                event_type=ClientEventType.ASSESSMENT.value,
                event_metadata={
                    "assessment_score": 10,
                    "previous_assessment_score": 9,
                },
            ),
            ETLClientEvent(
                state_code=self.client_1.state_code,
                person_external_id=self.client_1.person_external_id,
                event_date=date.today() - timedelta(days=1),
                event_type=ClientEventType.ASSESSMENT.value,
                event_metadata={
                    "assessment_score": 5,
                    "previous_assessment_score": 10,
                },
            ),
            ETLClientEvent(
                state_code=self.client_2.state_code,
                person_external_id=self.client_2.person_external_id,
                event_date=date.today() - timedelta(days=1),
                event_type=ClientEventType.ASSESSMENT.value,
                event_metadata={
                    "assessment_score": 5,
                    "previous_assessment_score": 10,
                },
            ),
        ]

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
                *self.client_events,
            ]
        )
        self.session.commit()

    def tearDown(self) -> None:
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.database_key)

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_no_clients(self) -> None:
        with self.helpers.using_officer(self.officer_without_clients):
            self.assertEqual(self.helpers.get_clients(), [])

    def test_get_clients(self) -> None:
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer_without_clients):
            self.assertEqual([], self.helpers.get_opportunities())

    @freeze_time("2021-12-01")
    def test_get_opportunities(self) -> None:

        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer) as test_client:
            # GET instead of POST
            response = self.test_client.get("/api/opportunity_deferrals")
            self.assertEqual(response.status_code, HTTPStatus.METHOD_NOT_ALLOWED)

            # `personExternalId` doesn't map to a real person
            response = test_client.post(
                "/api/opportunity_deferrals",
                json={
                    "personExternalId": "nonexistent-person",
                    "opportunityType": OpportunityType.EARLY_DISCHARGE.value,
                    "deferralType": OpportunityDeferralType.REMINDER.value,
                    "deferUntil": str(datetime.now()),
                    "requestReminder": True,
                },
            )

            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                assert_type(response.get_json(), dict)["code"], "not_found"
            )
            self.assertIn(
                "does not correspond to a known person",
                assert_type(response.get_json(), dict)["description"][
                    "personExternalId"
                ],
            )

    def test_defer_computed_opportunity_successful(self) -> None:
        with self.helpers.using_officer(self.officer):
            # Employment is a ComputedOpportunity and not actually stored
            # in the database.
            opportunity_type = OpportunityType.EMPLOYMENT.value
            person_external_id = self.client_1.person_external_id

            # Verify that the opportunity is initially present
            self.assertEqual(
                len(
                    self.helpers.get_opportunity_for_person(
                        opportunity_type, person_external_id
                    )
                ),
                1,
            )

            # Submit API request
            self.helpers.defer_opportunity(person_external_id, opportunity_type)
            self.mock_segment_client.track_opportunity_deferred.assert_called()

            # Verify that the opportunity is deferred
            self.assertEqual(
                len(
                    self.helpers.get_opportunity_for_person(
                        opportunity_type, person_external_id
                    )
                ),
                0,
            )

            # Deferring a second time does not raise
            self.helpers.defer_opportunity(person_external_id, opportunity_type)

    def test_defer_opportunity_successful(self) -> None:
        with self.helpers.using_officer(self.officer):
            opportunity_type = self.opportunity_2.opportunity_type
            person_external_id = self.client_2.person_external_id

            # Submit API request
            self.helpers.defer_opportunity(person_external_id, opportunity_type)
            self.mock_segment_client.track_opportunity_deferred.assert_called()

            # Verify that the opportunity is deferred
            self.assertEqual(
                len(
                    self.helpers.get_opportunity_for_person(
                        opportunity_type, person_external_id
                    )
                ),
                0,
            )

    @freeze_time("2021-12-01")
    def test_delete_opportunity_deferral(self) -> None:
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer_without_clients) as test_client:
            response = test_client.delete(
                f"/api/opportunity_deferrals/{self.deferral_1.deferral_id}"
            )
            self.assertEqual(
                response.status_code,
                HTTPStatus.BAD_REQUEST,
                assert_type(response.get_json(), dict),
            )

    def test_case_updates_malformed_input(self) -> None:
        with self.helpers.using_officer(self.officer) as test_client:
            # GET instead of POST
            response = test_client.get("/api/case_updates")
            self.assertEqual(response.status_code, HTTPStatus.METHOD_NOT_ALLOWED)

            # `personExternalId` doesn't map to a real person
            response = test_client.post(
                "/api/case_updates",
                json={
                    "personExternalId": "nonexistent-person",
                    "actionType": CaseUpdateActionType.NOT_ON_CASELOAD.value,
                },
            )

            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
            self.assertEqual(
                assert_type(response.get_json(), dict)["code"], "not_found"
            )
            self.assertIn(
                "does not correspond to a known person",
                assert_type(response.get_json(), dict)["description"][
                    "personExternalId"
                ],
            )

    def test_case_updates_success(self) -> None:
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer) as test_client:

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

            response = test_client.delete(f"/api/case_updates/{case_update.update_id}")
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
        with self.helpers.using_officer(self.officer_without_clients) as test_client:
            response = test_client.delete(f"/api/case_updates/{case_update.update_id}")
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

        # No signed in user
        with self.helpers.using_demo_user() as test_client:
            response = test_client.delete(f"/api/case_updates/{case_update.update_id}")
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)

    def test_post_state_policy(self) -> None:
        with self.helpers.using_officer(self.officer) as test_client:
            response = test_client.post(
                "/api/policy_requirements_for_state", json={"state": "US_ID"}
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)

    def test_set_preferred_name(self) -> None:
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer) as test_client:
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
            response = test_client.post(
                "/api/set_preferred_contact_method",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "contactMethod": None,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_set_receiving_ssi_or_disability_income(self) -> None:
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer_without_clients) as test_client:
            # Test that you cannot set preferred contact
            response = test_client.post(
                "/api/set_preferred_contact_method",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "contactMethod": PreferredContactMethod.Call.value,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

            # Test that you cannot set preferred name
            response = test_client.post(
                "/api/set_preferred_name",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "name": "Preferred",
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

            # Test that you cannot mark as receiving SSI/disability income
            response = test_client.post(
                "/api/set_receiving_ssi_or_disability_income",
                json={
                    "personExternalId": self.client_1.person_external_id,
                    "markReceiving": True,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_ssi_disability_satisfies_employment_opportunity(self) -> None:
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer):
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
        with self.helpers.using_officer(self.officer) as test_client:
            # Check that creating an empty note fails
            response = test_client.post(
                "/api/create_note",
                json={
                    "personExternalId": self.officer.external_id,
                    "text": "",
                },
            )
            self.assertEqual(
                response.status_code,
                HTTPStatus.BAD_REQUEST,
                assert_type(response.get_json(), dict),
            )

            # Check that updating to empty fails
            note_id = self.helpers.create_note(
                self.client_1.person_external_id, "Demo user note."
            )

            response = test_client.post(
                "/api/update_note",
                json={
                    "noteId": note_id,
                    "text": "",
                },
            )
            self.assertEqual(
                response.status_code,
                HTTPStatus.BAD_REQUEST,
                assert_type(response.get_json(), dict),
            )

    def test_get_client_events(self) -> None:
        with self.helpers.using_officer(self.officer):
            self.assertEqual(
                len(
                    self.helpers.get_events_for_client(self.client_1.person_external_id)
                ),
                2,
            )
            self.assertEqual(
                len(
                    self.helpers.get_events_for_client(self.client_2.person_external_id)
                ),
                1,
            )
            self.assertEqual(
                len(
                    self.helpers.get_events_for_client(self.client_3.person_external_id)
                ),
                0,
            )

    def test_notes_sql_injection_gut_check(self) -> None:
        with self.helpers.using_officer(self.officer):
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

    def test_post_set_has_seen_onboarding(self) -> None:
        with self.helpers.using_officer(self.officer):
            response = self.helpers.post_set_has_seen_onboarding(
                has_seen_onboarding=True
            )
            self.assertEqual(response["status"], "ok")

        with self.helpers.using_demo_user():
            response = self.helpers.post_set_has_seen_onboarding(
                has_seen_onboarding=True
            )
            self.assertEqual(response["status"], "ok")

    def test_readonly_access_to_endpoints(self) -> None:
        with self.helpers.using_readonly_user(
            self.officer.email_address
        ) as test_client:
            read_operations: List[str] = ["/api/clients", "/api/opportunities"]
            post_operations: List[Tuple[str, Dict[str, Any]]] = [
                (
                    "/api/create_note",
                    {
                        "personExternalId": self.client_1.person_external_id,
                        "text": "test_text",
                    },
                ),
                (
                    "/api/opportunity_deferrals",
                    {
                        "personExternalId": self.client_2.person_external_id,
                        "opportunityType": OpportunityType.EARLY_DISCHARGE.value,
                        "deferralType": OpportunityDeferralType.REMINDER.value,
                        "deferUntil": str(datetime.now() + timedelta(days=1)),
                        "requestReminder": True,
                    },
                ),
                (
                    "/api/case_updates",
                    {
                        "personExternalId": self.client_3.person_external_id,
                        "actionType": CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
                        "comment": "test",
                    },
                ),
            ]
            delete_operations: List[str] = [
                f"/api/opportunity_deferrals/{self.deferral_1.deferral_id}",
                f"/api/case_updates/{self.case_update_1.update_id}",
            ]
            for read_endpoint in read_operations:
                response = test_client.get(read_endpoint)
                self.assertEqual(response.status_code, HTTPStatus.OK)

            for post_endpoint, json in post_operations:
                response = test_client.post(post_endpoint, json=json)
                self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

            for delete_endpoint in delete_operations:
                response = test_client.delete(delete_endpoint)
                self.assertEqual(response.status_code, HTTPStatus.UNAUTHORIZED)

    @patch("recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_email")
    @patch(
        "recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_hashed_email"
    )
    def test_fetch_user_info_correct_officer_when_impersonating(
        self,
        mock_officer_for_hashed_email: MagicMock,
        mock_officer_for_email: MagicMock,
    ) -> None:
        def mock_officer_hash(_session: Session, hashed_email: str) -> ETLOfficer:
            if hashed_email == self.officer.hashed_email_address:
                return self.officer
            return generate_fake_officer("test", self.officer.email_address)

        def mock_officer(_session: Session, email: str) -> ETLOfficer:
            if email == self.officer.email_address:
                return self.officer
            return generate_fake_officer("test", email)

        mock_officer_for_email.side_effect = mock_officer
        mock_officer_for_hashed_email.side_effect = mock_officer_hash
        with self.helpers.using_readonly_user(
            self.officer.email_address
        ) as test_client:
            self.test_client.get("/api/clients")
            # Impersonated user
            self.assertIn(
                self.officer.hashed_email_address,
                mock_officer_for_hashed_email.call_args_list[0].args,
            )

        with self.helpers.using_ordinary_user("admin@not-recidiviz.org") as test_client:
            test_client.get("/api/clients")
            # Non-impersonated user
            self.assertIn(
                "admin@not-recidiviz.org", mock_officer_for_email.call_args_list[0].args
            )

    @patch("recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_email")
    @patch(
        "recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_hashed_email"
    )
    def test_fetch_user_info_defaults_to_own_if_impersonation_invalid(
        self,
        mock_officer_for_hashed_email: MagicMock,
        mock_officer_for_email: MagicMock,
    ) -> None:
        def mock_officer_for_hash(_session: Session, _hashed_email: str) -> ETLOfficer:
            raise OfficerDoesNotExistError

        def mock_officer(_session: Session, _email: str) -> ETLOfficer:
            return self.officer

        mock_officer_for_email.side_effect = mock_officer
        mock_officer_for_hashed_email.side_effect = mock_officer_for_hash
        with self.helpers.using_readonly_user(
            "not-found@not-recidiviz.org"
        ) as test_client:
            test_client.get("/api/clients")
            # Assert that we called both functions once
            self.assertEqual(mock_officer_for_email.call_count, 1)
            self.assertEqual(mock_officer_for_hashed_email.call_count, 1)
            # Assert that we attempted to call impersonated user, but this ran into exception
            self.assertIn(
                hash_email("not-found@not-recidiviz.org"),
                mock_officer_for_hashed_email.call_args_list[0].args,
            )
            # Assert that we default to fetching their own Case Triage officer if impersonation fails
            self.assertIn(
                "admin@not-recidiviz.org", mock_officer_for_email.call_args_list[0].args
            )

    @patch("recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_email")
    @patch(
        "recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_hashed_email"
    )
    def test_no_exception_if_both_impersonated_officer_and_self_not_found(
        self,
        mock_officer_for_hashed_email: MagicMock,
        mock_officer_for_email: MagicMock,
    ) -> None:
        def mock_officer_for_hash(_session: Session, _hashed_email: str) -> ETLOfficer:
            raise OfficerDoesNotExistError

        def mock_officer(_session: Session, _email: str) -> ETLOfficer:
            raise OfficerDoesNotExistError

        mock_officer_for_email.side_effect = mock_officer
        mock_officer_for_hashed_email.side_effect = mock_officer_for_hash
        auth_store = AuthorizationStore()
        with self.test_app.test_request_context():
            auth_store.case_triage_admin_users = ["not-found-admin@not-recidiviz.org"]
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {"email": "not-found-admin@not-recidiviz.org"}
            # TODO(PyCQA/pylint#5317): Remove ignore fixed by PyCQA/pylint#5457
            g.user_context = UserContext.base_context_for_email(  # pylint: disable=assigning-non-slot
                "not-found-admin@not-recidiviz.org", auth_store
            )
            g.user_context.jwt_claims[
                REGISTRATION_DATE_CLAIM
            ] = "2021-01-08T22:02:17.863Z"
            # Perform initial impersonation
            response = self.test_client.get(
                f"/api/bootstrap?{IMPERSONATED_EMAIL_KEY}={parse.quote(hash_email('non-existent@not-recidiviz.org'))}"
            )
            # Assert we called officer_for_email twice, once for the non-existent impersonated user, once for non-existent self
            self.assertEqual(mock_officer_for_email.call_count, 1)
            self.assertEqual(mock_officer_for_hashed_email.call_count, 1)
            self.assertIn(
                hash_email("non-existent@not-recidiviz.org"),
                mock_officer_for_hashed_email.call_args_list[0].args,
            )
            self.assertIn(
                "not-found-admin@not-recidiviz.org",
                mock_officer_for_email.call_args_list[0].args,
            )
            self.assertIsNone(g.user_context.current_user)
            with self.test_client.session_transaction() as sess:  # type: ignore
                self.assertTrue(IMPERSONATED_EMAIL_KEY not in session)
            self.assertEqual(
                response.status_code,
                HTTPStatus.OK,
                assert_type(response.get_json(), dict),
            )

    def test_unimpersonating_without_impersonating(self) -> None:
        auth_store = AuthorizationStore()
        auth_store.case_triage_admin_users = ["admin@not-recidiviz.org"]
        with self.test_app.test_request_context():
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {"email": "admin@not-recidiviz.org"}
            # TODO(PyCQA/pylint#5317): Remove ignore fixed by PyCQA/pylint#5457
            g.user_context = UserContext.base_context_for_email(  # pylint: disable=assigning-non-slot
                "admin@not-recidiviz.org", auth_store
            )
            # Perform initial impersonation
            response = self.test_client.delete("/api/impersonation")
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
        self.officer = generate_fake_officer(
            "officer_id_1", "officer1@not-recidiviz.org"
        )

        self.admin_email = "admin@not-recidiviz.org"
        self.non_admin_email = "non-admin@not-recidiviz.org"

        self.test_app = Flask(__name__)
        self.test_app.secret_key = "NOT-A-SECRET"
        self.helpers = CaseTriageTestHelpers.from_test(self, self.test_app)
        self.test_client = self.helpers.test_client

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

    @patch(
        "recidiviz.case_triage.officer_metadata.interface.OfficerMetadataInterface.get_officer_metadata"
    )
    @patch("recidiviz.case_triage.authorization.AuthorizationStore.can_impersonate")
    @patch("recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_email")
    @patch(
        "recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_hashed_email"
    )
    @patch(
        "recidiviz.case_triage.authorization.AuthorizationStore.get_access_permissions"
    )
    def test_non_admin(
        self,
        mock_get_access_permissions: MagicMock,
        mock_officer_for_hashed_email: MagicMock,
        mock_officer_for_email: MagicMock,
        mock_can_impersonate: MagicMock,
        mock_get_officer_metadata: MagicMock,
    ) -> None:
        officer = generate_fake_officer(officer_id="test", email=self.non_admin_email)

        mock_can_impersonate.return_value = False
        mock_officer_for_email.return_value = officer
        mock_officer_for_hashed_email.side_effect = OfficerDoesNotExistError
        mock_get_access_permissions.return_value = AccessPermissions(
            can_access_case_triage=True,
            can_access_leadership_dashboard=False,
            impersonatable_state_codes=set(),
        )
        mock_get_officer_metadata.return_value = OfficerMetadata.from_officer(officer)

        with self.test_app.test_request_context():
            # Test that if there's no given email, a non-admin falls through as if
            # impersonation didn't happen at all.
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {"email": self.non_admin_email}
            # TODO(PyCQA/pylint#5317): Remove ignore fixed by PyCQA/pylint#5457
            g.user_context = UserContext(  # pylint: disable=assigning-non-slot
                self.non_admin_email, self.auth_store
            )
            g.user_context.jwt_claims[
                REGISTRATION_DATE_CLAIM
            ] = "2021-01-08T22:02:17.863Z"
            response = self.test_client.get("/api/bootstrap")
            self.assertEqual(
                response.status_code,
                HTTPStatus.OK,
                assert_type(response.get_json(), dict),
            )

            # Test that if there's a given email, a non-admin also falls through as if
            # impersonation didn't happen at all and that key is no longer in the session.
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {"email": self.non_admin_email}
            # TODO(PyCQA/pylint#5317): Remove ignore fixed by PyCQA/pylint#5457
            g.user_context = UserContext(  # pylint: disable=assigning-non-slot
                self.non_admin_email, self.auth_store
            )
            g.user_context.jwt_claims[
                REGISTRATION_DATE_CLAIM
            ] = "2021-01-08T22:02:17.863Z"
            response = self.test_client.get(
                f"/api/bootstrap?{IMPERSONATED_EMAIL_KEY}={parse.quote(hash_email('user@not-recidiviz.org'))}"
            )
            with self.test_client.session_transaction() as sess:  # type: ignore
                self.assertTrue(IMPERSONATED_EMAIL_KEY not in session)
            self.assertEqual(
                response.status_code,
                HTTPStatus.OK,
                assert_type(response.get_json(), dict),
            )
            self.assertEqual(
                g.user_context.current_user.email_address, self.non_admin_email
            )

    @patch(
        "recidiviz.case_triage.officer_metadata.interface.OfficerMetadataInterface.get_officer_metadata"
    )
    @patch(
        "recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_hashed_email"
    )
    @patch(
        "recidiviz.case_triage.authorization.AuthorizationStore.get_access_permissions"
    )
    def test_happy_path(
        self,
        mock_get_access_permissions: MagicMock,
        mock_officer_for_hashed_email: MagicMock,
        mock_get_officer_metadata: MagicMock,
    ) -> None:
        officer = generate_fake_officer(
            officer_id="test",
            email=self.non_admin_email,
            state_code="US_XX",
        )

        officer_metadata = OfficerMetadata.from_officer(officer)

        access_permissions = AccessPermissions(
            can_access_case_triage=True,
            can_access_leadership_dashboard=False,
            impersonatable_state_codes={"US_XX"},
        )

        mock_officer_for_hashed_email.return_value = officer
        mock_get_officer_metadata.return_value = officer_metadata
        mock_get_access_permissions.return_value = access_permissions

        with self.test_app.test_request_context():
            self.auth_store.case_triage_admin_users = [self.admin_email]
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {"email": self.admin_email}
            # TODO(PyCQA/pylint#5317): Remove ignore fixed by PyCQA/pylint#5457
            g.user_context = UserContext(  # pylint: disable=assigning-non-slot
                self.admin_email, self.auth_store
            )
            g.user_context.jwt_claims[
                REGISTRATION_DATE_CLAIM
            ] = "2021-01-08T22:02:17.863Z"
            response = self.test_client.get(
                f"/api/bootstrap?{IMPERSONATED_EMAIL_KEY}={parse.quote(hash_email('non-admin@not-recidiviz.org'))}"
            )
            self.assertEqual(response.status_code, HTTPStatus.OK)

            with self.test_client.session_transaction() as sess:  # type: ignore
                self.assertEqual(
                    sess[IMPERSONATED_EMAIL_KEY],
                    hash_email("non-admin@not-recidiviz.org"),
                )
            self.assertEqual(
                g.user_context.current_user.email_address, "non-admin@not-recidiviz.org"
            )

    @patch("recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_email")
    @patch(
        "recidiviz.case_triage.querier.querier.CaseTriageQuerier.officer_for_hashed_email"
    )
    @patch(
        "recidiviz.case_triage.authorization.AuthorizationStore.get_access_permissions"
    )
    def test_remove_impersonation(
        self,
        mock_get_access_permissions: MagicMock,
        mock_officer_for_hashed_email: MagicMock,
        mock_officer_for_email: MagicMock,
    ) -> None:
        def test_officer_for_hashed_email(_: Session, _hashed_email: str) -> ETLOfficer:
            return generate_fake_officer(officer_id="test", email=self.non_admin_email)

        def test_officer_for_email(_: Session, email: str) -> ETLOfficer:
            return generate_fake_officer(officer_id="test", email=email)

        def test_get_access_permissions(_email: str) -> AccessPermissions:
            return AccessPermissions(
                can_access_case_triage=True,
                can_access_leadership_dashboard=False,
                impersonatable_state_codes={"US_ID"},
            )

        mock_officer_for_hashed_email.side_effect = test_officer_for_hashed_email
        mock_officer_for_email.side_effect = test_officer_for_email
        mock_get_access_permissions.side_effect = test_get_access_permissions
        with self.test_app.test_request_context():
            self.auth_store.case_triage_admin_users = [self.admin_email]
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {"email": self.admin_email}
            # TODO(PyCQA/pylint#5317): Remove ignore fixed by PyCQA/pylint#5457
            g.user_context = UserContext(  # pylint: disable=assigning-non-slot
                self.admin_email, self.auth_store
            )
            # Perform initial impersonation
            self.test_client.get(
                f"/api/bootstrap?{IMPERSONATED_EMAIL_KEY}={parse.quote(hash_email(self.non_admin_email))}"
            )

            # Undo impersonation
            response = self.test_client.delete("/api/impersonation")
            self.assertEqual(response.status_code, HTTPStatus.OK)
            with self.test_client.session_transaction() as sess:  # type: ignore
                self.assertTrue(IMPERSONATED_EMAIL_KEY not in sess)

    def test_no_user_context(self) -> None:
        with self.test_app.test_request_context():
            response = self.test_client.get(
                f"/impersonate_user?{IMPERSONATED_EMAIL_KEY}={parse.quote(self.non_admin_email)}"
            )
            self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
