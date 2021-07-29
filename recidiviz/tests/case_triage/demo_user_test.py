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
from datetime import date
from http import HTTPStatus
from typing import Optional
from unittest import TestCase

import pytest
from flask import Flask

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.demo_helpers import (
    get_fixture_clients,
    get_fixture_opportunities,
    unconvert_fake_person_id_for_demo_user,
)
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.case_triage.scoped_sessions import setup_scoped_sessions
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.case_triage.api_test_helpers import CaseTriageTestHelpers
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
        self.helpers = CaseTriageTestHelpers.from_test(self, self.test_app)

        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_postgres_helpers.update_local_sqlalchemy_postgres_env_vars()
        )
        db_url = local_postgres_helpers.postgres_db_url_from_env_vars()
        engine = setup_scoped_sessions(self.test_app, db_url)
        # Auto-generate all tables that exist in our schema in this database
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.database_key.declarative_meta.metadata.create_all(engine)

        self.demo_clients = get_fixture_clients()
        self.demo_opportunities = get_fixture_opportunities()

        self.client_1 = self.demo_clients[0]

        with self.helpers.using_demo_user():
            self.helpers.create_case_update(
                self.client_1.person_external_id,
                CaseUpdateActionType.COMPLETED_ASSESSMENT.value,
            )

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

    def test_fixture_time_shift(self) -> None:
        """Spot-check that fixture data is properly time-shifted
        relative to the current date."""
        test_client = next(
            (c for c in self.demo_clients if c.person_external_id == "227")
        )
        self.assertEqual(test_client.most_recent_face_to_face_date, date.today())

    def test_get_clients(self) -> None:
        with self.helpers.using_demo_user():
            self.assertEqual(len(self.helpers.get_clients()), len(self.demo_clients))

            client = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertTrue(len(client["caseUpdates"]) == 1)

    def test_create_case_updates_action(self) -> None:
        with self.helpers.using_demo_user():
            all_clients = self.helpers.get_clients()
            client_to_modify = None
            for client in all_clients:
                if (
                    unconvert_fake_person_id_for_demo_user(client["personExternalId"])
                    != self.client_1.person_external_id
                ):
                    client_to_modify = client
                    break
            assert client_to_modify is not None
            self.assertEqual(client_to_modify["caseUpdates"], {})

            action = CaseUpdateActionType.FOUND_EMPLOYMENT.value
            self.helpers.create_case_update(
                client_to_modify["personExternalId"], action
            )

            new_client = self.helpers.find_client_in_api_response(
                client_to_modify["personExternalId"]
            )
            self.assertIn(
                action,
                new_client["caseUpdates"],
            )

    def test_delete_case_updates_action(self) -> None:
        with self.helpers.using_demo_user() as test_client:
            client_to_modify = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertTrue(len(client_to_modify["caseUpdates"]) == 1)

            case_update_id = client_to_modify["caseUpdates"][
                CaseUpdateActionType.COMPLETED_ASSESSMENT.value
            ]["updateId"]

            response = test_client.delete(f"/api/case_updates/{case_update_id}")
            self.assertEqual(response.status_code, HTTPStatus.OK)

            new_client = self.helpers.find_client_in_api_response(
                self.client_1.person_external_id
            )
            self.assertTrue(len(new_client["caseUpdates"]) == 0)

    def test_get_opportunities(self) -> None:
        with self.helpers.using_demo_user():
            # these numbers reflect the conditions represented in data fixtures
            # that result in opportunities
            num_unemployed = 58
            num_assessment_overdue = 31
            num_assessment_upcoming = 2
            num_contact_overdue = 4
            num_contact_upcoming = 1

            expected_opportunity_count = sum(
                [
                    len(self.demo_opportunities),
                    num_unemployed,
                    num_assessment_overdue,
                    num_assessment_upcoming,
                    num_contact_overdue,
                    num_contact_upcoming,
                ]
            )

            self.assertEqual(
                len(self.helpers.get_opportunities()), expected_opportunity_count
            )

    def test_defer_opportunity(self) -> None:
        with self.helpers.using_demo_user():
            prior_opportunity_count = len(self.helpers.get_undeferred_opportunities())

            opportunity = self.demo_opportunities[0]
            self.helpers.defer_opportunity(
                opportunity.person_external_id,
                opportunity.opportunity_type,
            )

            # There should be one fewer available opportunity post-deferral
            self.assertEqual(
                len(self.helpers.get_undeferred_opportunities()),
                prior_opportunity_count - 1,
            )

    def test_delete_opportunity_deferral(self) -> None:
        with self.helpers.using_demo_user():
            opportunity = self.demo_opportunities[0]
            self.helpers.defer_opportunity(
                opportunity.person_external_id,
                opportunity.opportunity_type,
            )

            api_opportunities = self.helpers.get_opportunities()
            deferral_id = None
            for api_opp in api_opportunities:
                if deferral_id := api_opp.get("deferralId"):
                    break
            assert deferral_id is not None

            self.helpers.delete_opportunity_deferral(deferral_id)

            # After deleting the deferral, all opportunities should be available
            self.assertEqual(
                len(self.helpers.get_undeferred_opportunities()),
                len(api_opportunities),
            )

    def test_set_preferred_name(self) -> None:
        with self.helpers.using_demo_user():
            client = self.demo_clients[0]

            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertTrue("preferredName" not in client_info)

            # Set preferred name
            self.helpers.set_preferred_name(client.person_external_id, "Preferred")
            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertEqual(client_info["preferredName"], "Preferred")

            # Unset preferred name
            self.helpers.set_preferred_name(client.person_external_id, None)
            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertTrue("preferredName" not in client_info)

    def test_set_preferred_contact(self) -> None:
        with self.helpers.using_demo_user() as test_client:
            client = self.demo_clients[0]

            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertTrue("preferredContactMethod" not in client_info)

            # Set preferred name
            self.helpers.set_preferred_contact_method(
                client.person_external_id, PreferredContactMethod.Call
            )
            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertEqual(client_info["preferredContactMethod"], "CALL")

            # Unset preferred contact fails
            response = test_client.post(
                "/api/set_preferred_contact_method",
                json={
                    "personExternalId": client.person_external_id,
                    "contactMethod": None,
                },
            )
            self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_set_receiving_ssi_or_disability_income(self) -> None:
        with self.helpers.using_demo_user():
            client = self.demo_clients[0]

            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertFalse(client_info["receivingSSIOrDisabilityIncome"])

            # Set receiving SSI/disability income
            self.helpers.set_receiving_ssi_or_disability_income(
                client.person_external_id, True
            )
            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertTrue(client_info["receivingSSIOrDisabilityIncome"])

            # Unset receiving SSI/disability income
            self.helpers.set_receiving_ssi_or_disability_income(
                client.person_external_id, False
            )
            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertFalse(client_info["receivingSSIOrDisabilityIncome"])

    def test_ssi_disability_satisfies_employment_opportunity(self) -> None:
        with self.helpers.using_demo_user():
            client = self.demo_clients[0]

            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertFalse(client_info["receivingSSIOrDisabilityIncome"])

            # Check that an employment opportunity exists
            opportunities = self.helpers.get_opportunities()
            employment_opportunity = None
            for opportunity in opportunities:
                if (
                    opportunity["personExternalId"] == client.person_external_id
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
                    opportunity["personExternalId"] == client.person_external_id
                    and opportunity["opportunityType"]
                    == OpportunityType.EMPLOYMENT.value
                ):
                    employment_opportunity = opportunity
                    break
            self.assertIsNone(employment_opportunity)

    def test_notes(self) -> None:
        with self.helpers.using_demo_user():
            client = self.demo_clients[0]

            client_info = self.helpers.find_client_in_api_response(
                client.person_external_id
            )
            self.assertEqual(client_info["notes"], [])

            note_id = self.helpers.create_note(
                client.person_external_id, "Demo user note."
            )

            # If this fetch doesn't crash, the note was created successfully.
            _ = self.helpers.find_note_for_person(client.person_external_id, note_id)

            # Check that updates work
            self.helpers.update_note(note_id, "New text")
            note = self.helpers.find_note_for_person(client.person_external_id, note_id)
            self.assertEqual(note["text"], "New text")

            # Check that resolutions work
            self.helpers.resolve_note(note_id, True)
            note = self.helpers.find_note_for_person(client.person_external_id, note_id)
            self.assertTrue(note["resolved"])
            self.helpers.resolve_note(note_id, False)
            note = self.helpers.find_note_for_person(client.person_external_id, note_id)
            self.assertFalse(note["resolved"])
