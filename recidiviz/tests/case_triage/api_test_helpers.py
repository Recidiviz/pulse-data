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
""" Test helpers for the Case Triage app """
import contextlib
from datetime import datetime, timedelta
from functools import wraps
from http import HTTPStatus
from typing import Any, Callable, Dict, Generator, List, Optional
from unittest import TestCase

import attr
from flask import Flask
from flask.testing import FlaskClient
from mock import MagicMock

from recidiviz.case_triage.admin_flask_views import IMPERSONATED_EMAIL_KEY
from recidiviz.case_triage.api_routes import create_api_blueprint
from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.persistence.database.schema.case_triage.schema import ETLOfficer

DEMO_USER_EMAIL = "demo_user@recidiviz.org"
ADMIN_USER_EMAIL = "admin@recidiviz.org"


def passthrough_authorization_decorator() -> Callable:
    def decorated(route: Callable) -> Callable:
        @wraps(route)
        def inner(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            return route(*args, **kwargs)

        return inner

    return decorated


@attr.s
class CaseTriageTestHelpers:
    """Helpers for our Case Triage API tests"""

    test: TestCase = attr.ib()
    test_app: Flask = attr.ib()

    def __attrs_post_init__(self) -> None:
        register_error_handlers(self.test_app)
        self.test_app.secret_key = "NOT-A-SECRET"
        self.mock_segment_client = MagicMock()
        self.mock_auth_store = AuthorizationStore()
        self.mock_auth_store.demo_users = [DEMO_USER_EMAIL]
        self.mock_auth_store.admin_users = [ADMIN_USER_EMAIL]
        api = create_api_blueprint(
            self.mock_segment_client,
            passthrough_authorization_decorator(),
            self.mock_auth_store,
        )
        self.test_app.register_blueprint(api)
        self.test_client = self.test_app.test_client()

    def set_session_user_info(self, email: str) -> None:
        with self.test_client.session_transaction() as sess:  # type: ignore
            sess["user_info"] = {"email": email}

    @contextlib.contextmanager
    def using_demo_user(self) -> Generator[FlaskClient, None, None]:
        with self.test_app.test_request_context():
            self.set_session_user_info(DEMO_USER_EMAIL)

            yield self.test_client

    @contextlib.contextmanager
    def using_officer(self, officer: ETLOfficer) -> Generator[FlaskClient, None, None]:
        with self.test_app.test_request_context():
            self.set_session_user_info(officer.email_address)

            yield self.test_client

    @contextlib.contextmanager
    def using_readonly_user(
        self, officer: ETLOfficer
    ) -> Generator[FlaskClient, None, None]:
        with self.test_app.test_request_context():
            with self.test_client.session_transaction() as sess:  # type: ignore
                sess["user_info"] = {"email": ADMIN_USER_EMAIL}
                sess[IMPERSONATED_EMAIL_KEY] = officer.email_address

            yield self.test_client

    def get_clients(self) -> List[Dict[Any, Any]]:
        response = self.test_client.get("/clients")
        self.test.assertEqual(response.status_code, HTTPStatus.OK)
        return response.get_json()

    def get_opportunities(self) -> List[Dict[Any, Any]]:
        response = self.test_client.get("/opportunities")
        self.test.assertEqual(response.status_code, HTTPStatus.OK)
        return response.get_json()

    def get_undeferred_opportunities(self) -> List[Dict[Any, Any]]:
        all_opportunities = self.get_opportunities()
        return [opp for opp in all_opportunities if "deferredUntil" not in opp]

    def create_case_update(
        self, person_external_id: str, action_type: str, comment: str = ""
    ) -> None:
        response = self.test_client.post(
            "/case_updates",
            json={
                "personExternalId": person_external_id,
                "actionType": action_type,
                "comment": comment,
            },
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())
        self.test.assertIsNotNone(response.get_json()["updateId"])

    def create_note(self, person_external_id: str, text: str) -> str:
        response = self.test_client.post(
            "/create_note",
            json={
                "personExternalId": person_external_id,
                "text": text,
            },
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())
        note_id = response.get_json()["noteId"]
        self.test.assertIsNotNone(note_id)
        return note_id

    def defer_opportunity(
        self,
        person_external_id: str,
        opportunity_type: str,
        deferral_type: str = "REMINDER",
    ) -> None:
        response = self.test_client.post(
            "/opportunity_deferrals",
            json={
                "personExternalId": person_external_id,
                "opportunityType": opportunity_type,
                "deferralType": deferral_type,
                "deferUntil": str(datetime.now() + timedelta(days=1)),
                "requestReminder": True,
            },
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())

    def delete_opportunity_deferral(
        self,
        deferral_id: str,
    ) -> None:
        response = self.test_client.delete(f"/opportunity_deferrals/{deferral_id}")
        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())

    def find_client_in_api_response(self, person_external_id: str) -> Dict[Any, Any]:
        client_json = self.get_clients()

        for data in client_json:
            if data["personExternalId"] == person_external_id:
                return data

        raise ValueError(f"Could not find client {person_external_id} in response")

    def find_note_for_person(
        self, person_external_id: str, note_id: str
    ) -> Dict[str, Any]:
        client = self.find_client_in_api_response(person_external_id)

        for note in client["notes"]:
            if note["noteId"] == note_id:
                return note

        raise ValueError(f"Could not find {note_id=} for {person_external_id=}")

    def resolve_note(self, note_id: str, is_resolved: bool) -> None:
        response = self.test_client.post(
            "/resolve_note",
            json={"noteId": note_id, "isResolved": is_resolved},
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())

    def set_preferred_contact_method(
        self, person_external_id: str, contact_method: PreferredContactMethod
    ) -> None:
        response = self.test_client.post(
            "/set_preferred_contact_method",
            json={
                "personExternalId": person_external_id,
                "contactMethod": contact_method.value,
            },
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())

    def set_preferred_name(self, person_external_id: str, name: Optional[str]) -> None:
        response = self.test_client.post(
            "/set_preferred_name",
            json={
                "personExternalId": person_external_id,
                "name": name,
            },
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())

    def set_receiving_ssi_or_disability_income(
        self, person_external_id: str, mark_receiving: bool
    ) -> None:
        response = self.test_client.post(
            "/set_receiving_ssi_or_disability_income",
            json={
                "personExternalId": person_external_id,
                "markReceiving": mark_receiving,
            },
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())

    def update_note(self, note_id: str, text: str) -> None:
        response = self.test_client.post(
            "/update_note",
            json={
                "noteId": note_id,
                "text": text,
            },
        )

        self.test.assertEqual(response.status_code, HTTPStatus.OK, response.get_json())

    @staticmethod
    def from_test(test: TestCase, test_app: Flask) -> "CaseTriageTestHelpers":
        return CaseTriageTestHelpers(test, test_app)
