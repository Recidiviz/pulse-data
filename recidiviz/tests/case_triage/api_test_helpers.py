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
from http import HTTPStatus
from typing import List, Dict, Any, Generator
from unittest import TestCase

import attr
from flask import Flask, g
from flask.testing import FlaskClient

from recidiviz.persistence.database.schema.case_triage.schema import ETLOfficer


@attr.s
class CaseTriageTestHelpers:
    """ Helpers for our Case Triage API tests"""

    test: TestCase = attr.ib()
    test_app: Flask = attr.ib()
    test_client: FlaskClient = attr.ib()

    @contextlib.contextmanager
    def as_demo_user(self) -> Generator[None, None, None]:
        with self.test_app.test_request_context():
            g.current_user = None
            g.email = "demo_user@recidiviz.org"
            g.can_see_demo_data = True

            yield

    @contextlib.contextmanager
    def as_officer(self, officer: ETLOfficer) -> Generator[None, None, None]:
        with self.test_app.test_request_context():
            g.current_user = officer
            g.email = "demo_user@recidiviz.org"
            g.can_see_demo_data = True

            yield

    def get_clients(self) -> List[Dict[Any, Any]]:
        response = self.test_client.get("/clients")
        self.test.assertEqual(response.status_code, HTTPStatus.OK)
        return response.get_json()

    def get_opportunities(self) -> List[Dict[Any, Any]]:
        response = self.test_client.get("/opportunities")
        self.test.assertEqual(response.status_code, HTTPStatus.OK)
        return response.get_json()

    def find_client_in_api_response(self, person_external_id: str) -> Dict[Any, Any]:
        client_json = self.get_clients()

        for data in client_json:
            if data["personExternalId"] == person_external_id:
                return data

        raise ValueError(f"Could not find client {person_external_id} in response")

    @staticmethod
    def from_test(test: TestCase, test_app: Flask) -> "CaseTriageTestHelpers":
        return CaseTriageTestHelpers(test, test_app, test_app.test_client())
