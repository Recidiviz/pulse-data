# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements test for interface used to support external requests in Workflows"""
from http import HTTPStatus
from typing import Generator
from unittest import TestCase

import pytest
import responses
from flask import Flask
from mock import MagicMock, patch

from recidiviz.case_triage.workflows.interface import WorkflowsExternalRequestInterface
from recidiviz.tools.workflows.fixtures.tomis_contact_notes import complete_request_obj
from recidiviz.utils.types import assert_type


@pytest.fixture(autouse=True)
def app_context() -> Generator[None, None, None]:
    test_app = Flask("test_workflows_authorization")
    with test_app.app_context():
        yield


class TestWorkflowsInterface(TestCase):
    """Test class for making external requests in Workflows"""

    def setUp(self) -> None:
        self.fake_url = "http://fake-url.com"

    @patch("requests.put")
    def test_insert_contact_note_missing_params(self, mock_put: MagicMock) -> None:
        data = {"blah": ""}
        response = WorkflowsExternalRequestInterface.insert_contact_note(data)
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        mock_put.assert_not_called()

    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    def test_insert_contact_note_is_test_success(
        self, mock_get_secret: MagicMock
    ) -> None:
        data = {"isTest": True, "env": "staging", "fixture": complete_request_obj}
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, json={"status": "OK"})

            response = WorkflowsExternalRequestInterface.insert_contact_note(data)
            response_obj = assert_type(response.json, dict)
            self.assertEqual(response.status_code, HTTPStatus.OK)
            self.assertEqual(response_obj.get("message"), "Called TOMIS")

    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    def test_insert_contact_note_is_test_exception_raised(
        self, mock_get_secret: MagicMock
    ) -> None:
        data = {"isTest": True, "env": "staging", "fixture": complete_request_obj}
        mock_get_secret.return_value = self.fake_url
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(responses.PUT, self.fake_url, body=ConnectionRefusedError())
            response = WorkflowsExternalRequestInterface.insert_contact_note(data)
            self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @patch("requests.put")
    @patch("recidiviz.case_triage.workflows.interface.get_secret")
    def test_insert_contact_note_is_test_no_secret(
        self, mock_get_secret: MagicMock, mock_put: MagicMock
    ) -> None:
        data = {"isTest": True, "env": "staging"}
        mock_get_secret.return_value = None
        response = WorkflowsExternalRequestInterface.insert_contact_note(data)

        mock_put.assert_not_called()
        response_obj = assert_type(response.json, dict)
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(response_obj.get("message"), "Unable to get secrets for TOMIS")
