# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for RosterTicketService"""

from typing import Any, TypedDict
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.roster_ticket_service import (
    IntercomAPIClient,
    RosterChangeType,
    RosterTicketService,
)
from recidiviz.outliers.types import (
    IntercomTicketResponse,
    RosterChangeRequestResponseSchema,
)
from recidiviz.persistence.database.schema.insights.schema import (
    SupervisionOfficer,
    SupervisionOfficerSupervisor,
)


class MockEntities(TypedDict):
    officers: list[SupervisionOfficer]
    supervisors: list[SupervisionOfficerSupervisor]


@pytest.fixture(name="mock_entities")
def mock_entities_fixture() -> MockEntities:
    """Loads officers and supervisors."""

    # Load officers
    officers = [
        SupervisionOfficer(
            external_id="off1",
            email="jane@testdomain.com",
            full_name={"given_names": "Jane", "surname": "Officer"},
            supervision_district="District A",
            supervisor_external_ids=["sup1", "sup2"],
        ),
        SupervisionOfficer(
            external_id="off2",
            full_name={"given_names": "John", "surname": "Officer"},
            supervision_district="District B",
            supervisor_external_ids=["sup2"],
        ),
        SupervisionOfficer(
            external_id="off3",
            email="sam@testdomain.com",
            full_name={"given_names": "Sam", "surname": "Officer"},
            supervision_district="District C",
            supervisor_external_ids=[],
        ),
    ]

    # Load supervisors
    supervisors = [
        SupervisionOfficerSupervisor(
            external_id="sup1",
            email="alice@testdomain.com",
            full_name={"given_names": "Alice", "surname": "Supervisor"},
        ),
        SupervisionOfficerSupervisor(
            external_id="sup2",
            full_name={"given_names": "Bob", "surname": "Supervisor"},
        ),
    ]
    return {"officers": officers, "supervisors": supervisors}


@pytest.fixture(name="mock_querier")
def mock_querier_fixture(mock_entities: Mock) -> Mock:
    return Mock(
        spec=OutliersQuerier,
        get_product_configuration=Mock(
            return_value=Mock(
                supervision_officer_label="officer",
                supervision_supervisor_label="supervisor",
            )
        ),
        get_supervision_officers_by_external_ids=Mock(
            side_effect=lambda external_ids: [
                officer
                for officer in mock_entities["officers"]
                if officer.external_id in external_ids
            ]
        ),
        get_supervision_officer_supervisors_by_external_ids=Mock(
            side_effect=lambda external_ids: [
                supervisor
                for supervisor in mock_entities["supervisors"]
                if supervisor.external_id in external_ids
            ]
        ),
    )


@pytest.fixture(name="intercom_api_client")
def intercom_api_client_fixture() -> IntercomAPIClient:  # type: ignore
    """
    Returns an IntercomAPIClient with a patched session and `get_secret`
    so we don't make real network calls in tests.
    """
    # Patch get_secret to avoid real secret lookups
    with patch(
        "recidiviz.outliers.roster_ticket_service.get_secret",
        return_value="mock_intercom_token",
    ):
        # Instantiate the client
        client = IntercomAPIClient()

        # Patch the _session property to avoid real HTTP calls
        with patch.object(client, "_session", autospec=True) as mock_session:

            def fake_post(*args: Any, **kwargs: Any) -> MagicMock:  # type: ignore # pylint: disable=unused-argument
                fake_response = MagicMock()
                payload = kwargs.get("json")
                if isinstance(payload, dict):
                    # Create a copy to avoid mutating the original object.
                    payload_copy = payload.copy()
                    payload_copy["id"] = "1"
                    payload_copy["ticket_id"] = "1"
                    fake_response.json.return_value = payload_copy
                else:
                    fake_response.json.return_value = payload
                return fake_response

            mock_session.post.side_effect = fake_post
            yield client


@pytest.fixture(name="ticket_service")
def ticket_service_fixture(
    mock_querier: Mock, intercom_api_client: IntercomAPIClient
) -> RosterTicketService:
    """
    Returns a RosterTicketService with a mocked OutliersQuerier and
    an IntercomAPIClient whose client is also patched.
    """
    return RosterTicketService(
        querier=mock_querier, intercom_api_client=intercom_api_client
    )


@pytest.mark.parametrize(
    "title,description,email",
    [
        ("Test Title A", "Test Description A", "testA@example.com"),
        ("Another Title", "Another Description", "another@example.com"),
    ],
    ids=["basic_ticket", "another_ticket"],
)
def test_create_ticket(
    intercom_api_client: IntercomAPIClient, title: str, description: str, email: str
) -> None:
    """
    Test IntercomAPIClient.create_ticket with parameterization.
    Ensures the correct payload is sent and response is parsed properly.
    """

    result_ticket = intercom_api_client.create_ticket(title, description, email)

    # Assert
    assert result_ticket == IntercomTicketResponse(id="1")

    # Verify the POST call
    intercom_api_client._session.post.assert_called_once()  # type: ignore # pylint: disable=protected-access
    called_args, _ = intercom_api_client._session.post.call_args  # type: ignore # pylint: disable=protected-access
    assert "/tickets" in called_args[0], "Should POST to /tickets endpoint"


@pytest.mark.parametrize(
    "status_code,error_message",
    [
        (400, "Bad Request"),
        (503, "Service Unavailable"),
        # test at your own discretion, the first should suffice
        # (401, "Unauthorized"),
        # (403, "Forbidden"),
        # (404, "Not Found"),
        # (500, "Internal Server Error"),
    ],
    ids=[
        "bad_request",
        "unauthorized",
        # "forbidden",
        # "not_found",
        # "server_error",
        # "service_unavailable",
    ],
)
def test_create_ticket_fails(
    intercom_api_client: IntercomAPIClient, status_code: int, error_message: str
) -> None:
    """
    Test IntercomAPIClient.create_ticket error handling.
    Ensures that it properly raises HTTP status errors.

    Args:
        intercom_api_client: The client fixture
        status_code: HTTP status code to test
        error_message: Expected error message
    """
    title = "Test Title A"
    description = "Test Description A"
    email = "testA@example.com"

    # Mock the error response
    mock_response = Mock()
    mock_response.status_code = status_code
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        error_message, request=Mock(), response=mock_response
    )
    intercom_api_client._session.post.side_effect = None  # type: ignore # pylint: disable=protected-access
    intercom_api_client._session.post.return_value = mock_response  # type: ignore # pylint: disable=protected-access

    # Test that it raises the HTTP error
    with pytest.raises(requests.exceptions.HTTPError, match=error_message):
        intercom_api_client.create_ticket(title, description, email)

    # Verify the POST call tried three times
    assert intercom_api_client._session.post.call_count == 3  # type: ignore # pylint: disable=protected-access


@pytest.mark.parametrize(
    "change_type, note, num_of_officers, is_test",
    [
        (RosterChangeType.ADD, "Add this officer, please.", 1, False),
        (
            RosterChangeType.ADD,
            "Add these officers to the caseload."
            + "It's imperative that they're added.\nPlease speak with admin about this.",
            3,
            False,
        ),
        (
            RosterChangeType.REMOVE,
            "Remove this officer from my caseload.\nThank you!",
            1,
            False,
        ),
        (RosterChangeType.REMOVE, "Remove these officers from my caseloads.", 2, False),
        (
            RosterChangeType.REMOVE,
            "As a recidiviz user, I'm asking to remove these officers from my caseloads",
            2,
            True,
        ),
        (
            RosterChangeType.ADD,
            "From staging, this request is to add these officers to my caseloads",
            2,
            True,
        ),
    ],
    ids=[
        "add_single_officer",
        "add_multiple_officer",
        "remove_single_officer",
        "remove_multiple_officers",
        "remove_multiple_officers_test",
        "add_multiple_officers_test",
    ],
)
def test_request_roster_change(
    ticket_service: RosterTicketService,
    mock_entities: MockEntities,
    change_type: RosterChangeType,
    note: str,
    num_of_officers: int,
    is_test: bool,
    snapshot: Any,
) -> None:
    """
    Tests the full workflow of request_roster_change with parameterized scenarios.
    """

    test_supervisor_id = mock_entities["supervisors"][0].external_id
    email = "requester@example.com"

    response = ticket_service.request_roster_change(
        requester="Test Requester",
        email=email,
        change_type=change_type,
        target_supervisor_id=test_supervisor_id,
        # Parse down to the number of officers for test
        officer_ids=[o.external_id for o in mock_entities["officers"]][
            :num_of_officers
        ],
        note=note,
        is_test=is_test,
    )

    # Capture and verify the API call
    request = ticket_service.intercom_api_client._session.post.call_args[1]["json"]  # type: ignore # pylint: disable=protected-access
    ticket_attributes = request.get("ticket_attributes", {})
    title = ticket_attributes.get("_default_title_")
    description = ticket_attributes.get("_default_description_")

    assert RosterChangeRequestResponseSchema(id="1", email=email) == response
    # These are expected only if this is test_mode
    assert ("[TEST] " in title) == is_test
    assert ("PLEASE DISREGARD. THIS IS A TEST REQUEST." in description) == is_test

    snapshot.assert_match(request)


@pytest.mark.parametrize(
    "entity_type",
    [
        ("officer"),
        ("supervisor"),
    ],
    ids=["missing_officers", "missing_supervisors"],
)
def test_request_roster_change_fails(
    ticket_service: RosterTicketService,
    mock_querier: Mock,
    mock_entities: MockEntities,
    entity_type: str,
) -> None:
    """
    Ensures ValueError is raised when:
    - The target supervisor is missing from the querier results.
    - The requested officers do not match the fetched officers.

    Uses parameterized test cases for flexibility.
    """
    missing_entities = entity_type + "s"
    modified_entities = {**mock_entities, missing_entities: []}

    mock_querier.get_supervision_officers_by_external_ids.side_effect = None
    mock_querier.get_supervision_officer_supervisors_by_external_ids.side_effect = None

    mock_querier.get_supervision_officers_by_external_ids.return_value = (
        modified_entities["officers"]
    )
    mock_querier.get_supervision_officer_supervisors_by_external_ids.return_value = (
        modified_entities["supervisors"]
    )

    beginning_of_error_message = entity_type.capitalize() + "\\(s\\) not found: "
    # ValueError starts with
    with pytest.raises(ValueError, match=f"^{beginning_of_error_message}"):
        ticket_service.request_roster_change(
            requester="Test Requester",
            email="requester@example.com",
            change_type=RosterChangeType.ADD,
            target_supervisor_id=mock_entities["supervisors"][0].external_id,
            officer_ids=[o.external_id for o in mock_entities["officers"]],
            note="This test should fail",
            is_test=False,
        )
