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

from itertools import chain
from typing import Any, TypedDict
from unittest.mock import Mock, patch

import pytest
import requests

from recidiviz.outliers.querier.querier import OutliersQuerier
from recidiviz.outliers.roster_ticket_service import (
    IntercomAPIClient,
    RosterChangeType,
    RosterTicketService,
)
from recidiviz.outliers.types import IntercomTicketResponse
from recidiviz.persistence.database.schema.insights.schema import (
    SupervisionOfficer,
    SupervisionOfficerSupervisor,
)


class MockEntities(TypedDict):
    officers: list[SupervisionOfficer]
    supervisors: list[SupervisionOfficerSupervisor]


@pytest.fixture(name="mock_querier")
def mock_querier_fixture() -> Mock:
    """Mocks the OutliersQuerier for use in RosterTicketService."""
    return Mock(spec=OutliersQuerier)


@pytest.fixture(name="mock_entities")
def mock_entities_fixture() -> MockEntities:
    """Loads officers and supervisors."""

    # Load officers
    officers = [
        SupervisionOfficer(
            external_id="off1",
            full_name="John Officer",
            supervision_district="District A",
            supervisor_external_ids=["sup1", "sup2"],
        ),
        SupervisionOfficer(
            external_id="off2",
            full_name="Jane Officer",
            supervision_district="District B",
            supervisor_external_ids=["sup2"],
        ),
    ]

    # Load supervisors
    supervisors = [
        SupervisionOfficerSupervisor(external_id="sup1", full_name="Bob Supervisor"),
        SupervisionOfficerSupervisor(external_id="sup2", full_name="Alice Supervisor"),
    ]
    return {"officers": officers, "supervisors": supervisors}


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
        with patch.object(client, "_session", autospec=True):
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
    mock_response_data = {
        "id": 1,
        "ticket_type_id": 1,
        "contacts": [{"email": email}],
        "ticket_attributes": {
            "_default_title_": title,
            "_default_description_": description,
        },
    }

    intercom_api_client._session.post.return_value.json.return_value = mock_response_data  # type: ignore # pylint: disable=protected-access

    result_ticket = intercom_api_client.create_ticket(title, description, email)

    # Assert
    assert isinstance(result_ticket, IntercomTicketResponse)
    assert result_ticket.id == 1
    assert result_ticket.email == email

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
    intercom_api_client._session.post.return_value = mock_response  # type: ignore # pylint: disable=protected-access

    # Test that it raises the HTTP error
    with pytest.raises(requests.exceptions.HTTPError, match=error_message):
        intercom_api_client.create_ticket(title, description, email)

    # Verify the POST call tried three times
    assert intercom_api_client._session.post.call_count == 3  # type: ignore # pylint: disable=protected-access


@pytest.mark.parametrize(
    "change_type, note, num_of_officers",
    [
        (RosterChangeType.ADD, "Add this officer, please.", 1),
        (
            RosterChangeType.ADD,
            "Add these officers to the caseload."
            + "It's imperative that they're added.\nPlease speak with admin about this.",
            5,
        ),
        (
            RosterChangeType.REMOVE,
            "Remove this officer from my caseload.\nThank you!",
            1,
        ),
        (RosterChangeType.REMOVE, "Remove these officers from my caseloads.", 5),
    ],
    ids=[
        "add_single_officer",
        "add_multiple_officer",
        "remove_single_officer",
        "remove_multiple_officers",
    ],
)
def test_build_ticket_description(
    ticket_service: RosterTicketService,
    mock_querier: Mock,
    mock_entities: MockEntities,
    change_type: RosterChangeType,
    note: str,
    num_of_officers: int,
    snapshot: Any,
) -> None:
    """
    Tests _build_ticket_description with parameterized RosterChangeType and note text.
    Uses snapshots to validate the generated description string.
    """

    mock_querier.get_product_configuration.return_value = Mock(
        supervision_officer_label="officer"
    )

    test_officers = mock_entities["officers"][:num_of_officers]
    supervisors_of_test_officers_ids = list(
        chain(*[o.supervisor_external_ids for o in test_officers])
    )
    test_supervisors = list(
        filter(
            lambda s: s.external_id in supervisors_of_test_officers_ids,
            mock_entities["supervisors"],
        )
    )

    description = ticket_service._build_ticket_description(  # type: ignore # pylint: disable=protected-access
        requester_name="Requesting Person",
        target_name=test_supervisors[0],
        change_type=change_type,
        note=note,
        supervisors=test_supervisors,
        officers=test_officers,
    )

    # Snapshot the generated description
    snapshot.assert_match(description)


@pytest.mark.parametrize(
    "roster_change_type, num_of_officers",
    [
        (RosterChangeType.ADD, 1),
        (RosterChangeType.ADD, 5),
        (RosterChangeType.REMOVE, 1),
        (RosterChangeType.REMOVE, 5),
    ],
    ids=[
        "single_add_officers",
        "muliple_add_officers",
        "multiple_remove",
        "multiple_remove_officers",
    ],
)
def test_request_roster_change(
    ticket_service: RosterTicketService,
    mock_querier: Mock,
    mock_entities: MockEntities,
    roster_change_type: RosterChangeType,
    num_of_officers: int,
) -> None:
    """
    Tests the full workflow of request_roster_change with parameterized scenarios.
    Uses snapshot to confirm the final IntercomTicket dict.
    """

    mock_querier.get_supervision_officers_by_external_ids.return_value = mock_entities[
        "officers"
    ]
    mock_querier.get_supervision_officer_supervisors_by_external_ids.return_value = (
        mock_entities["supervisors"]
    )
    mock_querier.get_product_configuration.return_value = Mock(
        supervision_officer_label="Officer"
    )
    test_supervisor_id = mock_entities["supervisors"][0].external_id

    api_response = {
        "id": 1,
        "ticket_type_id": 1,
        "contacts": [{"email": "requester@example.com"}],
        "ticket_attributes": {
            "_default_title_": "Request",
            "_default_description_": "Please process this request",
        },
    }

    # Mock the Intercom response
    ticket_service.intercom_api_client._session.post.return_value.json.return_value = api_response  # type: ignore # pylint: disable=protected-access

    result = ticket_service.request_roster_change(
        requester="Test Requester",
        email="requester@example.com",
        change_type=roster_change_type,
        target_supervisor_id=test_supervisor_id,
        # Parse down to the number of officers for test
        officer_ids=[o.external_id for o in mock_entities["officers"]][
            :num_of_officers
        ],
        note="Please process this request",
    )

    # validate
    assert isinstance(result, IntercomTicketResponse)
    assert result == IntercomTicketResponse.from_intercom_dict(api_response)


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

    mock_querier.get_supervision_officers_by_external_ids.return_value = (
        modified_entities["officers"]
    )
    mock_querier.get_supervision_officer_supervisors_by_external_ids.return_value = (
        modified_entities["supervisors"]
    )
    mock_querier.get_product_configuration.return_value = Mock(
        supervision_officer_label="officer", supervision_supervisor_label="supervisor"
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
        )
