# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for IntercomAPIClient"""

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from recidiviz.intercom.client import IntercomAPIClient
from recidiviz.intercom.types import (
    IntercomExportJobResponse,
    IntercomJobStatus,
    IntercomTicketResponse,
)

TIME_NOW = datetime.now(timezone.utc)
TIME_2018 = datetime(2018, 1, 1, tzinfo=timezone.utc)
TIME_HOUR_AGO = TIME_NOW - timedelta(hours=1)


@pytest.fixture(name="intercom_api_client")
def intercom_api_client_fixture() -> IntercomAPIClient:  # type: ignore
    """
    Returns an IntercomAPIClient with a patched session and `get_secret`
    so we don't make real network calls in tests.
    """
    # Patch get_secret to avoid real secret lookups
    with patch(
        "recidiviz.intercom.client.get_secret",
        return_value="mock_intercom_token",
    ):
        # Instantiate the client
        client = IntercomAPIClient()

        # Patch the _session property to avoid real HTTP calls
        with patch.object(client, "_session", autospec=True) as mock_session:

            def fake_post(url: str, **_kwargs: Any) -> MagicMock:
                fake_response = MagicMock()
                if "export/content/data" in url:
                    fake_response.status_code = 200
                    fake_response.json.return_value = {
                        "job_identifier": "orzzsbd7hk67xyu",
                        "status": IntercomJobStatus.COMPLETED,
                        "download_url": "example.com",
                        "download_expires_at": "1674917488",
                    }
                elif "/tickets" in url:
                    fake_response.status_code = 200
                    fake_response.json.return_value = {"id": "1"}
                else:
                    raise NotImplementedError(f"Unexpected URL in fake_post: {url}")

                return fake_response

            def fake_get(url: str, **_kwargs: Any) -> MagicMock:
                fake_response = MagicMock()
                if "export" in url:
                    fake_response.status_code = 200
                    fake_response.json.return_value = {
                        "job_identifier": "orzzsbd7hk67xyu",
                        "status": IntercomJobStatus.COMPLETED,
                        "download_url": "example.com",
                        "download_expires_at": "1674917488",
                    }
                elif "download" in url:
                    fake_response.status_code = 200
                    fake_response.content = "Test content"
                else:
                    raise NotImplementedError(f"Unexpected URL in fake_get: {url}")

                return fake_response

            mock_session.post.side_effect = fake_post
            mock_session.get.side_effect = fake_get
            yield client


@pytest.mark.parametrize(
    "ticket_type_id,title,description,email",
    [
        (1, "Test Title A", "Test Description A", "testA@example.com"),
        (1, "Another Title", "Another Description", "another@example.com"),
    ],
    ids=["basic_ticket", "another_ticket"],
)
def test_create_ticket(
    intercom_api_client: IntercomAPIClient,
    ticket_type_id: int,
    title: str,
    description: str,
    email: str,
) -> None:
    """
    Test IntercomAPIClient.create_ticket with parameterization.
    Ensures the correct payload is sent and response is parsed properly.
    """

    result_ticket = intercom_api_client.create_ticket(
        ticket_type_id, title, description, email
    )

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
    ticket_type_id = 1
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
        intercom_api_client.create_ticket(ticket_type_id, title, description, email)

    # Verify the POST call tried three times
    assert intercom_api_client._session.post.call_count == 3  # type: ignore # pylint: disable=protected-access


@pytest.mark.parametrize(
    "created_at_after,created_at_before",
    [
        (TIME_2018, TIME_NOW),
        (TIME_HOUR_AGO, TIME_NOW),
    ],
    ids=["all_time", "one_hour"],
)
def test_create_data_export(
    intercom_api_client: IntercomAPIClient,
    created_at_after: datetime,
    created_at_before: datetime,
) -> None:
    """
    Test IntercomAPIClient.create_data_export to ensure the data export is created.
    """

    result_export_job_response = intercom_api_client.create_data_export(
        created_at_after, created_at_before
    )

    # Assert
    assert result_export_job_response == IntercomExportJobResponse(
        job_identifier="orzzsbd7hk67xyu",
        status=IntercomJobStatus.COMPLETED,
        download_url="example.com",
        download_expires_at="1674917488",
    )

    # Verify the POST call
    called_args, _ = intercom_api_client._session.post.call_args  # type: ignore # pylint: disable=protected-access
    assert "/export/content/data" in called_args[0]


@pytest.mark.parametrize(
    "status_code,error_message",
    [
        (429, "Exceeded Rate Limit"),
        (500, "Internal Server Error"),
    ],
    ids=["exceeded_rate_limit", "server_error"],
)
def test_create_data_export_fails(
    intercom_api_client: IntercomAPIClient, status_code: int, error_message: str
) -> None:
    """
    Test IntercomAPIClient.create_data_export error handling.
    Ensures that it properly raises HTTP status errors.

    Args:
        intercom_api_client: The client fixture
        status_code: HTTP status code to test
        error_message: Expected error message
    """

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
        intercom_api_client.create_data_export(TIME_2018, TIME_NOW)

    # Verify the POST call tried three times
    assert intercom_api_client._session.post.call_count == 3  # type: ignore # pylint: disable=protected-access


def test_get_export_status(
    intercom_api_client: IntercomAPIClient,
) -> None:
    """
    Test IntercomAPIClient.get_export_status to ensure the correct export status is returned.
    """
    job_identifier = "orzzsbd7hk67xyu"

    result_export_response = intercom_api_client.get_export_status(job_identifier)

    # Assert
    assert result_export_response.status == IntercomJobStatus.COMPLETED


def test_download_export_data(
    intercom_api_client: IntercomAPIClient,
) -> None:
    """
    Test IntercomAPIClient.download_export_data to ensure export data is downloaded.
    """
    job_identifier = "orzzsbd7hk67xyu"

    result_export_data = intercom_api_client.download_export_data(job_identifier)
    print(result_export_data)

    # Assert
    assert result_export_data == "Test content"
