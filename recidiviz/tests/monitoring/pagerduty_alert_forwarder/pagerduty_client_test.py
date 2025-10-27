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
"""Tests for PagerDuty client."""
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import requests

from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_alert import (
    PagerDutyAlert,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_client import (
    PAGERDUTY_EVENTS_V2_URL,
    PagerDutyClient,
    PagerDutyError,
)


class TestPagerDutyClient(unittest.TestCase):
    """Tests for PagerDutyClient."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.integration_keys = {
            "test-service": "test-integration-key-123",
            "production-service": "prod-integration-key-456",
        }
        self.client = PagerDutyClient(self.integration_keys)

    def test_init(self) -> None:
        """Test client initialization."""
        self.assertEqual(self.client.integration_keys, self.integration_keys)
        self.assertEqual(self.client.timeout, 10)

    def test_init_custom_timeout(self) -> None:
        """Test client initialization with custom timeout."""
        client = PagerDutyClient(self.integration_keys, timeout=30)
        self.assertEqual(client.timeout, 30)

    @patch("requests.post")
    def test_trigger_incident_success(self, mock_post: MagicMock) -> None:
        """Test successful incident trigger."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "dedup_key": "test-policy:incident-123",
        }
        mock_post.return_value = mock_response

        alert_data = {
            "incident": {
                "incident_id": "incident-123",
                "policy_name": "test-policy",
                "url": "https://console.cloud.google.com/...",
            }
        }

        result = self.client.trigger_incident(
            service_name="test-service",
            title="Test Alert",
            severity="error",
            alert=PagerDutyAlert(alert_data),
        )

        # Verify the result
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["dedup_key"], "test-policy:incident-123")

        # Verify the API call
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertEqual(call_args.args[0], PAGERDUTY_EVENTS_V2_URL)

        payload = call_args.kwargs["json"]
        self.assertEqual(payload["routing_key"], "test-integration-key-123")
        self.assertEqual(payload["event_action"], "trigger")
        self.assertEqual(payload["dedup_key"], "test-policy:incident-123")
        self.assertEqual(payload["payload"]["summary"], "Test Alert")
        self.assertEqual(payload["payload"]["severity"], "error")
        self.assertEqual(payload["payload"]["source"], "cloud-monitoring")
        self.assertIn("cloud_monitoring_alert", payload["payload"]["custom_details"])
        self.assertEqual(len(payload["links"]), 1)

    @patch("requests.post")
    def test_trigger_incident_custom_dedup_key(self, mock_post: MagicMock) -> None:
        """Test incident trigger with custom dedup key."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "dedup_key": "custom-key",
        }
        mock_post.return_value = mock_response

        result = self.client.trigger_incident(
            service_name="test-service",
            title="Test Alert",
            severity="warning",
            dedup_key="custom-key",
        )

        self.assertEqual(result["dedup_key"], "custom-key")

        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["dedup_key"], "custom-key")

    @patch("requests.post")
    def test_trigger_incident_unknown_service(self, mock_post: MagicMock) -> None:
        """Test incident trigger with unknown service name."""
        with self.assertRaises(PagerDutyError) as context:
            self.client.trigger_incident(
                service_name="unknown-service",
                title="Test Alert",
                severity="error",
            )

        self.assertIn("Unknown PagerDuty service", str(context.exception))
        mock_post.assert_not_called()

    @patch("requests.post")
    def test_trigger_incident_api_error(self, mock_post: MagicMock) -> None:
        """Test incident trigger with API error."""
        mock_post.side_effect = requests.exceptions.RequestException("Network error")

        with self.assertRaises(PagerDutyError) as context:
            self.client.trigger_incident(
                service_name="test-service",
                title="Test Alert",
                severity="critical",
            )

        self.assertIn("PagerDuty API request failed", str(context.exception))

    @patch("requests.post")
    def test_trigger_incident_http_error(self, mock_post: MagicMock) -> None:
        """Test incident trigger with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=400, content=b"Bad Request"),
        )
        mock_post.return_value = mock_response

        with self.assertRaises(PagerDutyError):
            self.client.trigger_incident(
                service_name="test-service",
                title="Test Alert",
                severity="error",
            )

    @patch("requests.post")
    def test_resolve_incident_success(self, mock_post: MagicMock) -> None:
        """Test successful incident resolution."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "dedup_key": "test-policy:incident-123",
        }
        mock_post.return_value = mock_response

        alert_data = {
            "incident": {
                "incident_id": "incident-123",
                "policy_name": "test-policy",
            }
        }

        result = self.client.resolve_incident(
            service_name="test-service",
            alert=PagerDutyAlert(alert_data),
        )

        # Verify the result
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["dedup_key"], "test-policy:incident-123")

        # Verify the API call
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertEqual(call_args.args[0], PAGERDUTY_EVENTS_V2_URL)

        payload = call_args.kwargs["json"]
        self.assertEqual(payload["routing_key"], "test-integration-key-123")
        self.assertEqual(payload["event_action"], "resolve")
        self.assertEqual(payload["dedup_key"], "test-policy:incident-123")
        # Resolve events don't have a payload section
        self.assertNotIn("payload", payload)

    @patch("requests.post")
    def test_resolve_incident_custom_dedup_key(self, mock_post: MagicMock) -> None:
        """Test incident resolution with custom dedup key."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "success",
            "dedup_key": "custom-key",
        }
        mock_post.return_value = mock_response

        result = self.client.resolve_incident(
            service_name="test-service",
            dedup_key="custom-key",
        )

        self.assertEqual(result["dedup_key"], "custom-key")

        payload = mock_post.call_args.kwargs["json"]
        self.assertEqual(payload["dedup_key"], "custom-key")

    @patch("requests.post")
    def test_resolve_incident_no_dedup_key(self, mock_post: MagicMock) -> None:
        """Test incident resolution without dedup key raises error."""
        with self.assertRaises(PagerDutyError) as context:
            self.client.resolve_incident(service_name="test-service")

        self.assertIn("dedup_key is required", str(context.exception))
        mock_post.assert_not_called()

    @patch("requests.post")
    def test_resolve_incident_unknown_service(self, mock_post: MagicMock) -> None:
        """Test incident resolution with unknown service name."""
        with self.assertRaises(PagerDutyError) as context:
            self.client.resolve_incident(
                service_name="unknown-service",
                dedup_key="test-key",
            )

        self.assertIn("Unknown PagerDuty service", str(context.exception))
        mock_post.assert_not_called()

    @patch("requests.post")
    def test_resolve_incident_api_error(self, mock_post: MagicMock) -> None:
        """Test incident resolution with API error."""
        mock_post.side_effect = requests.exceptions.RequestException("Network error")

        with self.assertRaises(PagerDutyError) as context:
            self.client.resolve_incident(
                service_name="test-service",
                dedup_key="test-key",
            )

        self.assertIn("PagerDuty API request failed", str(context.exception))

    def test_dedup_key_generation_with_incident_id(self) -> None:
        """Test dedup key generation with incident ID."""
        alert_data: dict[str, Any] = {
            "incident": {
                "incident_id": "incident-123",
                "policy_name": "test-policy",
            }
        }

        with patch("requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {"status": "success"}
            mock_post.return_value = mock_response

            self.client.trigger_incident(
                service_name="test-service",
                title="Test",
                severity="error",
                alert=PagerDutyAlert(alert_data),
            )

            payload = mock_post.call_args.kwargs["json"]
            self.assertEqual(payload["dedup_key"], "test-policy:incident-123")

    def test_dedup_key_generation_without_incident_id(self) -> None:
        """Test dedup key generation without incident ID."""
        alert_data: dict[str, Any] = {
            "incident": {
                "policy_name": "test-policy",
            }
        }

        with patch("requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {"status": "success"}
            mock_post.return_value = mock_response

            self.client.trigger_incident(
                service_name="test-service",
                title="Test",
                severity="error",
                alert=PagerDutyAlert(alert_data),
            )

            payload = mock_post.call_args.kwargs["json"]
            self.assertEqual(payload["dedup_key"], "test-policy")
