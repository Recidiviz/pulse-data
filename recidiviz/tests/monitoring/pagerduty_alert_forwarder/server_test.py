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
"""Tests for alert forwarder server."""
import base64
import json
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_alert import (
    PagerDutyAlert,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.server import AlertForwarder, app


class TestAlertForwarder(unittest.TestCase):
    """Tests for AlertForwarder class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a minimal config file
        self.config_content = """
rules:
  - name: "Test Rule"
    match:
      incident.resource.labels.project_id: "test-project"
    actions:
      severity: error
      pagerduty_service: "test-service"
      title_prefix: "[TEST]"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as config_file:
            config_file.write(self.config_content)
            self.config_file = config_file

        # Mock environment variables
        self.env_patcher = patch.dict(
            "os.environ",
            {
                "CONFIG_PATH": self.config_file.name,
                "GCP_PROJECT": "test-project",
                "PAGERDUTY_INTEGRATION_KEYS": json.dumps(
                    {"test-service": "test-key-123"}
                ),
            },
        )
        self.env_patcher.start()

        # Create alert forwarder instance
        self.forwarder = AlertForwarder()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        self.env_patcher.stop()

    def _create_alert(
        self,
        policy_name: str = "Test Policy",
        incident_id: str = "incident-123",
        state: str = "open",
        project_id: str = "test-project",
    ) -> PagerDutyAlert:
        """Helper to create alert."""
        return PagerDutyAlert(
            {
                "incident": {
                    "incident_id": incident_id,
                    "policy_name": policy_name,
                    "state": state,
                    "started_at": 1234567890,
                    "ended_at": None if state == "open" else 1234567999,
                    "url": "https://console.cloud.google.com/monitoring/alerting",
                    "resource": {
                        "type": "gce_instance",
                        "labels": {
                            "project_id": project_id,
                            "instance_id": "test-instance",
                        },
                    },
                    "metric": {
                        "type": "compute.googleapis.com/instance/cpu/utilization",
                    },
                }
            }
        )

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_process_alert_open_state(self, mock_trigger: MagicMock) -> None:
        """Test processing an alert with open state triggers incident."""
        mock_trigger.return_value = {
            "status": "success",
            "dedup_key": "Test Policy:incident-123",
        }

        alert = self._create_alert(state="open")
        response = self.forwarder.process_alert(alert)

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Alert forwarded successfully", response.data)

        # Verify trigger_incident was called
        mock_trigger.assert_called_once()
        call_kwargs = mock_trigger.call_args.kwargs
        self.assertEqual(call_kwargs["service_name"], "test-service")
        self.assertEqual(call_kwargs["severity"], "error")
        self.assertIn("[TEST]", call_kwargs["title"])

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.resolve_incident"
    )
    def test_process_alert_closed_state(self, mock_resolve: MagicMock) -> None:
        """Test processing an alert with closed state resolves incident."""
        mock_resolve.return_value = {
            "status": "success",
            "dedup_key": "Test Policy:incident-123",
        }

        alert = self._create_alert(state="closed")
        response = self.forwarder.process_alert(alert)

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Alert resolved successfully", response.data)

        # Verify resolve_incident was called
        mock_resolve.assert_called_once()
        call_kwargs = mock_resolve.call_args.kwargs
        self.assertEqual(call_kwargs["service_name"], "test-service")
        # Verify the alert parameter is a PagerDutyAlert with the correct data
        self.assertIsInstance(call_kwargs["alert"], PagerDutyAlert)
        self.assertEqual(call_kwargs["alert"].raw_data, alert.raw_data)

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.resolve_incident"
    )
    def test_process_alert_lifecycle(
        self, mock_resolve: MagicMock, mock_trigger: MagicMock
    ) -> None:
        """Test full lifecycle: open then close."""
        mock_trigger.return_value = {"status": "success", "dedup_key": "test-key"}
        mock_resolve.return_value = {"status": "success", "dedup_key": "test-key"}

        # First, process open alert
        open_alert = self._create_alert(state="open", incident_id="test-123")
        response = self.forwarder.process_alert(open_alert)
        self.assertEqual(response.status_code, 200)
        mock_trigger.assert_called_once()

        # Then, process closed alert with same incident ID
        closed_alert = self._create_alert(state="closed", incident_id="test-123")
        response = self.forwarder.process_alert(closed_alert)
        self.assertEqual(response.status_code, 200)
        mock_resolve.assert_called_once()

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_process_alert_no_matching_rule(self, mock_trigger: MagicMock) -> None:
        """Test processing an alert that doesn't match any rules."""
        alert = self._create_alert(project_id="different-project")
        response = self.forwarder.process_alert(alert)

        # Should fail because no rule matches, so no PagerDuty service is set
        self.assertEqual(response.status_code, 500)
        self.assertIn(b"No PagerDuty service configured", response.data)
        mock_trigger.assert_not_called()

    def test_load_pagerduty_keys_from_env(self) -> None:
        """Test loading PagerDuty keys from environment variable."""
        keys = self.forwarder.load_pagerduty_keys()
        self.assertEqual(keys, {"test-service": "test-key-123"})

    @patch.dict("os.environ", {"PAGERDUTY_INTEGRATION_KEYS": ""})
    def test_load_pagerduty_keys_empty(self) -> None:
        """Test loading PagerDuty keys with empty env var."""
        forwarder = AlertForwarder()
        keys = forwarder.load_pagerduty_keys()
        self.assertEqual(keys, {})

    @patch.dict("os.environ", {"PAGERDUTY_INTEGRATION_KEYS": "invalid-json"})
    def test_load_pagerduty_keys_invalid_json(self) -> None:
        """Test loading PagerDuty keys with invalid JSON."""
        forwarder = AlertForwarder()
        keys = forwarder.load_pagerduty_keys()
        self.assertEqual(keys, {})


class TestServerEndpoints(unittest.TestCase):
    """Tests for Flask server endpoints."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a minimal config file
        config_content = """
rules:
  - name: "Test Rule"
    match:
      incident.resource.labels.project_id: "test-project"
    actions:
      severity: error
      pagerduty_service: "test-service"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as config_file:
            config_file.write(config_content)
            self.config_file = config_file

        # Mock environment and create forwarder
        self.env_patcher = patch.dict(
            "os.environ",
            {
                "CONFIG_PATH": self.config_file.name,
                "GCP_PROJECT": "test-project",
                "PAGERDUTY_INTEGRATION_KEYS": json.dumps(
                    {"test-service": "test-key-123"}
                ),
            },
        )
        self.env_patcher.start()

        # Import and initialize forwarder module-level variable
        # This must be done after config / env has been set up
        # pylint: disable=import-outside-toplevel
        from recidiviz.monitoring.pagerduty_alert_forwarder import server

        server.forwarder = AlertForwarder()

        # Create test client
        app.config["TESTING"] = True
        self.client = app.test_client()

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        self.env_patcher.stop()

    def test_health_endpoint(self) -> None:
        """Test health check endpoint."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"OK")

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_pubsub_push_endpoint_open(self, mock_trigger: MagicMock) -> None:
        """Test Pub/Sub push endpoint with open alert."""
        mock_trigger.return_value = {"status": "success", "dedup_key": "test-key"}

        alert_data = {
            "incident": {
                "incident_id": "test-123",
                "policy_name": "Test Policy",
                "state": "open",
                "resource": {
                    "type": "gce_instance",
                    "labels": {"project_id": "test-project"},
                },
            }
        }

        # Create Pub/Sub message format
        message_data = base64.b64encode(json.dumps(alert_data).encode()).decode()
        pubsub_message = {"message": {"data": message_data}}

        response = self.client.post(
            "/pubsub-push",
            data=json.dumps(pubsub_message),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Alert forwarded successfully", response.data)
        mock_trigger.assert_called_once()

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.resolve_incident"
    )
    def test_pubsub_push_endpoint_closed(self, mock_resolve: MagicMock) -> None:
        """Test Pub/Sub push endpoint with closed alert."""
        mock_resolve.return_value = {"status": "success", "dedup_key": "test-key"}

        alert_data = {
            "incident": {
                "incident_id": "test-123",
                "policy_name": "Test Policy",
                "state": "closed",
                "resource": {
                    "type": "gce_instance",
                    "labels": {"project_id": "test-project"},
                },
            }
        }

        # Create Pub/Sub message format
        message_data = base64.b64encode(json.dumps(alert_data).encode()).decode()
        pubsub_message = {"message": {"data": message_data}}

        response = self.client.post(
            "/pubsub-push",
            data=json.dumps(pubsub_message),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Alert resolved successfully", response.data)
        mock_resolve.assert_called_once()

    def test_pubsub_push_endpoint_invalid_format(self) -> None:
        """Test Pub/Sub push endpoint with invalid message format."""
        response = self.client.post(
            "/pubsub-push",
            data="not json",
            content_type="text/plain",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Invalid request format", response.data)

    def test_pubsub_push_endpoint_missing_message(self) -> None:
        """Test Pub/Sub push endpoint with missing message field."""
        response = self.client.post(
            "/pubsub-push",
            data=json.dumps({"no_message": "here"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Invalid Pub/Sub message", response.data)

    def test_pubsub_push_endpoint_invalid_base64(self) -> None:
        """Test Pub/Sub push endpoint with invalid base64 data."""
        pubsub_message = {"message": {"data": "not-valid-base64!!!"}}

        response = self.client.post(
            "/pubsub-push",
            data=json.dumps(pubsub_message),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Invalid message data", response.data)
