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
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from recidiviz.monitoring import pagerduty_alert_forwarder
from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_alert import (
    PagerDutyAlert,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_client import (
    PagerDutyError,
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

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_process_alert_suppressed(self, mock_trigger: MagicMock) -> None:
        """Test that suppressed alerts are not forwarded to PagerDuty."""
        # Create a config with a suppression rule
        config_content = """
rules:
  - name: "Suppress Test Rule"
    match:
      incident.resource.labels.project_id: "test-project"
    actions:
      suppress: true
"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as config_file:
            config_file.write(config_content)
            temp_config_file = config_file

        with patch.dict(
            "os.environ",
            {
                "CONFIG_PATH": temp_config_file.name,
                "GCP_PROJECT": "test-project",
                "PAGERDUTY_INTEGRATION_KEYS": json.dumps(
                    {"test-service": "test-key-123"}
                ),
            },
        ):
            forwarder = AlertForwarder()

            alert = self._create_alert(state="open")
            response = forwarder.process_alert(alert)

            self.assertEqual(response.status_code, 200)
            self.assertIn(b"Alert suppressed successfully", response.data)

            # Verify trigger_incident was NOT called
            mock_trigger.assert_not_called()

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_process_alert_permanent_pagerduty_rejection_is_acknowledged(
        self, mock_trigger: MagicMock
    ) -> None:
        """A payload rejection returns 200 so Pub/Sub stops retrying, and logs the body."""
        mock_trigger.side_effect = PagerDutyError(
            "PagerDuty API request failed: 400",
            status_code=400,
            response_body='{"message":"Event object is invalid"}',
        )

        with self.assertLogs(
            "recidiviz.monitoring.pagerduty_alert_forwarder.server", level="ERROR"
        ) as logs:
            response = self.forwarder.process_alert(self._create_alert())

        self.assertEqual(response.status_code, 200)
        self.assertIn("Event object is invalid", "\n".join(logs.output))
        self.assertNotIn("test-key-123", "\n".join(logs.output))

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_process_alert_retryable_pagerduty_errors_return_500(
        self, mock_trigger: MagicMock
    ) -> None:
        """Auth failures, 408, 429, 5xx and network errors return 500 so Pub/Sub retries."""
        for status_code in (401, 403, 408, 429, 500, 503, None):
            with self.subTest(status_code=status_code):
                mock_trigger.side_effect = PagerDutyError(
                    "failed", status_code=status_code
                )

                response = self.forwarder.process_alert(self._create_alert())

                self.assertEqual(response.status_code, 500)


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


class TestBudgetNotificationEndpoint(unittest.TestCase):
    """Tests for GCP Billing budget notifications using the real config.yaml."""

    def setUp(self) -> None:
        config_path = Path(pagerduty_alert_forwarder.__file__).parent / "config.yaml"
        self.env_patcher = patch.dict(
            "os.environ",
            {
                "CONFIG_PATH": str(config_path),
                "PAGERDUTY_INTEGRATION_KEYS": json.dumps(
                    {
                        "Cloud Billing": "billing-key",
                        "GCP Infrastructure": "infra-key",
                    }
                ),
            },
        )
        self.env_patcher.start()
        # pylint: disable=import-outside-toplevel
        from recidiviz.monitoring.pagerduty_alert_forwarder import server

        server.forwarder = AlertForwarder()
        app.config["TESTING"] = True
        self.client = app.test_client()

    def tearDown(self) -> None:
        self.env_patcher.stop()

    def _post(
        self, data: dict[str, Any], attributes: dict[str, str] | None = None
    ) -> Any:
        message: dict[str, Any] = {
            "data": base64.b64encode(json.dumps(data).encode()).decode()
        }
        if attributes is not None:
            message["attributes"] = attributes
        return self.client.post(
            "/pubsub-push",
            data=json.dumps({"message": message}),
            content_type="application/json",
        )

    @staticmethod
    def _budget_message(**overrides: Any) -> dict[str, Any]:
        message: dict[str, Any] = {
            "budgetDisplayName": "[Cloud Costs] BigQuery - $20,000 per month",
            "costAmount": 14684.37,
            "costIntervalStart": "2026-10-01T07:00:00Z",
            "budgetAmount": 20000.0,
            "budgetAmountType": "SPECIFIED_AMOUNT",
            "currencyCode": "USD",
        }
        message.update(overrides)
        return message

    ATTRIBUTES = {
        "billingAccountId": "012345-ABCDEF-678901",
        "budgetId": "budget-1",
        "schemaVersion": "1.0",
    }

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_actual_threshold_pages_cloud_billing(
        self, mock_trigger: MagicMock
    ) -> None:
        mock_trigger.return_value = {"status": "success", "dedup_key": "k"}

        response = self._post(
            self._budget_message(alertThresholdExceeded=0.71), self.ATTRIBUTES
        )

        self.assertEqual(response.status_code, 200)
        mock_trigger.assert_called_once()
        kwargs = mock_trigger.call_args.kwargs
        self.assertEqual(kwargs["service_name"], "Cloud Billing")
        self.assertEqual(
            kwargs["title"],
            "[Cloud Costs] BigQuery - $20,000 per month: 71% of budget reached "
            "(cost $14,684)",
        )
        self.assertEqual(kwargs["severity"], "warning")
        self.assertEqual(kwargs["component"], "cloud-billing")
        self.assertEqual(
            kwargs["dedup_key"],
            "012345-ABCDEF-678901:budget-1:actual:0.71:2026-10-01T07:00:00Z",
        )
        self.assertEqual(kwargs["custom_details"]["budgetId"], "budget-1")

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_actual_threshold_critical(self, mock_trigger: MagicMock) -> None:
        mock_trigger.return_value = {"status": "success", "dedup_key": "k"}

        self._post(self._budget_message(alertThresholdExceeded=1.0), self.ATTRIBUTES)

        self.assertEqual(mock_trigger.call_args.kwargs["severity"], "critical")

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_actual_threshold_info(self, mock_trigger: MagicMock) -> None:
        mock_trigger.return_value = {"status": "success", "dedup_key": "k"}

        self._post(self._budget_message(alertThresholdExceeded=0.5), self.ATTRIBUTES)

        self.assertEqual(mock_trigger.call_args.kwargs["severity"], "info")

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_forecast_threshold(self, mock_trigger: MagicMock) -> None:
        mock_trigger.return_value = {"status": "success", "dedup_key": "k"}

        self._post(self._budget_message(forecastThresholdExceeded=1.0), self.ATTRIBUTES)

        kwargs = mock_trigger.call_args.kwargs
        self.assertEqual(kwargs["severity"], "warning")
        self.assertIn("forecast to reach 100% of budget", kwargs["title"])
        self.assertEqual(kwargs["service_name"], "Cloud Billing")

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.resolve_incident"
    )
    def test_routine_cost_update_is_suppressed(
        self, mock_resolve: MagicMock, mock_trigger: MagicMock
    ) -> None:
        response = self._post(self._budget_message(), self.ATTRIBUTES)

        self.assertEqual(response.status_code, 200)
        mock_trigger.assert_not_called()
        mock_resolve.assert_not_called()

    @patch("requests.post")
    def test_pagerduty_400_for_budget_is_acknowledged(
        self, mock_post: MagicMock
    ) -> None:
        # pylint: disable=import-outside-toplevel
        import requests

        mock_response = MagicMock(status_code=400, text="invalid event")
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_post.return_value = mock_response

        response = self._post(
            self._budget_message(alertThresholdExceeded=0.71), self.ATTRIBUTES
        )

        self.assertEqual(response.status_code, 200)
        mock_post.assert_called_once()

    @patch("requests.post")
    def test_pagerduty_503_for_budget_is_retried(self, mock_post: MagicMock) -> None:
        # pylint: disable=import-outside-toplevel
        import requests

        mock_response = MagicMock(status_code=503, text="unavailable")
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_post.return_value = mock_response

        response = self._post(
            self._budget_message(alertThresholdExceeded=0.71), self.ATTRIBUTES
        )

        self.assertEqual(response.status_code, 500)

    @patch(
        "recidiviz.monitoring.pagerduty_alert_forwarder.server.PagerDutyClient.trigger_incident"
    )
    def test_non_budget_non_incident_message_gets_fallback_title(
        self, mock_trigger: MagicMock
    ) -> None:
        mock_trigger.return_value = {"status": "success", "dedup_key": "k"}

        response = self._post({"something": "else"})

        self.assertEqual(response.status_code, 200)
        kwargs = mock_trigger.call_args.kwargs
        self.assertEqual(kwargs["title"], "GCP alert (no title)")
        self.assertEqual(kwargs["service_name"], "GCP Infrastructure")
