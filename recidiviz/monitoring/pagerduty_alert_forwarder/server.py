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
"""Flask server for receiving Cloud Monitoring alerts via Pub/Sub push."""
import base64
import json
import logging
import os
import sys
from collections.abc import Mapping

from flask import Flask, Request, Response, request

from recidiviz.monitoring.pagerduty_alert_forwarder.budget_alert import (
    BUDGET_SOURCE,
    build_budget_event,
    is_budget_notification,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.config_loader import (
    AlertForwarderConfig,
    ConfigurationError,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_alert import (
    PagerDutyAlert,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_client import (
    PagerDutyClient,
    PagerDutyError,
)
from recidiviz.monitoring.pagerduty_alert_forwarder.rule_engine import RuleEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)


class AlertForwarder:
    """Alert forwarder service."""

    def __init__(self) -> None:
        """Initialize alert forwarder with configuration and clients."""
        # Load configuration
        config_path = os.getenv("CONFIG_PATH", "/app/config/config.yaml")
        logger.info("Loading configuration from: %s", config_path)
        try:
            self.config = AlertForwarderConfig.from_file(config_path)
        except ConfigurationError as e:
            logger.error("Failed to load configuration: %s", e)
            raise

        # Initialize rule engine
        self.rule_engine = RuleEngine(self.config)

        # Load PagerDuty integration keys from environment variable
        integration_keys = self.load_pagerduty_keys()
        self.pagerduty_client = PagerDutyClient(integration_keys)

        logger.info(
            "Alert forwarder initialized with %d rules and %d PagerDuty services",
            len(self.config.rules),
            len(integration_keys),
        )

    def load_pagerduty_keys(self) -> dict[str, str]:
        """Load PagerDuty integration keys from environment variable.

        Returns:
            Dictionary mapping service names to integration keys
        """
        # Load integration keys from environment variable (JSON blob)
        keys_json = os.getenv("PAGERDUTY_INTEGRATION_KEYS")
        if not keys_json:
            logger.warning("PAGERDUTY_INTEGRATION_KEYS environment variable not set")
            return {}

        try:
            integration_keys = json.loads(keys_json)
            logger.info(
                "Loaded PagerDuty integration keys for %d services",
                len(integration_keys),
            )
            return integration_keys
        except json.JSONDecodeError as e:
            logger.error("Failed to parse PAGERDUTY_INTEGRATION_KEYS: %s", e)
            return {}

    def handle_pubsub_push(self, req: Request) -> Response:
        """Handle Pub/Sub push message containing Cloud Monitoring alert.

        Args:
            req: Flask request object

        Returns:
            Flask response (200 OK or error)
        """
        # Validate request
        if not req.is_json:
            logger.warning("Received non-JSON request")
            return Response("Invalid request format", status=400)

        envelope = req.get_json()
        if not envelope:
            logger.warning("Empty request body")
            return Response("No Pub/Sub message received", status=400)

        # Extract and decode Pub/Sub message
        pubsub_message = envelope.get("message")
        if not pubsub_message:
            logger.warning("No message field in request")
            return Response("Invalid Pub/Sub message", status=400)

        # Decode base64 data
        try:
            data = base64.b64decode(pubsub_message.get("data", "")).decode("utf-8")
            alert_data = json.loads(data)
        except (ValueError, json.JSONDecodeError) as e:
            logger.error("Failed to decode message data: %s", e)
            return Response(f"Invalid message data: {e}", status=400)

        logger.debug("Processing pubsub message: %s", alert_data)

        # Process alert
        try:
            # Wrap alert data in PagerDutyAlert for cached field lookups
            alert = PagerDutyAlert(alert_data)
            attributes = pubsub_message.get("attributes") or {}
            return self.process_alert(alert, attributes)
        except Exception as e:
            logger.exception("Unexpected error processing alert: %s", e)
            # Return 500 to trigger Pub/Sub retry
            return Response(f"Internal error: {e}", status=500)

    def process_alert(
        self, alert: PagerDutyAlert, attributes: Mapping[str, str] | None = None
    ) -> Response:
        """Process alert through rule engine and forward to PagerDuty.

        Args:
            alert: PagerDuty alert data wrapper
            attributes: Pub/Sub message attributes, if any

        Returns:
            Flask response
        """
        if is_budget_notification(alert.raw_data):
            return self._process_budget_notification(alert, attributes or {})

        incident = alert.raw_data.get("incident", {})
        policy_name = incident.get("policy_name", "unknown")
        incident_id = incident.get("incident_id", "")
        incident_state = incident.get("state", "open")

        logger.info(
            "Processing alert",
            extra={
                "policy_name": policy_name,
                "incident_id": incident_id,
                "state": incident_state,
            },
        )

        # Evaluate rules and transform alert
        try:
            processed = self.rule_engine.process_alert(alert)
        except Exception as e:
            logger.error(
                "Rule engine failed: %s",
                e,
                extra={"policy_name": policy_name, "error": str(e)},
            )
            return Response(f"Rule processing failed: {e}", status=500)

        # Extract processed information
        severity = processed["severity"]
        service = processed["pagerduty_service"]
        title = processed["title"]
        suppress = processed.get("suppress", False)

        # Check if alert should be suppressed
        if suppress:
            logger.info(
                "Alert suppressed by rule configuration",
                extra={"policy_name": policy_name, "incident_id": incident_id},
            )
            return Response("Alert suppressed successfully", status=200)

        # Validate that we have a PagerDuty service configured
        if not service:
            logger.error(
                "No PagerDuty service configured for alert",
                extra={"policy_name": policy_name},
            )
            return Response(
                "No PagerDuty service configured for this alert", status=500
            )

        # Forward to PagerDuty - trigger or resolve based on incident state
        try:
            if incident_state == "closed":
                # Resolve the incident in PagerDuty
                result = self.pagerduty_client.resolve_incident(
                    service_name=service,
                    alert=alert,
                )

                logger.info(
                    "Successfully resolved alert in PagerDuty",
                    extra={
                        "policy_name": policy_name,
                        "service": service,
                        "dedup_key": result.get("dedup_key"),
                    },
                )

                return Response("Alert resolved successfully", status=200)

            # Otherwise trigger/update the incident in PagerDuty
            result = self.pagerduty_client.trigger_incident(
                service_name=service,
                title=title,
                severity=severity,
                source="recidiviz-cloud-monitoring",
                alert=alert,
            )

            logger.info(
                "Successfully forwarded alert to PagerDuty",
                extra={
                    "policy_name": policy_name,
                    "service": service,
                    "severity": severity,
                    "dedup_key": result.get("dedup_key"),
                },
            )

            return Response("Alert forwarded successfully", status=200)

        except PagerDutyError as e:
            return self._pagerduty_error_response(e, service, policy_name)

    def _process_budget_notification(
        self, alert: PagerDutyAlert, attributes: Mapping[str, str]
    ) -> Response:
        """Forward a GCP Billing budget notification to PagerDuty.

        Routine budget cost updates that have not crossed a threshold are acknowledged
        without alerting. The rule engine is only used to pick the PagerDuty service.

        Args:
            alert: PagerDuty alert data wrapper holding the budget message
            attributes: Pub/Sub message attributes (billingAccountId, budgetId, ...)

        Returns:
            Flask response
        """
        budget_name = str(alert.raw_data.get("budgetDisplayName", "unknown"))

        event = build_budget_event(alert.raw_data, attributes)
        if event is None:
            logger.info(
                "Budget notification has no exceeded threshold, not alerting",
                extra={"budget_name": budget_name},
            )
            return Response("Budget update acknowledged, no alert sent", status=200)

        try:
            processed = self.rule_engine.process_alert(alert)
        except Exception as e:
            logger.error(
                "Rule engine failed: %s",
                e,
                extra={"budget_name": budget_name, "error": str(e)},
            )
            return Response(f"Rule processing failed: {e}", status=500)

        if processed.get("suppress", False):
            logger.info(
                "Budget notification suppressed by rule configuration",
                extra={"budget_name": budget_name},
            )
            return Response("Alert suppressed successfully", status=200)

        service = processed["pagerduty_service"]
        if not service:
            logger.error(
                "No PagerDuty service configured for budget notification",
                extra={"budget_name": budget_name},
            )
            return Response(
                "No PagerDuty service configured for this alert", status=500
            )

        try:
            result = self.pagerduty_client.trigger_incident(
                service_name=service,
                title=event.title,
                severity=event.severity,
                source=BUDGET_SOURCE,
                dedup_key=event.dedup_key,
                component=event.component,
                custom_details=event.custom_details,
            )
        except PagerDutyError as e:
            return self._pagerduty_error_response(e, service, budget_name)

        logger.info(
            "Successfully forwarded budget notification to PagerDuty",
            extra={
                "budget_name": budget_name,
                "service": service,
                "severity": event.severity,
                "dedup_key": result.get("dedup_key"),
            },
        )
        return Response("Alert forwarded successfully", status=200)

    @staticmethod
    def _pagerduty_error_response(
        error: PagerDutyError, service: str, alert_name: str
    ) -> Response:
        """Choose the response for a failed PagerDuty call.

        Pub/Sub redelivers on any non-2xx response and eventually dead-letters the
        message. Retrying cannot fix a payload-shape rejection (see
        PagerDutyError.is_permanent), so those are logged and acknowledged. Everything else returns 500 to be retried.

        Args:
            error: The PagerDuty failure
            service: Name of the PagerDuty service the alert was sent to
            alert_name: Policy or budget name, for logging

        Returns:
            Flask response
        """
        extra = {
            "alert_name": alert_name,
            "service": service,
            "status_code": error.status_code,
            "response_body": error.response_body,
        }
        if error.is_permanent:
            logger.error(
                "PagerDuty permanently rejected alert, not retrying: %s (response: %s)",
                error,
                error.response_body,
                extra=extra,
            )
            return Response(f"PagerDuty rejected alert: {error}", status=200)

        logger.error("Failed to forward alert to PagerDuty: %s", error, extra=extra)
        # Return 500 to trigger Pub/Sub retry and DLQ
        return Response(f"PagerDuty error: {error}", status=500)


# Global forwarder instance
forwarder: AlertForwarder


@app.route("/health", methods=["GET"])
def health() -> Response:
    """Health check endpoint."""
    return Response("OK", status=200)


@app.route("/pubsub-push", methods=["POST"])
def pubsub_push() -> Response:
    """Pub/Sub push endpoint for Cloud Monitoring alerts."""
    return forwarder.handle_pubsub_push(request)


def main() -> None:
    """Main entry point for the server."""
    global forwarder

    # Initialize forwarder
    try:
        forwarder = AlertForwarder()
    except Exception as e:
        logger.error("Failed to initialize alert forwarder: %s", e)
        sys.exit(1)

    # Start server
    port = int(os.getenv("PORT", "8080"))
    logger.info("Starting server on port %d", port)
    app.run(host="0.0.0.0", port=port)  # nosec B104


if __name__ == "__main__":
    main()
