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
"""PagerDuty Events V2 API client."""
import logging
from typing import Any

import requests

from recidiviz.monitoring.pagerduty_alert_forwarder.pagerduty_alert import (
    PagerDutyAlert,
)

logger = logging.getLogger(__name__)

# PagerDuty Events V2 API endpoint
PAGERDUTY_EVENTS_V2_URL = "https://events.pagerduty.com/v2/enqueue"


class PagerDutyError(Exception):
    """Raised when PagerDuty API call fails."""


class PagerDutyClient:
    """Client for PagerDuty Events V2 API."""

    def __init__(self, integration_keys: dict[str, str], timeout: int = 10) -> None:
        """Initialize PagerDuty client.

        Args:
            integration_keys: Mapping of service names to integration keys
            timeout: Request timeout in seconds
        """
        self.integration_keys = integration_keys
        self.timeout = timeout

    def trigger_incident(
        self,
        service_name: str,
        title: str,
        severity: str,
        source: str = "cloud-monitoring",
        alert: PagerDutyAlert | None = None,
        dedup_key: str | None = None,
    ) -> dict[str, Any]:
        """Trigger a PagerDuty incident.

        Args:
            service_name: Name of the PagerDuty service (maps to integration key)
            title: Incident title/summary
            severity: Incident severity (info/warning/error/critical)
            source: Source of the alert (default: cloud-monitoring)
            alert: PagerDuty alert wrapper to include in custom details
            dedup_key: Deduplication key (auto-generated from alert if not provided)

        Returns:
            PagerDuty API response

        Raises:
            PagerDutyError: If API call fails or service name not configured
        """
        # Validate service name and get integration key
        if service_name not in self.integration_keys:
            raise PagerDutyError(
                f"Unknown PagerDuty service: {service_name}. "
                f"Available services: {', '.join(self.integration_keys.keys())}"
            )

        integration_key = self.integration_keys[service_name]

        # Generate dedup key from alert if not provided
        if dedup_key is None and alert:
            dedup_key = alert.get_dedup_key()

        # Build event payload
        payload: dict[str, Any] = {
            "routing_key": integration_key,
            "event_action": "trigger",
            "payload": {
                "summary": title,
                "severity": severity,
                "source": source,
            },
        }

        # Add dedup key if available
        if dedup_key:
            payload["dedup_key"] = dedup_key

        # Add custom details if alert data provided
        if alert:
            payload["payload"]["custom_details"] = {
                "cloud_monitoring_alert": alert.raw_data,
            }

        # Add links to Cloud Console if available
        if alert:
            incident_url = alert.get_incident_url()
            if incident_url:
                payload["links"] = [
                    {
                        "href": incident_url,
                        "text": "View in Cloud Console",
                    }
                ]

        # Send request to PagerDuty
        try:
            logger.info(
                "Sending incident to PagerDuty",
                extra={
                    "service": service_name,
                    "severity": severity,
                    "title": title,
                    "dedup_key": dedup_key,
                },
            )

            response = requests.post(
                PAGERDUTY_EVENTS_V2_URL,
                json=payload,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )

            # Check for errors
            response.raise_for_status()

            result = response.json()
            logger.info(
                "Successfully created PagerDuty incident",
                extra={
                    "service": service_name,
                    "dedup_key": result.get("dedup_key"),
                    "status": result.get("status"),
                },
            )

            return result

        except requests.exceptions.RequestException as e:
            logger.error(
                "Failed to send incident to PagerDuty: %s",
                e,
                extra={"service": service_name, "error": str(e)},
            )
            raise PagerDutyError(f"PagerDuty API request failed: {e}") from e

    def resolve_incident(
        self,
        service_name: str,
        alert: PagerDutyAlert | None = None,
        dedup_key: str | None = None,
    ) -> dict[str, Any]:
        """Resolve a PagerDuty incident.

        Args:
            service_name: Name of the PagerDuty service (maps to integration key)
            alert: PagerDuty alert wrapper to extract dedup key from
            dedup_key: Deduplication key (auto-generated from alert if not provided)

        Returns:
            PagerDuty API response

        Raises:
            PagerDutyError: If API call fails or service name not configured
        """
        # Validate service name and get integration key
        if service_name not in self.integration_keys:
            raise PagerDutyError(
                f"Unknown PagerDuty service: {service_name}. "
                f"Available services: {', '.join(self.integration_keys.keys())}"
            )

        integration_key = self.integration_keys[service_name]

        # Generate dedup key from alert if not provided
        if dedup_key is None and alert:
            dedup_key = alert.get_dedup_key()

        # Dedup key is required for resolve
        if not dedup_key:
            raise PagerDutyError("dedup_key is required to resolve an incident")

        # Build event payload
        payload: dict[str, Any] = {
            "routing_key": integration_key,
            "event_action": "resolve",
            "dedup_key": dedup_key,
        }

        # Send request to PagerDuty
        try:
            logger.info(
                "Resolving incident in PagerDuty",
                extra={
                    "service": service_name,
                    "dedup_key": dedup_key,
                },
            )

            response = requests.post(
                PAGERDUTY_EVENTS_V2_URL,
                json=payload,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )

            # Check for errors
            response.raise_for_status()

            result = response.json()
            logger.info(
                "Successfully resolved PagerDuty incident",
                extra={
                    "service": service_name,
                    "dedup_key": result.get("dedup_key"),
                    "status": result.get("status"),
                },
            )

            return result

        except requests.exceptions.RequestException as e:
            logger.error(
                "Failed to resolve incident in PagerDuty: %s",
                e,
                extra={"service": service_name, "error": str(e)},
            )
            raise PagerDutyError(f"PagerDuty API request failed: {e}") from e
