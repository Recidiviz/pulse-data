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


MAX_ERROR_BODY_CHARS = 1000

# HTTP statuses that mean PagerDuty rejected the event itself, so resending the same
# message cannot succeed. Auth failures (401, 403), 408, 429 and 5xx are deliberately
# excluded so that they are retried and, if they persist, reach the DLQ alert.
PERMANENT_REJECTION_STATUS_CODES = frozenset({400, 404, 409, 410, 413, 422})


class PagerDutyError(Exception):
    """Raised when PagerDuty API call fails.

    status_code and response_body are only set when PagerDuty returned an HTTP error
    response, as opposed to a network failure or a client-side validation error.
    """

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

    @property
    def is_permanent(self) -> bool:
        """True for payload-shape rejections that retrying cannot fix."""
        return self.status_code in PERMANENT_REJECTION_STATUS_CODES

    @classmethod
    def from_request_exception(
        cls, e: requests.exceptions.RequestException
    ) -> "PagerDutyError":
        """Builds a PagerDutyError, capturing the HTTP response if there was one."""
        message = f"PagerDuty API request failed: {e}"
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            return cls(
                message,
                status_code=e.response.status_code,
                response_body=str(e.response.text)[:MAX_ERROR_BODY_CHARS],
            )
        return cls(message)


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
        component: str | None = None,
        custom_details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Trigger a PagerDuty incident.

        Args:
            service_name: Name of the PagerDuty service (maps to integration key)
            title: Incident title/summary
            severity: Incident severity (info/warning/error/critical)
            source: Source of the alert (default: cloud-monitoring)
            alert: PagerDuty alert wrapper to include in custom details
            dedup_key: Deduplication key (auto-generated from alert if not provided)
            component: Optional component of the source that is responsible for the event
            custom_details: Optional custom details. Defaults to the raw alert data
                when an alert is provided.

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

        if component:
            payload["payload"]["component"] = component

        # Add custom details if provided, otherwise fall back to the alert data
        if custom_details:
            payload["payload"]["custom_details"] = custom_details
        elif alert:
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
            raise PagerDutyError.from_request_exception(e) from e

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
            raise PagerDutyError.from_request_exception(e) from e
