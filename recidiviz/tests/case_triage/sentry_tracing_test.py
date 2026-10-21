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
"""Implements tests for sentry_tracing."""
from typing import Any
from unittest import TestCase

import sentry_sdk
from flask import Flask
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.transport import Transport

from recidiviz.case_triage.sentry_tracing import (
    GCP_LOGS_CONTEXT,
    GCP_TRACE_ID_TAG,
    logs_explorer_url,
    tag_sentry_scope_with_gcp_trace,
)
from recidiviz.utils.types import assert_type

_PROJECT_ID = "recidiviz-staging"
_TRACE_ID = "0af7651916cd43dd8448eb211c80319c"


class _CapturingTransport(Transport):
    """Keeps every event the Sentry client would send, instead of sending it."""

    def __init__(self) -> None:
        super().__init__()
        self.events: list[dict[str, Any]] = []

    def capture_envelope(self, envelope: Envelope) -> None:
        for item in envelope.items:
            if item.type in ("event", "transaction"):
                self.events.append(assert_type(item.payload.json, dict))


class TagSentryScopeWithGcpTraceTest(TestCase):
    """Tests for tag_sentry_scope_with_gcp_trace."""

    def _send_failing_request(self, headers: dict[str, str]) -> list[dict[str, Any]]:
        """Returns the events Sentry captures for a request with the given headers
        that fails inside the route."""
        transport = _CapturingTransport()
        app = Flask(__name__)

        @app.before_request
        def tag() -> None:
            tag_sentry_scope_with_gcp_trace(_PROJECT_ID)

        @app.route("/fail")
        def fail() -> str:
            raise ValueError("Unexpected failure")

        sentry_sdk.init(
            dsn="https://public@sentry.example.com/1",
            integrations=[FlaskIntegration()],
            traces_sample_rate=1.0,
            transport=transport,
        )
        try:
            app.test_client().get("/fail", headers=headers)
            sentry_sdk.flush()
        finally:
            sentry_sdk.get_client().close()
            sentry_sdk.get_global_scope().set_client(None)
        return transport.events

    def _assert_tagged(self, events: list[dict[str, Any]]) -> None:
        self.assertEqual(
            {"event", "transaction"},
            {event.get("type", "event") for event in events},
        )
        for event in events:
            self.assertEqual(_TRACE_ID, event["tags"][GCP_TRACE_ID_TAG])
            self.assertEqual(
                logs_explorer_url(_PROJECT_ID, _TRACE_ID),
                event["contexts"][GCP_LOGS_CONTEXT]["url"],
            )

    def test_tags_error_and_transaction_from_traceparent(self) -> None:
        events = self._send_failing_request(
            {"traceparent": f"00-{_TRACE_ID}-b7ad6b7169203331-01"}
        )

        self._assert_tagged(events)

    def test_tags_from_x_cloud_trace_context(self) -> None:
        events = self._send_failing_request(
            {"X-Cloud-Trace-Context": f"{_TRACE_ID}/123456;o=1"}
        )

        self._assert_tagged(events)

    def test_does_not_tag_without_a_trace_header(self) -> None:
        events = self._send_failing_request({})

        self.assertTrue(events)
        for event in events:
            self.assertNotIn(GCP_TRACE_ID_TAG, event.get("tags", {}))
            self.assertNotIn(GCP_LOGS_CONTEXT, event.get("contexts", {}))


class LogsExplorerUrlTest(TestCase):
    """Tests for logs_explorer_url."""

    def test_url_filters_on_the_trace(self) -> None:
        self.assertEqual(
            "https://console.cloud.google.com/logs/query;"
            "query=trace%3D%22projects%2Frecidiviz-staging%2Ftraces%2F"
            "0af7651916cd43dd8448eb211c80319c%22?project=recidiviz-staging",
            logs_explorer_url(_PROJECT_ID, _TRACE_ID),
        )
