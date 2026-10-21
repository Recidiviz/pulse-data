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
"""Implements tests for sentry_filters."""
import json
from typing import Any, cast
from unittest import TestCase

import sentry_sdk
from flask import Flask
from flask import request as flask_request
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.transport import Transport
from sentry_sdk.types import Event

from recidiviz.case_triage.sentry_filters import (
    NO_REQUEST_DATA_SENTRY_OPTIONS,
    scrub_email_user_pii,
)
from recidiviz.utils.types import assert_type

EMAIL_USER_URL = "https://app.recidiviz.org/workflows/external_request/US_ND/email_user"
OTHER_URL = (
    "https://app.recidiviz.org/workflows/external_request/US_ND/insert_contact_note"
)
PII_CLIENT_MARKER = "Below is a list of your selected clients in the order they appear on the map linked above:"
GOOGLE_MAPS_MARKER = "Here is the Google Maps link"


def _build_event(*, url: str, request_data: Any = None, event_type: str = "") -> Event:
    request: dict = {"url": url}
    if request_data is not None:
        request["data"] = request_data
    raw_event: dict = {"request": request}
    if event_type:
        raw_event["type"] = event_type
    return cast(Event, raw_event)


def _email_body(event: Event) -> str:
    data = assert_type(event["request"]["data"], dict)
    return assert_type(data["emailBody"], str)


class SentryFiltersTest(TestCase):
    """Tests for scrub_email_user_pii."""

    def test_scrubs_all_info_from_email_body(self) -> None:
        email_body = f"Hi there! \n\n {GOOGLE_MAPS_MARKER} \n\n{PII_CLIENT_MARKER}\n- Jane Doe\n- John Smith"
        event = _build_event(url=EMAIL_USER_URL, request_data={"emailBody": email_body})

        result = scrub_email_user_pii(event, {})

        assert result is not None
        self.assertEqual("", _email_body(result))

    def test_ignores_requests_to_other_paths(self) -> None:
        email_body = f"Hi there!\n\n{PII_CLIENT_MARKER}\n- Jane Doe"
        event = _build_event(url=OTHER_URL, request_data={"emailBody": email_body})

        result = scrub_email_user_pii(event, {})

        assert result is not None
        self.assertEqual(email_body, _email_body(result))

    def test_returns_event_unchanged_when_request_has_no_data(self) -> None:
        event = _build_event(url=EMAIL_USER_URL)

        result = scrub_email_user_pii(event, {})

        self.assertEqual(event, result)

    def test_returns_event_unchanged_when_request_data_is_not_a_dict(self) -> None:
        # Sentry represents an oversized or unparsed body as a sentinel AnnotatedValue
        # rather than the parsed dict; scrubbing should not crash on it.
        event = _build_event(url=EMAIL_USER_URL, request_data="<removed>")

        result = scrub_email_user_pii(event, {})

        self.assertEqual(event, result)

    def test_scrubs_transaction_events_the_same_as_error_events(self) -> None:
        email_body = f"Hi there!\n\n{PII_CLIENT_MARKER}\n- Jane Doe"
        event = _build_event(
            url=EMAIL_USER_URL,
            request_data={"emailBody": email_body},
            event_type="transaction",
        )

        result = scrub_email_user_pii(event, {})

        assert result is not None
        self.assertEqual("", _email_body(result))


class _CapturingTransport(Transport):
    """Keeps every event the Sentry client would send, instead of sending it."""

    def __init__(self) -> None:
        super().__init__()
        self.events: list[dict[str, Any]] = []

    def capture_envelope(self, envelope: Envelope) -> None:
        for item in envelope.items:
            if item.type in ("event", "transaction"):
                self.events.append(assert_type(item.payload.json, dict))


class NoRequestDataSentryOptionsTest(TestCase):
    """Tests that NO_REQUEST_DATA_SENTRY_OPTIONS keeps request bodies and frame
    local variables out of both error and transaction events."""

    def _send_failing_course_completion(
        self, sentry_options: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Returns the events Sentry captures when a request with a learner's name
        in its body fails inside the route."""
        transport = _CapturingTransport()
        app = Flask(__name__)

        @app.route("/edovo/course-completions", methods=["POST"])
        def course_completions() -> str:
            body = flask_request.get_json()
            if body["first_name"]:
                raise ValueError("Unexpected course")
            return "OK"

        sentry_sdk.init(
            dsn="https://public@sentry.example.com/1",
            integrations=[FlaskIntegration()],
            traces_sample_rate=1.0,
            transport=transport,
            **sentry_options,
        )
        try:
            app.test_client().post(
                "/edovo/course-completions",
                json={
                    "person_external_id": "A123456",
                    "first_name": "Jane",
                    "last_name": "Doe",
                },
            )
            sentry_sdk.flush()
        finally:
            sentry_sdk.get_client().close()
            sentry_sdk.get_global_scope().set_client(None)
        return transport.events

    def test_events_carry_no_request_body_or_local_variables(self) -> None:
        events = self._send_failing_course_completion(NO_REQUEST_DATA_SENTRY_OPTIONS)

        self.assertEqual(
            {"event", "transaction"},
            {event.get("type", "event") for event in events},
        )
        for event in events:
            # Sentry keeps the data key but empties it, and notes the removal in _meta.
            self.assertFalse(event["request"].get("data"))
            frames = [
                frame
                for exception in event.get("exception", {}).get("values", [])
                for frame in exception["stacktrace"]["frames"]
            ]
            self.assertFalse(any("vars" in frame for frame in frames))
            self.assertNotIn("Jane", json.dumps(event))
            self.assertNotIn("Doe", json.dumps(event))

    def test_sentry_defaults_send_the_learner_name(self) -> None:
        events = self._send_failing_course_completion({})

        self.assertTrue(any("Jane" in json.dumps(event) for event in events))
