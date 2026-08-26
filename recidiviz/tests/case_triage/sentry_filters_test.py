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
from typing import Any, cast
from unittest import TestCase

from sentry_sdk.types import Event

from recidiviz.case_triage.sentry_filters import scrub_email_user_pii
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
