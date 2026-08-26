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
"""Sentry event filters for the Case Triage server."""
from urllib.parse import urlparse

from sentry_sdk.types import Event, Hint

from recidiviz.utils.types import assert_type

EMAIL_USER_PATH_SUFFIX = "/email_user"

# before_send and before_send_transaction both require hint parameter
# but it is not needed for our use cases
# pylint: disable=unused-argument
def scrub_email_user_pii(event: Event, hint: Hint) -> Event | None:
    """Removes client PII from the emailBody of email_user requests before the event
    is sent to Sentry.

    Passed as both before_send and before_send_transaction in sentry_sdk.init(): a
    successful email_user request only ever produces a transaction event, while a
    failed one also produces a separate error event (from the captured exception
    and/or the logging.error call), and both can carry the same request body.
    """
    parsed_url = urlparse(assert_type(event["request"]["url"], str))
    if not parsed_url.path.endswith(EMAIL_USER_PATH_SUFFIX):
        return event

    request_data = event["request"].get("data")
    if not isinstance(request_data, dict):
        return event

    email_body = request_data.get("emailBody")
    if not isinstance(email_body, str):
        return event

    # Scrubs the emailBody so no PII including the email list of clients
    # or the google maps link is able to make it through to Sentry
    request_data["emailBody"] = ""
    return event
