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
"""Tests for the SendGrid retry predicate in ut_app_enablement.py."""

import unittest

from python_http_client.exceptions import (
    GatewayTimeoutError,
    HTTPError,
    InternalServerError,
    ServiceUnavailableError,
    TooManyRequestsError,
    UnauthorizedError,
)

from recidiviz.tools.cpa.ut_app_enablement import _is_transient_sendgrid_error


def _http_error(error_class: type[HTTPError], status_code: int) -> HTTPError:
    return error_class(status_code, "reason", b"{}", {})


class TestIsTransientSendgridError(unittest.TestCase):
    """Tests that transient SendGrid responses are retried and permanent ones are not."""

    def test_retries_statuses_without_a_dedicated_exception_class(self) -> None:
        # SendGrid's err_dict has no entry for 499 or 502, so both surface as the base
        # HTTPError. A 499 is what broke the job in production.
        for status_code in (499, 502):
            with self.subTest(status_code=status_code):
                assert _is_transient_sendgrid_error(_http_error(HTTPError, status_code))

    def test_retries_mapped_transient_statuses(self) -> None:
        for error_class, status_code in (
            (TooManyRequestsError, 429),
            (InternalServerError, 500),
            (ServiceUnavailableError, 503),
            (GatewayTimeoutError, 504),
        ):
            with self.subTest(status_code=status_code):
                assert _is_transient_sendgrid_error(
                    _http_error(error_class, status_code)
                )

    def test_does_not_retry_permanent_http_errors(self) -> None:
        for error_class, status_code in (
            (UnauthorizedError, 401),
            (HTTPError, 400),
        ):
            with self.subTest(status_code=status_code):
                assert not _is_transient_sendgrid_error(
                    _http_error(error_class, status_code)
                )

    def test_does_not_retry_non_http_errors(self) -> None:
        assert not _is_transient_sendgrid_error(ValueError("not an HTTP error"))
