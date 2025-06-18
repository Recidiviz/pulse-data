# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Google API Retry Predicate to be used throughout the Recidiviz codebase."""


from google.api_core import exceptions  # pylint: disable=no-name-in-module
from google.api_core import retry
from requests.exceptions import SSLError

EXCEEDED_RATE_LIMITS_ERROR_MESSAGE: str = "Exceeded rate limits:"
RATE_LIMIT_INITIAL_DELAY = 15.0  # 15 seconds
RATE_LIMIT_MAXIMUM_DELAY = 60.0 * 2  # 2 minutes, in seconds
RATE_LIMIT_TOTAL_TIMEOUT = 60.0 * 5  # 5 minutes, in seconds


def google_api_retry_predicate(
    exception: Exception,
) -> bool:
    """A function that will determine whether we should retry a given Google exception."""
    return (
        retry.if_transient_error(exception)
        or retry.if_exception_type(exceptions.GatewayTimeout)(exception)
        or retry.if_exception_type(exceptions.BadGateway)(exception)
    )


def ssl_error_retry_predicate(exc: BaseException) -> bool:
    """Unexpected SSL EOF Errors may occur when working with threaded requests, this predicate will retry them"""
    return isinstance(exc, SSLError)


def rate_limit_retry_predicate(exc: Exception) -> bool:
    """Retries if there are SSL errors or rate-limit errors."""
    return ssl_error_retry_predicate(exc) or (
        isinstance(exc, exceptions.GoogleAPICallError)
        and isinstance(exc, exceptions.Forbidden)
        and EXCEEDED_RATE_LIMITS_ERROR_MESSAGE in str(exc)
    )


def bad_request_retry_predicate(exc: Exception) -> bool:
    """Retries google client errors and bad request errors.
    In general, bad request errors should not be retried, but there are some cases
    where google returns a 400 due to some underlying rate limiting."""
    return google_api_retry_predicate(exc) or isinstance(exc, exceptions.BadRequest)
