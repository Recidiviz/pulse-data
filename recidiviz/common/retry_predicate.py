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
from ssl import SSLError

from google.api_core import exceptions  # pylint: disable=no-name-in-module
from google.api_core import retry
from urllib3.connectionpool import MaxRetryError


def google_api_retry_predicate(
    exception: BaseException,
) -> bool:
    """A function that will determine whether we should retry a given Google exception."""
    return (
        retry.if_transient_error(exception)
        or retry.if_exception_type(exceptions.GatewayTimeout)(exception)
        or retry.if_exception_type(exceptions.BadGateway)(exception)
    )


def ssl_error_retry_predicate(exc: BaseException) -> bool:
    """Unexpected SSL EOF Errors may occur when working with threaded requests, this predicate will retry them"""
    return isinstance(exc, MaxRetryError) and isinstance(exc.reason, SSLError)
