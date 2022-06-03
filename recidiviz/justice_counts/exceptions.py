# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Contains the list of custom exceptions used by Justice Counts."""

from http import HTTPStatus

from recidiviz.utils.flask_exception import FlaskException


class JusticeCountsAuthorizationError(FlaskException):
    """Exception for when the user has signed into a valid account, but has not yet been allowed access."""

    def __init__(self, code: str, description: str) -> None:
        super().__init__(code, description, HTTPStatus.UNAUTHORIZED)


class JusticeCountsPermissionError(FlaskException):
    """Exception for when the user does not have the perissions to perform a certain action in the control panel."""

    def __init__(self, code: str, description: str) -> None:
        super().__init__(code, description, HTTPStatus.UNAUTHORIZED)


class JusticeCountsBadRequestError(FlaskException):
    """Exception for when the user makes a bad request."""

    def __init__(self, code: str, description: str) -> None:
        super().__init__(code, description, HTTPStatus.BAD_REQUEST)


class JusticeCountsDataError(FlaskException):
    """Exception for when the user submits invalid data for a metric."""

    def __init__(self, code: str, description: str) -> None:
        super().__init__(code, description, HTTPStatus.NOT_ACCEPTABLE)


class JusticeCountsServerError(FlaskException):
    """Exception for when an uncaught error occurs in an API request."""

    def __init__(self, code: str, description: str) -> None:
        super().__init__(code, description, HTTPStatus.INTERNAL_SERVER_ERROR)
