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

import enum
from datetime import date
from http import HTTPStatus
from typing import Any, Optional, Tuple

from recidiviz.utils.flask_exception import FlaskException


class BulkUploadMessageType(enum.Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class BulkUploadSheetNameError(KeyError):
    """BulkUploadSheetNameError is raised when a user uploads an Excel workbook
    with a sheet name that does not match the the sheets we are expecting
    for their agency. This error is different from JusticeCountsBulkUploadException
    because this is not ultimately surfaced to the user. This error is used to compile
    the list of invalid sheet names that will be surfaced in the Invalid Sheet Name error.
    """

    def __init__(
        self,
        sheet_name: str,
    ):
        super().__init__()
        self.sheet_name = sheet_name


class JusticeCountsBulkUploadException(Exception):
    """
    Each field is incorporated into the design in the following way:
    - title: header field is on the left side of the error row.
    - subtitle: displayed on the right side of the error row, and gives more description about the error.
    - error_type: helps the FE know what symbol to render to the left of the title.
    - description: smaller text displayed under the title/subtitle.
    """

    def __init__(
        self,
        title: str,
        description: str,
        message_type: BulkUploadMessageType,
        time_range: Optional[Tuple[date, date]] = None,
        subtitle: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ):
        super().__init__(description)
        self.title = title
        self.subtitle = subtitle or (
            f"{time_range[0].month}/{time_range[0].year}" if time_range else None
        )
        self.description = description

        # A JusticeCountsBulkUploadException can be either warnings or errors.
        # Warnings do now prevent data publishing, but errors do.
        self.message_type = message_type
        # JusticeCountsBulkUploadExceptions that are associated with a particular
        # sheet will have a non-None sheet name. JusticeCountsBulkUploadExceptions
        # that are associated with an entire metric will have a sheet_name
        # value of None.
        self.sheet_name = sheet_name

    def to_json(self) -> dict[str, Any]:
        return {
            "type": self.message_type.value,
            "title": self.title,
            "subtitle": self.subtitle,
            "description": self.description,
        }


class JusticeCountsServerError(FlaskException):
    """Exception for Justice Counts server errors."""

    def __init__(self, code: str, description: str) -> None:
        super().__init__(code, description, HTTPStatus.INTERNAL_SERVER_ERROR)
