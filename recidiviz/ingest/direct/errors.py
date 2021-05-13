# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""File that contains all errors specific to the direct ingest package"""
from enum import Enum, auto


class DirectIngestErrorType(Enum):
    ENVIRONMENT_ERROR = auto()
    INPUT_ERROR = auto()
    TASKS_ERROR = auto()
    READ_ERROR = auto()
    PARSE_ERROR = auto()
    PERSISTENCE_ERROR = auto()
    CLEANUP_ERROR = auto()


class DirectIngestError(Exception):
    """Raised when a direct ingest controller runs into an error."""

    def __init__(self, *, msg: str, error_type: DirectIngestErrorType):
        super().__init__(msg)
        self.error_type = error_type

    def is_bad_request(self) -> bool:
        return (
            self.error_type == DirectIngestErrorType.ENVIRONMENT_ERROR
            or self.error_type == DirectIngestErrorType.INPUT_ERROR
        )


class DirectIngestInstanceError(Exception):
    """Raised when trying to access functionality that is not allowed for a given instance."""

    def __init__(self, msg: str):
        super().__init__(msg)
