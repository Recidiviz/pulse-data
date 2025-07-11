# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""
Custom exception classes for resource search and CRUD operations.
"""

import uuid
from typing import Any, Union

from werkzeug.exceptions import HTTPException


class APIError(HTTPException):
    """Base class for API errors"""

    code = 400
    description = "API Error"


class NotFoundError(APIError):
    """Entity not found error"""

    code = 404

    def __init__(self, modelClass: Any, resource_id: Union[str, uuid.UUID]) -> None:
        super().__init__()
        self.description = f"{modelClass.__name__} with id {resource_id} not found"
