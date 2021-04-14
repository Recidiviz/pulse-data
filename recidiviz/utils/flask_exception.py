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
"""Implements common interface for exceptions served by Flask frontend."""
from http import HTTPStatus
from typing import Union, List, Any, Dict


class FlaskException(Exception):
    """Implements common interface for exceptions served by Flask frontend."""

    def __init__(
        self,
        code: str,
        description: Union[str, List[Any], Dict[Any, Any]],
        status_code: HTTPStatus,
    ) -> None:
        self.code = code
        self.description = description
        self.status_code = status_code
        super().__init__()
