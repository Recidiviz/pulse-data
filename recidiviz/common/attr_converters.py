# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Contains helper functions to use as a "converter" functions in an attr.ib
and largely exist to make MyPy happy.
"""
import json
from enum import Enum
from typing import Any, Dict, Optional

from recidiviz.utils.types import assert_type


def str_to_lowercase_str(s: str) -> str:
    return s.lower()


def optional_str_to_lowercase_str(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.lower()


def str_to_uppercase_str(s: str) -> str:
    return s.upper()


def optional_str_to_uppercase_str(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.upper()


def optional_json_str_to_dict(s: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parses a JSON string into a dictionary. If not null, the provided JSON string
    must contain a top-level dictionary or this throws.
    """
    if s is None:
        return None

    if not s:
        raise ValueError("Expecting valid JSON and not the empty string.")

    return assert_type(json.loads(s), dict)


def optional_enum_value_from_enum(s: Enum | None) -> Any | None:
    if s is None:
        return None
    return s.value
