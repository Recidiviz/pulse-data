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
"""Constants needed across multiple Justice Counts Metric files."""

import enum
from typing import Any, Dict, Tuple, Type


class ContextKey(enum.Enum):
    """Uniquely identifies Contexts that are required for metrics."""

    ADDITIONAL_CONTEXT = "ADDITIONAL_CONTEXT"
    AGENCIES_AVAILABLE_FOR_RESPONSE = "AGENCIES_AVAILABLE_FOR_RESPONSE"
    ALL_CALLS_OR_CALLS_RESPONDED = "ALL_CALLS_OR_CALLS_RESPONDED"
    JURISDICTION_AREA = "JURISDICTION_AREA"
    JURISDICTION_DEFINITION_OF_ARREST = "JURISDICTION_DEFINITION_OF_ARREST"
    JURISDICTION_DEFINITION_OF_USE_OF_FORCE = "JURISDICTION_DEFINITION_OF_USE_OF_FORCE"
    PRIMARY_FUNDING_SOURCE = "PRIMARY_FUNDING_SOURCE"
    PRETRIAL_SUPERVISION_FUNCTION = "PRETRIAL_SUPERVISION_FUNCTION"
    INCLUDES_PROGRAMATIC_STAFF = "INCLUDES_PROGRAMATIC_STAFF"
    JURISDICTION_DEFINITION_OF_ADMISSION = "JURISDICTION_DEFINITION_OF_ADMISSION"
    INCLUDES_VIOLATED_CONDITIONS = "INCLUDES_VIOLATED_CONDITIONS"


class ValueType(enum.Enum):
    """Different Context input types."""

    TEXT = "TEXT"
    NUMBER = "NUMBER"
    BOOLEAN = "BOOLEAN"

    @classmethod
    def value_type_to_python_type(cls) -> Dict[str, Tuple[Type[Any], ...]]:
        return {
            "TEXT": (str,),
            "NUMBER": (int, float),
            "BOOLEAN": (bool,),
        }

    def python_type(self) -> Tuple[Type[Any], ...]:
        return ValueType.value_type_to_python_type()[self.value]
