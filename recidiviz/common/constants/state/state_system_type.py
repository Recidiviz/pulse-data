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
# =============================================================================
"""
Defines an enum that can be used to associate data with an overarching part of the
criminal justice system.
"""
from enum import unique

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateSystemType(StateEntityEnum):
    INCARCERATION = state_enum_strings.state_system_type_incarceration
    SUPERVISION = state_enum_strings.state_system_type_supervision
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "Defines the overarching part of the criminal justice system that a piece "
            "of data should be associated with."
        )

    @classmethod
    def get_value_descriptions(cls) -> dict["StateEntityEnum", str]:
        return {
            StateSystemType.INCARCERATION: (
                "Can be used to identify any piece of information in our schema that "
                "pertains only to incarceration."
            ),
            StateSystemType.SUPERVISION: (
                "Can be used to identify any piece of information in our schema that "
                "pertains only to supervision (i.e. probation or parole)."
            ),
        }
