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
"""Defines enums used by the StatePersonStaffRelationshipPeriod entity"""

from enum import unique

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StatePersonStaffRelationshipType(StateEntityEnum):
    CASE_MANAGER = state_enum_strings.state_person_staff_relationship_type_case_manager
    SUPERVISING_OFFICER = (
        state_enum_strings.state_person_staff_relationship_type_supervising_officer
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "Defines the type of relationship or role a staff member has with respect "
            "to a justice impacted individual."
        )

    @classmethod
    def get_value_descriptions(cls) -> dict["StateEntityEnum", str]:
        return {
            StatePersonStaffRelationshipType.CASE_MANAGER: (
                "This staff member is the person who is responsible for working with an incarcerated individual "
                "through their time in prison. A case manager’s responsibilities include connecting the JII with "
                "programming and other services, completing assessments, and doing reentry planning."
            ),
            StatePersonStaffRelationshipType.SUPERVISING_OFFICER: (
                "This staff member is the parole / probation officer who is responsible for supervising a JII "
                "during their time on supervision. The relationship may have this value for a staff member "
                "with any defined StateStaffRoleSubtype. As long as this staff member directly supervises this person, "
                "this type applies. "
            ),
        }
