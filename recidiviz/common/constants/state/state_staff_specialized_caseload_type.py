# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Constants related to a SpecializedCaseloadType."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#24278): Delete this enum once StateStaffCaseloadType is fully implemented and
# appropriately hydrated.
@unique
class StateStaffSpecializedCaseloadType(StateEntityEnum):
    """Enum indicating the specialized case type an officer supervises"""

    SEX_OFFENSE = state_enum_strings.state_staff_specialized_caseload_type_sex_offense
    ADMINISTRATIVE_SUPERVISION = (
        state_enum_strings.state_staff_specialized_caseload_type_administrative_supervision
    )
    ALCOHOL_AND_DRUG = (
        state_enum_strings.state_staff_specialized_caseload_type_alcohol_and_drug
    )
    INTENSIVE = state_enum_strings.state_staff_specialized_caseload_type_intensive
    MENTAL_HEALTH = (
        state_enum_strings.state_staff_specialized_caseload_type_mental_health
    )
    ELECTRONIC_MONITORING = (
        state_enum_strings.state_staff_specialized_caseload_type_electronic_monitoring
    )
    OTHER_COURT = state_enum_strings.state_staff_specialized_caseload_type_other_court
    DRUG_COURT = state_enum_strings.state_staff_specialized_caseload_type_drug_court
    VETERANS_COURT = (
        state_enum_strings.state_staff_specialized_caseload_type_veterans_court
    )
    COMMUNITY_FACILITY = (
        state_enum_strings.state_staff_specialized_caseload_type_community_facility
    )
    OTHER = state_enum_strings.state_staff_specialized_caseload_type_other
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "Describes the specialized caseload type an officer supervises."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _SPECIALIZED_CASELOAD_TYPE_VALUE_DESCRIPTIONS


_SPECIALIZED_CASELOAD_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateStaffSpecializedCaseloadType.SEX_OFFENSE: (
        "A caseload that consists of only people charged with sex offenses."
    ),
    StateStaffSpecializedCaseloadType.ADMINISTRATIVE_SUPERVISION: (
        "A caseload that consists of only people who are on a very low level "
        "of supervision (e.g. compliant reporting), often referred to as 'administrative "
        "supervision. This is a caseload type, not a role type, and should be used for "
        "supervision officers, not people who do administrative work at an agency."
    ),
    StateStaffSpecializedCaseloadType.ALCOHOL_AND_DRUG: (
        "A caseload that consists of people charged with drug offenses or "
        "in need of substance abuse treatment."
    ),
    StateStaffSpecializedCaseloadType.INTENSIVE: (
        "A caseload that consists of people who require a high level of "
        "supervision or an intensive supervision program."
    ),
    StateStaffSpecializedCaseloadType.MENTAL_HEALTH: (
        "A caseload that consists of people who are in need of or are receiving "
        "mental health treatment, including designators for mental health court."
    ),
    StateStaffSpecializedCaseloadType.ELECTRONIC_MONITORING: (
        "A caseload of people who are being monitored electronically."
    ),
    StateStaffSpecializedCaseloadType.OTHER_COURT: (
        "Any court/hearing caseload designation that does not fall within "
        "any of the other specialty court categories."
    ),
    StateStaffSpecializedCaseloadType.DRUG_COURT: (
        "Any caseload that consists of people who are involved in proceedings in drug court."
    ),
    StateStaffSpecializedCaseloadType.VETERANS_COURT: (
        "Any caseload that consists of people who are veterans or in veterans treatment court."
    ),
    StateStaffSpecializedCaseloadType.COMMUNITY_FACILITY: (
        "A caseload that consists of people who are being supervised within "
        "a community reentry center or other community-based facility."
    ),
    StateStaffSpecializedCaseloadType.OTHER: (
        "A catch-all type for all other types of specialized caseloads. "
        "This should not be used when an officer has a general (non-specialized) caseload."
    ),
}


@unique
class StateStaffCaseloadType(StateEntityEnum):
    """Enum indicating the case type an officer supervises"""

    SEX_OFFENSE = state_enum_strings.state_staff_caseload_type_sex_offense
    ADMINISTRATIVE_SUPERVISION = (
        state_enum_strings.state_staff_caseload_type_administrative_supervision
    )
    ALCOHOL_AND_DRUG = state_enum_strings.state_staff_caseload_type_alcohol_and_drug
    INTENSIVE = state_enum_strings.state_staff_caseload_type_intensive
    MENTAL_HEALTH = state_enum_strings.state_staff_caseload_type_mental_health
    ELECTRONIC_MONITORING = (
        state_enum_strings.state_staff_caseload_type_electronic_monitoring
    )
    OTHER_COURT = state_enum_strings.state_staff_caseload_type_other_court
    DRUG_COURT = state_enum_strings.state_staff_caseload_type_drug_court
    VETERANS_COURT = state_enum_strings.state_staff_caseload_type_veterans_court
    COMMUNITY_FACILITY = state_enum_strings.state_staff_caseload_type_community_facility
    OTHER = state_enum_strings.state_staff_caseload_type_other
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown
    GENERAL = state_enum_strings.state_staff_caseload_type_general

    @classmethod
    def get_enum_description(cls) -> str:
        return "Describes the caseload type an officer supervises."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _CASELOAD_TYPE_VALUE_DESCRIPTIONS


_CASELOAD_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateStaffCaseloadType.SEX_OFFENSE: (
        "A caseload that consists of only people charged with sex offenses."
    ),
    StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION: (
        "A caseload that consists of only people who are on a very low level "
        "of supervision (e.g. compliant reporting), often referred to as 'administrative "
        "supervision. This is a caseload type, not a role type, and should be used for "
        "supervision officers, not people who do administrative work at an agency."
    ),
    StateStaffCaseloadType.ALCOHOL_AND_DRUG: (
        "A caseload that consists of people charged with drug offenses or "
        "in need of substance abuse treatment."
    ),
    StateStaffCaseloadType.INTENSIVE: (
        "A caseload that consists of people who require a high level of "
        "supervision or an intensive supervision program."
    ),
    StateStaffCaseloadType.MENTAL_HEALTH: (
        "A caseload that consists of people who are in need of or are receiving "
        "mental health treatment, including designators for mental health court."
    ),
    StateStaffCaseloadType.ELECTRONIC_MONITORING: (
        "A caseload of people who are being monitored electronically."
    ),
    StateStaffCaseloadType.OTHER_COURT: (
        "Any court/hearing caseload designation that does not fall within "
        "any of the other specialty court categories."
    ),
    StateStaffCaseloadType.DRUG_COURT: (
        "Any caseload that consists of people who are involved in proceedings in drug court."
    ),
    StateStaffCaseloadType.VETERANS_COURT: (
        "Any caseload that consists of people who are veterans or in veterans treatment court."
    ),
    StateStaffCaseloadType.COMMUNITY_FACILITY: (
        "A caseload that consists of people who are being supervised within "
        "a community reentry center or other community-based facility."
    ),
    StateStaffCaseloadType.GENERAL: ("Any caseload that is not specialized."),
    StateStaffCaseloadType.OTHER: (
        "A catch-all type for all other types of  caseloads. "
        "This should not be used when an officer has a general (non-specialized) caseload."
    ),
}
