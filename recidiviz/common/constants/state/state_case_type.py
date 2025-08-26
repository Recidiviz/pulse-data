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

"""Constants related to a StateSupervisionCaseTypeEntry entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateSupervisionCaseType(StateEntityEnum):
    """The classification of a period of supervision based on the offense type, the
    needs of the individual, or the diversionary program the
    supervision is associated with."""

    DOMESTIC_VIOLENCE = state_enum_strings.state_supervision_case_type_domestic_violence
    DRUG_COURT = state_enum_strings.state_supervision_case_type_drug_court
    FAMILY_COURT = state_enum_strings.state_supervision_case_type_family_court
    GENERAL = state_enum_strings.state_supervision_case_type_general
    LIFETIME_SUPERVISION = (
        state_enum_strings.state_supervision_case_type_lifetime_supervision
    )
    MENTAL_HEALTH_COURT = (
        state_enum_strings.state_supervision_case_type_mental_health_court
    )
    SERIOUS_MENTAL_ILLNESS_OR_DISABILITY = (
        state_enum_strings.state_supervision_case_type_serious_mental_illness_or_disability
    )
    SEX_OFFENSE = state_enum_strings.state_supervision_case_type_sex_offense
    VETERANS_COURT = state_enum_strings.state_supervision_case_type_veterans_court
    INTENSE_SUPERVISION = (
        state_enum_strings.state_supervision_case_type_intense_supervision
    )
    PHYSICAL_ILLNESS_OR_DISABILITY = (
        state_enum_strings.state_supervision_case_type_physical_illness_or_disability
    )
    DAY_REPORTING = state_enum_strings.state_supervision_case_type_day_reporting
    ELECTRONIC_MONITORING = (
        state_enum_strings.state_supervision_case_type_electronic_monitoring
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The classification of a period of supervision based on the offense "
            "type, the needs of the individual, or the diversionary program the "
            "supervision is associated with."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_CASE_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_CASE_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionCaseType.DOMESTIC_VIOLENCE: "Used when the person on supervision "
    "has convictions related to domestic violence.",
    StateSupervisionCaseType.DRUG_COURT: "Describes a period of supervision that "
    "someone is on as a participation requirement of a Drug Court diversionary "
    "program.",
    StateSupervisionCaseType.FAMILY_COURT: "Describes a period of supervision that "
    "someone is on as a participation requirement of a Family Court diversionary "
    "program.",
    StateSupervisionCaseType.GENERAL: "Used on a period of supervision that does not "
    "have any special classification.",
    StateSupervisionCaseType.LIFETIME_SUPERVISION: "Describes a type of supervision "
    "that someone is assigned to be on for life, it can be specific like a lifetime"
    "sex offense supervision or general as some kind of supervision intended to last for "
    "life. This is generally a type of supervision used in cases where someone has "
    "completed their initial sentence, then been sentenced to a term of lifetime "
    "supervision afterwards where the contact requirements are often different than "
    "those while they were serving their original sentence.",
    StateSupervisionCaseType.MENTAL_HEALTH_COURT: "Describes a period of supervision "
    "that someone is on as a participation requirement of a Mental Health Court "
    "diversionary program.",
    StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS_OR_DISABILITY: "Used when the person on "
    "supervision struggles with a serious mental illness or with a significant mental disability.",
    StateSupervisionCaseType.SEX_OFFENSE: "Used when the person on supervision has "
    "sex offense convictions.",
    StateSupervisionCaseType.VETERANS_COURT: "Describes a period of supervision that "
    "someone is on as a participation requirement of a Veterans Court diversionary "
    "program.",
    StateSupervisionCaseType.INTENSE_SUPERVISION: "Describes a period of supervision that "
    "someone is on intense or hightened supervision.",
    StateSupervisionCaseType.PHYSICAL_ILLNESS_OR_DISABILITY: "Used when the person on "
    "supervision struggles with a physical disability or with a significant physical "
    "illness, including being terminally ill.",
    StateSupervisionCaseType.DAY_REPORTING: "Describes a period of supervision where "
    "someone has to visit a reporting center daily",
    StateSupervisionCaseType.ELECTRONIC_MONITORING: "Describes a period of supervision where "
    "someone is under electronic monitoring",
}
