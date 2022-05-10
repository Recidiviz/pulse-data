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


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionCaseType(StateEntityEnum):
    """The classification of a period of supervision based on the offense type, the
    needs of the individual, or the diversionary program the
    supervision is associated with."""

    DOMESTIC_VIOLENCE = state_enum_strings.state_supervision_case_type_domestic_violence
    DRUG_COURT = state_enum_strings.state_supervision_case_type_drug_court
    FAMILY_COURT = state_enum_strings.state_supervision_case_type_family_court
    GENERAL = state_enum_strings.state_supervision_case_type_general
    MENTAL_HEALTH_COURT = (
        state_enum_strings.state_supervision_case_type_mental_health_court
    )
    SERIOUS_MENTAL_ILLNESS = (
        state_enum_strings.state_supervision_case_type_serious_mental_illness
    )
    SEX_OFFENSE = state_enum_strings.state_supervision_case_type_sex_offense
    VETERANS_COURT = state_enum_strings.state_supervision_case_type_veterans_court
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionCaseType"]:
        return _STATE_SUPERVISION_CASE_TYPE_MAP

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
    StateSupervisionCaseType.MENTAL_HEALTH_COURT: "Describes a period of supervision "
    "that someone is on as a participation requirement of a Mental Health Court "
    "diversionary program.",
    StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS: "Used when the person on "
    "supervision struggles with a serious mental illness.",
    StateSupervisionCaseType.SEX_OFFENSE: "Used when the person on supervision has "
    "sex offense convictions.",
    StateSupervisionCaseType.VETERANS_COURT: "Describes a period of supervision that "
    "someone is on as a participation requirement of a Veterans Court diversionary "
    "program.",
}

_STATE_SUPERVISION_CASE_TYPE_MAP = {
    "DOMESTIC VIOLENCE": StateSupervisionCaseType.DOMESTIC_VIOLENCE,
    "DRUG COURT": StateSupervisionCaseType.DRUG_COURT,
    "FAMILY COURT": StateSupervisionCaseType.FAMILY_COURT,
    "GENERAL": StateSupervisionCaseType.GENERAL,
    "MENTAL HEALTH COURT": StateSupervisionCaseType.MENTAL_HEALTH_COURT,
    "SERIOUS MENTAL ILLNESS": StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
    "SEX OFFENSE": StateSupervisionCaseType.SEX_OFFENSE,
    "VETERANS COURT": StateSupervisionCaseType.VETERANS_COURT,
    "INTERNAL UNKNOWN": StateSupervisionCaseType.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateSupervisionCaseType.EXTERNAL_UNKNOWN,
}
