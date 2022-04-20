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
    # People for whom drug/alcohol addiction has been identified as a "risk area"
    ALCOHOL_DRUG = state_enum_strings.state_supervision_case_type_alcohol_drug
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
        return _STATE_CASE_TYPE_MAP


_STATE_CASE_TYPE_MAP = {
    "ALCOHOL DRUG": StateSupervisionCaseType.ALCOHOL_DRUG,
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
