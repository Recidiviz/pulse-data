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

"""Constants related to an assessment entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateAssessmentClass(StateEntityEnum):
    """An enumeration of assessment classifications tracked in our schema."""

    MENTAL_HEALTH = state_enum_strings.state_assessment_class_mental_health
    RISK = state_enum_strings.state_assessment_class_risk
    SECURITY_CLASSIFICATION = (
        state_enum_strings.state_assessment_class_security_classification
    )
    SEX_OFFENSE = state_enum_strings.state_assessment_class_sex_offense
    SOCIAL = state_enum_strings.state_assessment_class_social
    SUBSTANCE_ABUSE = state_enum_strings.state_assessment_class_substance_abuse
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateAssessmentClass"]:
        return _STATE_ASSESSMENT_CLASS_MAP

    @staticmethod
    def get_enum_description() -> str:
        return "TODO(#12127): Add enum description"

    @staticmethod
    def get_value_descriptions() -> Dict["StateEntityEnum", str]:
        return _STATE_ASSESSMENT_CLASS_VALUE_DESCRIPTIONS


_STATE_ASSESSMENT_CLASS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAssessmentClass.MENTAL_HEALTH: "TODO(#12127): Add enum value description",
    StateAssessmentClass.RISK: "TODO(#12127): Add enum value description",
    StateAssessmentClass.SECURITY_CLASSIFICATION: "TODO(#12127): Add enum value description",
    StateAssessmentClass.SEX_OFFENSE: "TODO(#12127): Add enum value description",
    StateAssessmentClass.SOCIAL: "TODO(#12127): Add enum value description",
    StateAssessmentClass.SUBSTANCE_ABUSE: "TODO(#12127): Add enum value description",
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateAssessmentType(StateEntityEnum):
    """An enumeration of assessment types tracked in our schema."""

    ASI = state_enum_strings.state_assessment_type_asi
    CSSM = state_enum_strings.state_assessment_type_cssm
    HIQ = state_enum_strings.state_assessment_type_hiq
    J_SOAP = state_enum_strings.state_assessment_type_j_soap
    LSIR = state_enum_strings.state_assessment_type_lsir
    ODARA = state_enum_strings.state_assessment_type_odara
    ORAS_COMMUNITY_SUPERVISION = (
        state_enum_strings.state_assessment_type_oras_community_supervision
    )
    ORAS_COMMUNITY_SUPERVISION_SCREENING = (
        state_enum_strings.state_assessment_type_oras_community_supervision_screening
    )
    ORAS_MISDEMEANOR_ASSESSMENT = (
        state_enum_strings.state_assessment_type_oras_misdemeanor_assessment
    )
    ORAS_MISDEMEANOR_SCREENING = (
        state_enum_strings.state_assessment_type_oras_misdemeanor_screening
    )
    ORAS_PRE_TRIAL = state_enum_strings.state_assessment_type_oras_pre_trial
    ORAS_PRISON_SCREENING = (
        state_enum_strings.state_assessment_type_oras_prison_screening
    )
    ORAS_PRISON_INTAKE = state_enum_strings.state_assessment_type_oras_prison_intake
    ORAS_REENTRY = state_enum_strings.state_assessment_type_oras_reentry
    ORAS_STATIC = state_enum_strings.state_assessment_type_oras_static
    ORAS_SUPPLEMENTAL_REENTRY = (
        state_enum_strings.state_assessment_type_oras_supplemental_reentry
    )
    OYAS = state_enum_strings.state_assessment_type_oyas
    PA_RST = state_enum_strings.state_assessment_type_pa_rst
    PSA = state_enum_strings.state_assessment_type_psa
    SORAC = state_enum_strings.state_assessment_type_sorac
    SOTIPS = state_enum_strings.state_assessment_type_sotips
    SPIN_W = state_enum_strings.state_assessment_type_spin_w
    STABLE = state_enum_strings.state_assessment_type_stable
    STATIC_99 = state_enum_strings.state_assessment_type_static_99
    STRONG_R = state_enum_strings.state_assessment_type_strong_r
    TCU_DRUG_SCREEN = state_enum_strings.state_assessment_type_tcu_drug_screen
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateAssessmentType"]:
        return _STATE_ASSESSMENT_TYPE_MAP


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateAssessmentLevel(StateEntityEnum):
    MINIMUM = state_enum_strings.state_assessment_level_minimum
    LOW = state_enum_strings.state_assessment_level_low
    LOW_MEDIUM = state_enum_strings.state_assessment_level_low_medium
    MEDIUM = state_enum_strings.state_assessment_level_medium
    MEDIUM_HIGH = state_enum_strings.state_assessment_level_medium_high
    MODERATE = state_enum_strings.state_assessment_level_moderate
    HIGH = state_enum_strings.state_assessment_level_high
    VERY_HIGH = state_enum_strings.state_assessment_level_very_high
    MAXIMUM = state_enum_strings.state_assessment_level_maximum
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateAssessmentLevel"]:
        return _STATE_ASSESSMENT_LEVEL_TYPE_MAP


_STATE_ASSESSMENT_CLASS_MAP = {
    "MENTAL HEALTH": StateAssessmentClass.MENTAL_HEALTH,
    "MH": StateAssessmentClass.MENTAL_HEALTH,
    "RISK": StateAssessmentClass.RISK,
    "SECURITY CLASSIFICATION": StateAssessmentClass.SECURITY_CLASSIFICATION,
    "SEX OFFENSE": StateAssessmentClass.SEX_OFFENSE,
    "SOCIAL": StateAssessmentClass.SOCIAL,
    "SUBSTANCE ABUSE": StateAssessmentClass.SUBSTANCE_ABUSE,
    "SUBSTANCE": StateAssessmentClass.SUBSTANCE_ABUSE,
    "INTERNAL UNKNOWN": StateAssessmentClass.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateAssessmentClass.EXTERNAL_UNKNOWN,
}


_STATE_ASSESSMENT_TYPE_MAP = {
    "ASI": StateAssessmentType.ASI,
    "CSSM": StateAssessmentType.CSSM,
    "HIQ": StateAssessmentType.HIQ,
    "J SOAP": StateAssessmentType.J_SOAP,
    "LSIR": StateAssessmentType.LSIR,
    "ODARA": StateAssessmentType.ODARA,
    "ORAS COMMUNITY SUPERVISION": StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
    "ORAS COMMUNITY SUPERVISION SCREENING": StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
    "ORAS MISDEMEANOR ASSESSMENT": StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT,
    "ORAS MISDEMEANOR SCREENING": StateAssessmentType.ORAS_MISDEMEANOR_SCREENING,
    "ORAS PRE TRIAL": StateAssessmentType.ORAS_PRE_TRIAL,
    "ORAS PRISON SCREENING": StateAssessmentType.ORAS_PRISON_SCREENING,
    "ORAS PRISON INTAKE": StateAssessmentType.ORAS_PRISON_INTAKE,
    "ORAS REENTRY": StateAssessmentType.ORAS_REENTRY,
    "ORAS STATIC": StateAssessmentType.ORAS_STATIC,
    "ORAS SUPPLEMENTAL REENTRY": StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY,
    "OYAS": StateAssessmentType.OYAS,
    "PA RST": StateAssessmentType.PA_RST,
    "PSA": StateAssessmentType.PSA,
    "SORAC": StateAssessmentType.SORAC,
    "SOTIPS": StateAssessmentType.SOTIPS,
    "SPIN W": StateAssessmentType.SPIN_W,
    "STABLE": StateAssessmentType.STABLE,
    "STATIC 99": StateAssessmentType.STATIC_99,
    "STRONG R": StateAssessmentType.STRONG_R,
    "TCU": StateAssessmentType.TCU_DRUG_SCREEN,
    "TCU DRUG SCREEN": StateAssessmentType.TCU_DRUG_SCREEN,
    "INTERNAL UNKNOWN": StateAssessmentType.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateAssessmentType.EXTERNAL_UNKNOWN,
}

_STATE_ASSESSMENT_LEVEL_TYPE_MAP = {
    "MINIMUM": StateAssessmentLevel.MINIMUM,
    "LOW": StateAssessmentLevel.LOW,
    "LOW MEDIUM": StateAssessmentLevel.LOW_MEDIUM,
    "MEDIUM": StateAssessmentLevel.MEDIUM,
    "MEDIUM HIGH": StateAssessmentLevel.MEDIUM_HIGH,
    "MODERATE": StateAssessmentLevel.MODERATE,
    "HIGH": StateAssessmentLevel.HIGH,
    "VERY HIGH": StateAssessmentLevel.VERY_HIGH,
    "MAXIMUM": StateAssessmentLevel.MAXIMUM,
    "INTERNAL UNKNOWN": StateAssessmentLevel.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateAssessmentLevel.EXTERNAL_UNKNOWN,
}
