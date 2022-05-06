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

    RISK = state_enum_strings.state_assessment_class_risk
    SEX_OFFENSE = state_enum_strings.state_assessment_class_sex_offense
    SOCIAL = state_enum_strings.state_assessment_class_social
    SUBSTANCE_ABUSE = state_enum_strings.state_assessment_class_substance_abuse
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateAssessmentClass"]:
        return _STATE_ASSESSMENT_CLASS_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The classification of the assessment."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_ASSESSMENT_CLASS_VALUE_DESCRIPTIONS


_STATE_ASSESSMENT_CLASS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAssessmentClass.RISK: "Describes an assessment that evaluates the risk of an "
    "individual.",
    StateAssessmentClass.SEX_OFFENSE: "Describes an assessment that evaluates the "
    "risk of an individual, where the assessment is specifically built for individuals "
    "convicted of a sex offense.",
    StateAssessmentClass.SOCIAL: "Describes an assessment that evaluates the social "
    "attitudes and beliefs of an individual.",
    StateAssessmentClass.SUBSTANCE_ABUSE: "Describes an assessment that evaluates the "
    "degree of substance use by an individual.",
}

# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateAssessmentType(StateEntityEnum):
    """An enumeration of assessment types tracked in our schema."""

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

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of the assessment."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_ASSESSMENT_TYPE_VALUE_DESCRIPTIONS


_STATE_ASSESSMENT_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAssessmentType.CSSM: "Criminal Sentiments Scale – Modified (CSM-R)",
    StateAssessmentType.HIQ: "Hostile Interpretations Questionnaire (HIQ) ",
    StateAssessmentType.J_SOAP: "Juvenile Sex Offender Assessment Protocol-II "
    "(J-SOAP-II)",
    StateAssessmentType.LSIR: "Level of Service Inventory - Revised (LSI-R)",
    StateAssessmentType.ODARA: "Ontario Domestic Assault Risk Assessment",
    StateAssessmentType.ORAS_COMMUNITY_SUPERVISION: "Ohio Risk Assessment System - "
    "Community Supervision Tool (ORAS-CST)",
    StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING: "Ohio Risk Assessment "
    "System - Community Supervision Screening Tool (ORAS-CSST)",
    StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT: "Ohio Risk Assessment System - "
    "Misdemeanor Assessment Tool (ORAS-MAT)",
    StateAssessmentType.ORAS_MISDEMEANOR_SCREENING: "Ohio Risk Assessment System - "
    "Misdemeanor Screening Tool (ORAS-MST)",
    StateAssessmentType.ORAS_PRE_TRIAL: "Ohio Risk Assessment System - Pre-Trial "
    "Assessment Tool (ORAS-PAT)",
    StateAssessmentType.ORAS_PRISON_INTAKE: "Ohio Risk Assessment System - Prison "
    "Intake Tool (ORAS-PIT)",
    StateAssessmentType.ORAS_PRISON_SCREENING: "Ohio Risk Assessment System - Prison "
    "Screening Tool (ORAS-PST)",
    StateAssessmentType.ORAS_REENTRY: "Ohio Risk Assessment System - Reentry Tool "
    "(ORAS-RT)",
    StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY: "Ohio Risk Assessment System - "
    "Supplemental Reentry Tool (ORAS-SRT)",
    StateAssessmentType.OYAS: "Ohio Youth Assessment System (OYAS)",
    StateAssessmentType.PA_RST: "Pennsylvania Risk Screen Tool (RST)",
    StateAssessmentType.PSA: "Public Safety Assessment (PSA)",
    StateAssessmentType.SORAC: "Sex Offender Risk Assessment Committee (SORAC)",
    StateAssessmentType.SOTIPS: "Sex Offender Treatment Intervention and Progress "
    "Scale (SOTIPS)",
    StateAssessmentType.SPIN_W: "Service Planning Instrument for Women (SPIn-W)",
    StateAssessmentType.STABLE: "Sex offense risk assessment (STABLE-2007)",
    StateAssessmentType.STATIC_99: "Sex offense risk assessment (STATIC-99/STATIC-99R)",
    StateAssessmentType.STRONG_R: "Static Risk Offender Needs Guide – Revised "
    "(STRONG-R)",
    StateAssessmentType.TCU_DRUG_SCREEN: "Texas Christian University (TCU) Drug "
    "Screen (TCU)",
}


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

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The categorical classification determined by the values or score of "
            "the assessment, for assessments that produce discrete levels."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_ASSESSMENT_LEVEL_VALUE_DESCRIPTIONS


_STATE_ASSESSMENT_LEVEL_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAssessmentLevel.HIGH: "Category of “High”, as defined by the assessment.",
    StateAssessmentLevel.LOW: "Category of “Low”, as defined by the assessment.",
    StateAssessmentLevel.LOW_MEDIUM: "Category of “Low-Medium”, as defined by the "
    "assessment.",
    StateAssessmentLevel.MAXIMUM: "Category of “Maximum”, as defined by the "
    "assessment. This is considered higher than the `VERY_HIGH` category.",
    StateAssessmentLevel.MEDIUM: "Category of “Medium”, as defined by the assessment.",
    StateAssessmentLevel.MEDIUM_HIGH: "Category of “Medium-High”, as defined by the "
    "assessment.",
    StateAssessmentLevel.MINIMUM: "Category of “Minimum”, as defined by the "
    "assessment. This is considered lower than the `LOW` category.",
    StateAssessmentLevel.MODERATE: "Category of “Moderate”, as defined by the "
    "assessment.",
    StateAssessmentLevel.VERY_HIGH: "Category of “Very High”, as defined by the "
    "assessment.",
}


_STATE_ASSESSMENT_CLASS_MAP = {
    "RISK": StateAssessmentClass.RISK,
    "SEX OFFENSE": StateAssessmentClass.SEX_OFFENSE,
    "SOCIAL": StateAssessmentClass.SOCIAL,
    "SUBSTANCE ABUSE": StateAssessmentClass.SUBSTANCE_ABUSE,
    "SUBSTANCE": StateAssessmentClass.SUBSTANCE_ABUSE,
    "INTERNAL UNKNOWN": StateAssessmentClass.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateAssessmentClass.EXTERNAL_UNKNOWN,
}


_STATE_ASSESSMENT_TYPE_MAP = {
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
