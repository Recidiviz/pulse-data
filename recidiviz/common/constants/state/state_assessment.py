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


@unique
class StateAssessmentClass(StateEntityEnum):
    """An enumeration of assessment classifications tracked in our schema."""

    EDUCATION = state_enum_strings.state_assessment_class_education
    MENTAL_HEALTH = state_enum_strings.state_assessment_class_mental_health
    RISK = state_enum_strings.state_assessment_class_risk
    SEX_OFFENSE = state_enum_strings.state_assessment_class_sex_offense
    SOCIAL = state_enum_strings.state_assessment_class_social
    SUBSTANCE_ABUSE = state_enum_strings.state_assessment_class_substance_abuse
    MEDICAL = state_enum_strings.state_assessment_class_medical
    WORK = state_enum_strings.state_assessment_class_work
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The classification of the assessment."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_ASSESSMENT_CLASS_VALUE_DESCRIPTIONS


_STATE_ASSESSMENT_CLASS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAssessmentClass.EDUCATION: "Describes an assessment that evaluates the education level of an individual.",
    StateAssessmentClass.MENTAL_HEALTH: "Describes an assessment that evaluates the mental health of an individual.",
    StateAssessmentClass.RISK: "Describes an assessment that evaluates the risk of an "
    "individual.",
    StateAssessmentClass.SEX_OFFENSE: "Describes an assessment that evaluates the "
    "risk of an individual, where the assessment is specifically built for individuals "
    "convicted of a sex offense.",
    StateAssessmentClass.SOCIAL: "Describes an assessment that evaluates the social "
    "attitudes and beliefs of an individual.",
    StateAssessmentClass.SUBSTANCE_ABUSE: "Describes an assessment that evaluates the "
    "degree of substance use by an individual.",
    StateAssessmentClass.MEDICAL: "Describes an assessment that evaluates the health and/or medical history of an individual.",
    StateAssessmentClass.WORK: "Describes an assessment that evaluates the work experience and/or potential to gain employment.",
}


@unique
class StateAssessmentType(StateEntityEnum):
    """An enumeration of assessment types tracked in our schema."""

    CAF = state_enum_strings.state_assessment_type_caf
    CSRA = state_enum_strings.state_assessment_type_csra
    CSSM = state_enum_strings.state_assessment_type_cssm
    CMHS = state_enum_strings.state_assessment_type_cmhs
    COMPAS = state_enum_strings.state_assessment_type_compas
    HIQ = state_enum_strings.state_assessment_type_hiq
    ICASA = state_enum_strings.state_assessment_type_icasa
    J_SOAP = state_enum_strings.state_assessment_type_j_soap
    LSIR = state_enum_strings.state_assessment_type_lsir
    LS_RNR = state_enum_strings.state_assessment_type_ls_rnr
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
    SACA = state_enum_strings.state_assessment_type_saca
    SORAC = state_enum_strings.state_assessment_type_sorac
    SOTIPS = state_enum_strings.state_assessment_type_sotips
    SPIN_W = state_enum_strings.state_assessment_type_spin_w
    STABLE = state_enum_strings.state_assessment_type_stable
    STATIC_99 = state_enum_strings.state_assessment_type_static_99
    STRONG_R = state_enum_strings.state_assessment_type_strong_r
    STRONG_R2 = state_enum_strings.state_assessment_type_strong_r2
    TABE = state_enum_strings.state_assessment_type_tabe
    TCU_DRUG_SCREEN = state_enum_strings.state_assessment_type_tcu_drug_screen
    TX_CST = state_enum_strings.state_assessment_type_tx_cst
    TX_CSST = state_enum_strings.state_assessment_type_tx_csst
    TX_SRT = state_enum_strings.state_assessment_type_tx_srt
    TX_RT = state_enum_strings.state_assessment_type_tx_rt
    ACCAT = state_enum_strings.state_assessment_type_accat
    ACUTE = state_enum_strings.state_assessment_type_acute
    RSLS = state_enum_strings.state_assessment_type_rsls
    CCRRA = state_enum_strings.state_assessment_type_ccrra
    AZ_GEN_RISK_LVL = state_enum_strings.state_assessment_type_az_gen_risk_lvl
    AZ_VLNC_RISK_LVL = state_enum_strings.state_assessment_type_az_vlnc_risk_lvl
    MO_CLASSIFICATION_E = state_enum_strings.state_assessment_type_mo_classification_e
    MO_CLASSIFICATION_MH = state_enum_strings.state_assessment_type_mo_classification_mh
    MO_CLASSIFICATION_M = state_enum_strings.state_assessment_type_mo_classification_m
    MO_CLASSIFICATION_W = state_enum_strings.state_assessment_type_mo_classification_w
    MO_CLASSIFICATION_V = state_enum_strings.state_assessment_type_mo_classification_v
    MO_CLASSIFICATION_I = state_enum_strings.state_assessment_type_mo_classification_i
    MO_CLASSIFICATION_P = state_enum_strings.state_assessment_type_mo_classification_p
    MO_1270 = state_enum_strings.state_assessment_type_mo_1270
    MI_SECURITY_CLASS = state_enum_strings.state_assessment_type_mi_security_class
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The type of the assessment."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_ASSESSMENT_TYPE_VALUE_DESCRIPTIONS


_STATE_ASSESSMENT_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateAssessmentType.CAF: "Custody Assessment Form (CAF)",
    StateAssessmentType.CSRA: "California Static Risk Assessment",
    StateAssessmentType.CSSM: "Criminal Sentiments Scale – Modified (CSM-R)",
    StateAssessmentType.CMHS: "Correctional Mental Health Screen (CMHS)",
    StateAssessmentType.COMPAS: "Correctional Offender Management Profiling for Alternative Sanctions (COMPAS)",
    StateAssessmentType.HIQ: "Hostile Interpretations Questionnaire (HIQ) ",
    StateAssessmentType.ICASA: "International Collaboration on ADHD and Substance Abuse (ICASA)",
    StateAssessmentType.J_SOAP: "Juvenile Sex Offender Assessment Protocol-II "
    "(J-SOAP-II)",
    StateAssessmentType.LSIR: "Level of Service Inventory - Revised (LSI-R)",
    StateAssessmentType.LS_RNR: "Level of Service/Risk, Need, Responsivity",
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
    StateAssessmentType.SACA: "Screening for Alcohol and Chemical Abuse (SACA)",
    StateAssessmentType.SORAC: "Sex Offender Risk Assessment Committee (SORAC)",
    StateAssessmentType.SOTIPS: "Sex Offender Treatment Intervention and Progress "
    "Scale (SOTIPS)",
    StateAssessmentType.SPIN_W: "Service Planning Instrument for Women (SPIn-W)",
    StateAssessmentType.STABLE: "Sex offense risk assessment (STABLE-2007)",
    StateAssessmentType.STATIC_99: "Sex offense risk assessment (STATIC-99/STATIC-99R)",
    StateAssessmentType.STRONG_R: "Static Risk Offender Needs Guide – Revised "
    "(STRONG-R)",
    StateAssessmentType.STRONG_R2: "Static Risk Offender Needs Guide – Revised "
    "(STRONG-R) 2.0 Version",
    StateAssessmentType.TABE: "Test of Adult Basic Education",
    StateAssessmentType.TCU_DRUG_SCREEN: "Texas Christian University (TCU) Drug "
    "Screen (TCU)",
    StateAssessmentType.TX_CST: "Community Supervision Tool (CST)",
    StateAssessmentType.TX_CSST: "Community Supervision Screening Tool (CSST)",
    StateAssessmentType.TX_SRT: "Supplementary Reentry Tool (SRT)",
    StateAssessmentType.TX_RT: "Reentry Tool (RT)",
    StateAssessmentType.ACCAT: "Arizona Community Corrections Assessment Tool",
    StateAssessmentType.ACUTE: "Sex offense risk assessment for dynamic factors (ACUTE-2007)",
    StateAssessmentType.RSLS: "Reclassification Security Level Scoresheet (RSLS)",
    StateAssessmentType.CCRRA: "Arizona Community Corrections Risk Release Assessment",
    StateAssessmentType.AZ_GEN_RISK_LVL: "Arizona Community General Risk Level",
    StateAssessmentType.AZ_VLNC_RISK_LVL: "Arizona Community Violence Risk Level",
    StateAssessmentType.MO_CLASSIFICATION_E: "Missouri “E” (Education) Assessment",
    StateAssessmentType.MO_CLASSIFICATION_MH: "Missouri “MH” (Mental Health) Assessment",
    StateAssessmentType.MO_CLASSIFICATION_M: "Missouri “M” (Medical) Assessment",
    StateAssessmentType.MO_CLASSIFICATION_W: "Missouri “W” (Work) Assessment",
    StateAssessmentType.MO_CLASSIFICATION_V: "Missouri “V” (Vocational) Assessment",
    StateAssessmentType.MO_CLASSIFICATION_I: "Missouri “I” (Institutional Risk) Assessment",
    StateAssessmentType.MO_CLASSIFICATION_P: "Missouri “P” (Public Risk) Assessment",
    StateAssessmentType.MO_1270: "Missouri Gang/STG Affiliation Assessment",
    StateAssessmentType.MI_SECURITY_CLASS: "Michigan Security Classification Screen",
}


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
    LOW_MODERATE = state_enum_strings.state_assessment_level_low_moderate
    INTENSE = state_enum_strings.state_assessment_level_intense
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

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
    StateAssessmentLevel.LOW_MODERATE: "Category of “Low-Moderate”, as defined by the "
    "assessment.",
    StateAssessmentLevel.VERY_HIGH: "Category of “Very High”, as defined by the "
    "assessment.",
    StateAssessmentLevel.INTENSE: "Category of “Intense”, as defined by the assessment."
    " This is considered higher than the `MAXIMUM` category.",
}
