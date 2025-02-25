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

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateAssessmentClass(EntityEnum, metaclass=EntityEnumMeta):
    MENTAL_HEALTH = state_enum_strings.state_assessment_class_mental_health
    RISK = state_enum_strings.state_assessment_class_risk
    SECURITY_CLASSIFICATION = \
        state_enum_strings.state_assessment_class_security_classification
    SUBSTANCE_ABUSE = state_enum_strings.state_assessment_class_substance_abuse

    @staticmethod
    def _get_default_map():
        return _STATE_ASSESSMENT_CLASS_MAP


class StateAssessmentType(EntityEnum, metaclass=EntityEnumMeta):
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    ASI = state_enum_strings.state_assessment_type_asi
    LSIR = state_enum_strings.state_assessment_type_lsir
    ORAS = state_enum_strings.state_assessment_type_oras
    ORAS_COMMUNITY_SUPERVISION = \
        state_enum_strings.state_assessment_type_oras_community_supervision
    ORAS_COMMUNITY_SUPERVISION_SCREENING = \
        state_enum_strings.\
        state_assessment_type_oras_community_supervision_screening
    ORAS_MISDEMEANOR_ASSESSMENT = \
        state_enum_strings.state_assessment_type_oras_misdemeanor_assessment
    ORAS_MISDEMEANOR_SCREENING = \
        state_enum_strings.state_assessment_type_oras_misdemeanor_screening
    ORAS_PRE_TRIAL = state_enum_strings.state_assessment_type_oras_pre_trial
    ORAS_PRISON_SCREENING = \
        state_enum_strings.state_assessment_type_oras_prison_screening
    ORAS_PRISON_INTAKE = \
        state_enum_strings.state_assessment_type_oras_prison_intake
    ORAS_REENTRY = state_enum_strings.state_assessment_type_oras_reentry
    ORAS_STATIC = state_enum_strings.state_assessment_type_oras_static
    ORAS_SUPPLEMENTAL_REENTRY = \
        state_enum_strings.state_assessment_type_oras_supplemental_reentry
    PSA = state_enum_strings.state_assessment_type_psa
    SORAC = state_enum_strings.state_assessment_type_sorac

    @staticmethod
    def _get_default_map():
        return _STATE_ASSESSMENT_TYPE_MAP


class StateAssessmentLevel(EntityEnum, metaclass=EntityEnumMeta):
    LOW = state_enum_strings.state_assessment_level_low
    LOW_MEDIUM = state_enum_strings.state_assessment_level_low_medium
    MEDIUM = state_enum_strings.state_assessment_level_medium
    MEDIUM_HIGH = state_enum_strings.state_assessment_level_medium_high
    MODERATE = state_enum_strings.state_assessment_level_moderate
    HIGH = state_enum_strings.state_assessment_level_high
    VERY_HIGH = state_enum_strings.state_assessment_level_very_high
    NOT_APPLICABLE = state_enum_strings.state_assessment_level_not_applicable
    UNDETERMINED = state_enum_strings.state_assessment_level_undetermined

    @staticmethod
    def _get_default_map():
        return _STATE_ASSESSMENT_LEVEL_TYPE_MAP


_STATE_ASSESSMENT_CLASS_MAP = {
    'MENTAL HEALTH': StateAssessmentClass.MENTAL_HEALTH,
    'MH': StateAssessmentClass.MENTAL_HEALTH,
    'RISK': StateAssessmentClass.RISK,
    'SECURITY CLASSIFICATION': StateAssessmentClass.SECURITY_CLASSIFICATION,
    'SUBSTANCE ABUSE': StateAssessmentClass.SUBSTANCE_ABUSE,
    'SUBSTANCE': StateAssessmentClass.SUBSTANCE_ABUSE,
}


_STATE_ASSESSMENT_TYPE_MAP = {
    'ASI': StateAssessmentType.ASI,
    'LSIR': StateAssessmentType.LSIR,
    'ORAS': StateAssessmentType.ORAS,
    'ORAS COMMUNITY SUPERVISION':
        StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
    'ORAS COMMUNITY SUPERVISION SCREENING':
        StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
    'ORAS MISDEMEANOR ASSESSMENT':
        StateAssessmentType.ORAS_MISDEMEANOR_ASSESSMENT,
    'ORAS MISDEMEANOR SCREENING':
        StateAssessmentType.ORAS_MISDEMEANOR_SCREENING,
    'ORAS PRE TRIAL': StateAssessmentType.ORAS_PRE_TRIAL,
    'ORAS PRISON SCREENING': StateAssessmentType.ORAS_PRISON_SCREENING,
    'ORAS PRISON INTAKE': StateAssessmentType.ORAS_PRISON_INTAKE,
    'ORAS REENTRY': StateAssessmentType.ORAS_REENTRY,
    'ORAS STATIC': StateAssessmentType.ORAS_STATIC,
    'ORAS SUPPLEMENTAL REENTRY': StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY,
    'PSA': StateAssessmentType.PSA,
    'SORAC': StateAssessmentType.SORAC,
    'INTERNAL UNKNOWN': StateAssessmentType.INTERNAL_UNKNOWN,
}

_STATE_ASSESSMENT_LEVEL_TYPE_MAP = {
    'LOW': StateAssessmentLevel.LOW,
    'LOW MEDIUM': StateAssessmentLevel.LOW_MEDIUM,
    'MEDIUM': StateAssessmentLevel.MEDIUM,
    'MEDIUM HIGH': StateAssessmentLevel.MEDIUM_HIGH,
    'MODERATE': StateAssessmentLevel.MODERATE,
    'HIGH': StateAssessmentLevel.HIGH,
    'NOT APPLICABLE': StateAssessmentLevel.NOT_APPLICABLE,
    'UNDETERMINED': StateAssessmentLevel.UNDETERMINED,
    'VERY HIGH': StateAssessmentLevel.VERY_HIGH,
}
