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

import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class AssessmentClass(EntityEnum, metaclass=EntityEnumMeta):
    MENTAL_HEALTH = state_enum_strings.assessment_class_mental_health
    RISK = state_enum_strings.assessment_class_risk
    SECURITY_CLASSIFICATION = \
        state_enum_strings.assessment_class_security_classification
    SUBSTANCE_ABUSE = state_enum_strings.assessment_class_substance_abuse

    @staticmethod
    def _get_default_map():
        return _ASSESSMENT_CLASS_MAP


class AssessmentType(EntityEnum, metaclass=EntityEnumMeta):
    ASI = state_enum_strings.assessment_type_asi
    LSIR = state_enum_strings.assessment_type_lsir
    ORAS = state_enum_strings.assessment_type_oras
    PSA = state_enum_strings.assessment_type_psa

    @staticmethod
    def _get_default_map():
        return _ASSESSMENT_TYPE_MAP


_ASSESSMENT_CLASS_MAP = {
    'MENTAL HEALTH': AssessmentClass.MENTAL_HEALTH,
    'MH': AssessmentClass.MENTAL_HEALTH,
    'RISK': AssessmentClass.RISK,
    'SUBSTANCE ABUSE': AssessmentClass.SUBSTANCE_ABUSE,
    'SUBSTANCE': AssessmentClass.SUBSTANCE_ABUSE,
}


_ASSESSMENT_TYPE_MAP = {
    'ASI': AssessmentType.ASI,
    'LSIR': AssessmentType.LSIR,
    'ORAS': AssessmentType.ORAS,
    'PSA': AssessmentType.PSA,
}
