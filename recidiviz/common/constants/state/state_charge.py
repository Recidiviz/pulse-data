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

"""Constants related to a StateCharge entity in the state schema."""

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateChargeClassification(EntityEnum, metaclass=EntityEnumMeta):
    CIVIL = state_enum_strings.state_charge_classification_civil
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FELONY = state_enum_strings.state_charge_classification_felony
    INFRACTION = state_enum_strings.state_charge_classification_infraction
    MISDEMEANOR = state_enum_strings.state_charge_classification_misdemeanor
    OTHER = state_enum_strings.state_charge_classification_other

    @staticmethod
    def _get_default_map():
        return _STATE_CHARGE_CLASSIFICATION_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_STATE_CHARGE_CLASSIFICATION_MAP = {
    '*': None,
    '-': None,
    '.': None,
    '0': None,
    'ADMINISTRATIVE': StateChargeClassification.CIVIL,
    'C': StateChargeClassification.CIVIL,
    'CITED CITATION': StateChargeClassification.INFRACTION,
    'CIVIL': StateChargeClassification.CIVIL,
    'COMM': StateChargeClassification.CIVIL,
    'CRIMINAL TRAFFIC VIOLATION': StateChargeClassification.INFRACTION,
    'ENHANCEMENT': None,
    'F': StateChargeClassification.FELONY,
    'F2': StateChargeClassification.FELONY,
    'FA': StateChargeClassification.FELONY,
    'FAA': StateChargeClassification.FELONY,
    'FB': StateChargeClassification.FELONY,
    'FC': StateChargeClassification.FELONY,
    'FEL': StateChargeClassification.FELONY,
    'FELONY': StateChargeClassification.FELONY,
    'I': StateChargeClassification.INFRACTION,
    'INFRACTION': StateChargeClassification.INFRACTION,
    'M': StateChargeClassification.MISDEMEANOR,
    'MA': StateChargeClassification.MISDEMEANOR,
    'MB': StateChargeClassification.MISDEMEANOR,
    'MIS': StateChargeClassification.MISDEMEANOR,
    'MISD': StateChargeClassification.MISDEMEANOR,
    'MISDEMEANOR': StateChargeClassification.MISDEMEANOR,
    'MUNICIPAL ORDINANCE VIOLATION': StateChargeClassification.INFRACTION,
    'N A': None,
    'NA': None,
    'NON ARREST TRAFFIC VIOLATION': StateChargeClassification.INFRACTION,
    'NON MOVING TRAFFIC VIOLATION': StateChargeClassification.INFRACTION,
    'O': StateChargeClassification.OTHER,
    'OTH': StateChargeClassification.OTHER,
    'OTHER': StateChargeClassification.OTHER,
    'SUMMONS': StateChargeClassification.INFRACTION,
    'UNKNOWN': StateChargeClassification.EXTERNAL_UNKNOWN,
}
