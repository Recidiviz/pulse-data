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
from enum import unique

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


@unique
class StateChargeClassificationType(EntityEnum, metaclass=EntityEnumMeta):
    CIVIL = state_enum_strings.state_charge_classification_type_civil
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    FELONY = state_enum_strings.state_charge_classification_type_felony
    INFRACTION = state_enum_strings.state_charge_classification_type_infraction
    MISDEMEANOR = state_enum_strings.state_charge_classification_type_misdemeanor
    OTHER = state_enum_strings.state_charge_classification_type_other

    @staticmethod
    def _get_default_map():
        return _STATE_CHARGE_CLASSIFICATION_TYPE_MAP


# MappableEnum.parse will strip punctuation and separate tokens with a single
# space. Add mappings here using a single space between words and numbers.
# For example, `N/A` can be written as `N A` and `(10%)` can be written as `10`.
_STATE_CHARGE_CLASSIFICATION_TYPE_MAP = {
    "*": None,
    "-": None,
    ".": None,
    "0": None,
    "ADMINISTRATIVE": StateChargeClassificationType.CIVIL,
    "C": StateChargeClassificationType.CIVIL,
    "CITED CITATION": StateChargeClassificationType.INFRACTION,
    "CIVIL": StateChargeClassificationType.CIVIL,
    "COMM": StateChargeClassificationType.CIVIL,
    "CRIMINAL TRAFFIC VIOLATION": StateChargeClassificationType.INFRACTION,
    "ENHANCEMENT": None,
    "EXTERNAL UNKNOWN": StateChargeClassificationType.EXTERNAL_UNKNOWN,
    "F": StateChargeClassificationType.FELONY,
    "FEL": StateChargeClassificationType.FELONY,
    "FELONY": StateChargeClassificationType.FELONY,
    "I": StateChargeClassificationType.INFRACTION,
    "INFRACTION": StateChargeClassificationType.INFRACTION,
    "M": StateChargeClassificationType.MISDEMEANOR,
    "MIS": StateChargeClassificationType.MISDEMEANOR,
    "MISD": StateChargeClassificationType.MISDEMEANOR,
    "MISDEMEANOR": StateChargeClassificationType.MISDEMEANOR,
    "MUNICIPAL ORDINANCE VIOLATION": StateChargeClassificationType.INFRACTION,
    "N A": None,
    "NA": None,
    "NON ARREST TRAFFIC VIOLATION": StateChargeClassificationType.INFRACTION,
    "NON MOVING TRAFFIC VIOLATION": StateChargeClassificationType.INFRACTION,
    "O": StateChargeClassificationType.OTHER,
    "OTH": StateChargeClassificationType.OTHER,
    "OTHER": StateChargeClassificationType.OTHER,
    "SUMMONS": StateChargeClassificationType.INFRACTION,
    "U": StateChargeClassificationType.EXTERNAL_UNKNOWN,
    "UNKNOWN": StateChargeClassificationType.EXTERNAL_UNKNOWN,
}
