# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
# ============================================================================
"""Reads fields from an ingest_info proto and parses them into EntityEnums."""
from typing import Dict, Optional, Set, Mapping

from more_itertools import one

from recidiviz.common.constants.entity_enum import EntityEnumMeta, EntityEnum
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.persistence.converter.converter_utils import fn


class EnumMappings:
    """Reads fields from an ingest_info proto and parses them into EntityEnums.
    Initialize this class with a specific proto, a mapping of parsing functions,
    and the overrides map. Call enum_mappings.get to retrieve the value from
    an enum type."""

    def __init__(self, proto, enum_fields: Mapping[str, EntityEnumMeta],
                 overrides: EnumOverrides):
        """
        Initializes a mapping from enum fields within a single Entity to enum
        values. If enum fields map to enum values in a different entity, those
        values will be ignored. EnumMappings prefers values that are the same
        type as the field they are from; for example, if both ChargeClass and
        ChargeStatus fields map to a ChargeClass, we will return the ChargeClass
        from the charge_class field. However, if multiple fields map to
        different values of the another enum type (for example, if ChargeDegree
        and ChargeStatus fields map to two ChargeClasses), an error will be
        raised on calling |get|.
        Args:
            proto: the proto to read fields from, e.g. Person, Booking, etc.
            enum_fields: a mapping from field names (e.g. custody_status) to
                         parsing functions (e.g. CustodyStatus.parse).
            overrides: the enum overrides mapping.
        """
        self.parsed_enums_from_original_field: \
            Dict[EntityEnumMeta, EntityEnum] = dict()
        self.all_parsed_enums: Set[EntityEnum] = set()

        for field_name, from_enum in enum_fields.items():
            value = fn(from_enum.parse, field_name, proto, overrides)
            if not value:
                continue
            if isinstance(value, from_enum):
                self.parsed_enums_from_original_field[from_enum] = value
            else:
                self.all_parsed_enums.add(value)

    def get(self,
            enum_type: EntityEnumMeta,
            default: EntityEnum = None) -> Optional[EntityEnum]:
        value_from_field = self.parsed_enums_from_original_field.get(enum_type)
        if value_from_field:
            return value_from_field

        matching_values = {enum for enum in self.all_parsed_enums
                           if isinstance(enum, enum_type)}

        if not matching_values:
            return default

        multiple_matches_error = ValueError(
            "Found multiple values for enum field %s: %s" %
            (enum_type, matching_values))

        return one(matching_values, too_long=multiple_matches_error)
