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
from typing import Dict, Callable, Optional

from more_itertools import one

from recidiviz.common.constants.entity_enum import EntityEnumMeta, EntityEnum
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.persistence.converter.converter_utils import fn


class EnumMappings:
    """Reads fields from an ingest_info proto and parses them into EntityEnums.
    Initialize this class with a specific proto, a mapping of parsing functions,
    and the overrides map. Call enum_mappings.get to retrieve the value from
    an enum type."""
    def __init__(self, proto, enum_fields: Dict[str, Callable],
                 overrides: EnumOverrides):
        """
        Initializes a mapping from enum fields within a single Entity to enum
        values. If enum fields map to enum values in a different entity, those
        values will be ignored. If a single field maps to multiple enum values
        of the same enum class, an error will be raised on calling |get|.
        Args:
            proto: the proto to read fields from, e.g. Person, Booking, etc.
            enum_fields: a mapping from field names (e.g. custody_status) to
                         parsing functions (e.g. CustodyStatus.parse).
            overrides: the enum overrides mapping.
        """
        self.parsed_enums = {fn(parser, field_name, proto, overrides)
                             for field_name, parser in enum_fields.items()}

    def get(self,
            enum_type: EntityEnumMeta,
            default: EntityEnum = None) -> Optional[EntityEnum]:
        matching_values = {enum for enum in self.parsed_enums
                           if isinstance(enum, enum_type)}

        if not matching_values:
            return default

        multiple_matches_error = ValueError(
            "Found multiple values for enum field %s: %s" %
            (enum_type, matching_values))

        return one(matching_values, too_long=multiple_matches_error)
