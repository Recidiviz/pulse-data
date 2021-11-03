# Recidiviz - a data platform for criminal justice reform
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
from typing import Dict, Mapping, Optional, Set

from more_itertools import one

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import fn


class IngestInfoProtoEnumMapper:
    """Reads fields from an ingest_info proto and parses them into EntityEnums.
    Initialize this class with a specific proto, a mapping of parsing functions,
    and the overrides map. Call IngestInfoProtoEnumMapper.get to retrieve the value
    from an enum type."""

    def __init__(
        self, proto, enum_fields: Mapping[str, EntityEnumMeta], overrides: EnumOverrides
    ):
        """
        Initializes a mapping from enum fields within a single Entity to enum
        values. If enum fields map to enum values in a different entity, those
        values will be ignored. IngestInfoProtoEnumMapper prefers values that
        are the same type as the field they are from; for example, if both
        ChargeClass and ChargeStatus fields map to a ChargeClass, we will return
        the ChargeClass from the charge_class field. However, if multiple fields
        map to different values of the another enum type (for example, if
        ChargeDegree and ChargeStatus fields map to two ChargeClasses), an error
        will be raised on calling |get|. If the given enum fields dict contains
        multiple fields that map to the same enum type, both will be retrievable
        by their field name in the `get` method below.
        Args:
            proto: the proto to read fields from, e.g. Person, Booking, etc.
            enum_fields: a mapping from field names (e.g. custody_status) to
                         parsing functions (e.g. CustodyStatus.parse).
            overrides: the enum overrides mapping.
        """
        self.parsed_enums_from_original_field: Dict[
            EntityEnumMeta, Dict[str, EntityEnum]
        ] = {}
        self.all_parsed_enums: Set[EntityEnum] = set()

        for field_name, from_enum in enum_fields.items():
            value = fn(from_enum.parse, field_name, proto, overrides)
            if not value:
                continue
            if isinstance(value, from_enum):
                default_mapping: Dict[str, EntityEnum] = {}
                mappings_by_field = self.parsed_enums_from_original_field.get(
                    from_enum, default_mapping
                )
                mappings_by_field[field_name] = value
                self.parsed_enums_from_original_field[from_enum] = mappings_by_field
            else:
                self.all_parsed_enums.add(value)

    def get(
        self,
        enum_type: EntityEnumMeta,
        field_name: str = None,
        default: EntityEnum = None,
    ) -> Optional[EntityEnum]:
        """Returns the exact enum value mapped to the given |enum_type|.

        If this instance was instantiated with a map that contains multiple
        fields that map to the same enum type, you can retrieve the value for
        the specific field you want by passing it in via |field_name|. If this
        is the case but that arg is not provided, the returned value is
        arbitrary.

        |default| can be passed in to return a default value in the event that
        there was no properly parsed enum for a particular field.
        """
        field_mappings = self.parsed_enums_from_original_field.get(enum_type)
        if field_mappings:
            if field_name:
                return field_mappings.get(field_name, None)
            return next(iter(field_mappings.values()), None)

        matching_values = {
            enum for enum in self.all_parsed_enums if isinstance(enum, enum_type)
        }

        if not matching_values:
            return default

        multiple_matches_error = ValueError(
            f"Found multiple values for enum field {enum_type}: {matching_values}"
        )

        return one(matching_values, too_long=multiple_matches_error)
