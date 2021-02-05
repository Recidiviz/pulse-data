# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Class that parses an enum value from raw text, given a provided set of enum mappings."""

from typing import Generic, Optional, Type, Callable

import attr

from recidiviz.common.constants.entity_enum import EntityEnumT
from recidiviz.common.constants.enum_overrides import EnumOverrides


@attr.s(frozen=True)
class EnumParser(Generic[EntityEnumT]):
    """Class that parses an enum value from raw text, given a provided set of enum mappings."""

    raw_text: Optional[str] = attr.ib()
    enum_cls: Type[EntityEnumT] = attr.ib()
    enum_overrides: EnumOverrides = attr.ib()

    def parse(self) -> Optional[EntityEnumT]:
        if not self.raw_text:
            return None
        parsed = self.enum_cls.parse(self.raw_text, self.enum_overrides)

        if parsed is not None and not isinstance(parsed, self.enum_cls):
            raise ValueError(
                f'Unexpected type for parsed enum. Expected type [{self.enum_cls}], found [{type(parsed)}]. '
                f'Parsed value: [{parsed}].')
        return parsed


def get_parser_for_enum_with_default(default: EntityEnumT) -> Callable[[EnumParser[EntityEnumT]], EntityEnumT]:
    """Returns a converter function that parses a particular enum, but returns the default if parsing returns None."""
    def _parse_enum_with_default(enum_parser: EnumParser[EntityEnumT]) -> EntityEnumT:
        return enum_parser.parse() or default
    return _parse_enum_with_default
