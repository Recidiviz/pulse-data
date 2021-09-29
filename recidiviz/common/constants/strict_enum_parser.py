# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Defines a class that parses an enum value from raw text, given a provided set of enum
mappings.
"""

from typing import Optional, Type

import attr

from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumT
from recidiviz.common.constants.enum_parser import (
    EnumParser,
    EnumParsingError,
    parse_to_enum,
)


@attr.s(frozen=True)
class LiteralEnumParser(EnumParser[EnumT]):
    """Class that "parses" an enum value, always returning the exact same value."""

    enum_value: EnumT = attr.ib()

    def parse(self) -> EnumT:
        return self.enum_value


@attr.s(frozen=True)
class StrictEnumParser(EnumParser[EnumT]):
    """Class that parses an enum value from raw text, given a provided set of enum
    mappings. It does no normalization of the input text before attempting to find a
    mapping.
    """

    raw_text: Optional[str] = attr.ib()
    enum_cls: Type[EnumT] = attr.ib()
    enum_overrides: EnumOverrides = attr.ib()

    def parse(self) -> Optional[EnumT]:
        """Parses an enum value from raw text, given a provided set of enum mappings.
        It does no normalization of the input text before attempting to find a mapping.
        Throws an EnumParsingError if no mapping is found and the mappings object does
        not indicate it should be ignored. Returns null if the raw text value should
        just be ignored.
        """
        if not self.raw_text:
            return None

        if self.enum_overrides.should_ignore(self.raw_text, self.enum_cls):
            return None

        parsed_enum = parse_to_enum(self.enum_cls, self.raw_text, self.enum_overrides)
        if parsed_enum is None:
            raise EnumParsingError(self.enum_cls, self.raw_text)

        if not isinstance(parsed_enum, self.enum_cls):
            raise ValueError(
                f"Unexpected type [{type(parsed_enum)}] for parsed value "
                f"[{parsed_enum}] of raw text [{self.raw_text}]. "
                f"Expected [{self.enum_cls}]."
            )
        return parsed_enum
