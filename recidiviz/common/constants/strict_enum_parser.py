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

from typing import Callable, Generic, Optional, Type

import attr

from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumT
from recidiviz.common.constants.enum_parser import EnumParsingError


# TODO(#8905): Rename to EnumParser and move back into `enum_parser.py`
@attr.s(frozen=True)
class StrictEnumParser(Generic[EnumT]):
    """Class that parses an enum value from raw text, given a provided set of enum
    mappings. It does no normalization of the input text before attempting to find a
    mapping.
    """

    enum_cls: Type[EnumT] = attr.ib()

    # TODO(#8905): Delete the EnumOverrides class entirely and move relevant
    #  functionality to this class.
    enum_overrides_builder: EnumOverrides.Builder = attr.ib(
        factory=EnumOverrides.Builder
    )

    def add_raw_text_mapping(
        self, enum_value: EnumT, raw_text_value: str
    ) -> "StrictEnumParser":
        self.enum_overrides_builder.add(
            raw_text_value, enum_value, normalize_label=False
        )
        return self

    def add_mapper_fn(
        self, mapper_fn: Callable[[str], Optional[EnumT]]
    ) -> "StrictEnumParser":
        self.enum_overrides_builder.add_mapper_fn(mapper_fn, self.enum_cls)
        return self

    def ignore_raw_text_value(self, raw_text_value: str) -> "StrictEnumParser":
        self.enum_overrides_builder.ignore(
            raw_text_value, from_field=self.enum_cls, normalize_label=False
        )
        return self

    def parse(self, raw_text: Optional[str]) -> Optional[EnumT]:
        """Parses an enum value from raw text, given a provided set of enum mappings.
        It does no normalization of the input text before attempting to find a mapping.
        Throws an EnumParsingError if no mapping is found and the mappings object does
        not indicate it should be ignored. Returns null if the raw text value should
        just be ignored.
        """
        if not raw_text:
            return None

        enum_overrides = self.enum_overrides_builder.build()
        if enum_overrides.should_ignore(raw_text, self.enum_cls):
            return None

        try:
            parsed_enum = enum_overrides.parse(raw_text, self.enum_cls)
        except Exception as e:
            if isinstance(e, EnumParsingError):
                raise e

            # If a mapper function throws another type of error, convert it to an enum
            # parsing error.
            raise EnumParsingError(self.enum_cls, raw_text) from e

        if parsed_enum is None:
            raise EnumParsingError(self.enum_cls, raw_text)

        if not isinstance(parsed_enum, self.enum_cls):
            raise ValueError(
                f"Unexpected type [{type(parsed_enum)}] for parsed value "
                f"[{parsed_enum}] of raw text [{raw_text}]. "
                f"Expected [{self.enum_cls}]."
            )
        return parsed_enum
