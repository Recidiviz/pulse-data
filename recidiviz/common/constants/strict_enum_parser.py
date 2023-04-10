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
from enum import Enum
from typing import Callable, Dict, Generic, Optional, Set, Type, TypeVar

from recidiviz.common.constants.enum_parser import EnumParsingError

EnumT = TypeVar("EnumT", bound=Enum)


# TODO(#8905): Rename to EnumParser and move back into `enum_parser.py`
class StrictEnumParser(Generic[EnumT]):
    """Class that parses an enum value from raw text, given a provided set of enum
    mappings. It does no normalization of the input text before attempting to find a
    mapping.
    """

    def __init__(self, enum_cls: Type[EnumT]) -> None:
        self.enum_cls = enum_cls
        self.ignored_raw_text_values: Set[str] = set()
        self.raw_text_mappings: Optional[Dict[str, EnumT]] = None
        self.mapper_fn: Optional[Callable[[str], Optional[EnumT]]] = None

    def add_raw_text_mapping(
        self, enum_value: EnumT, raw_text_value: str
    ) -> "StrictEnumParser":
        if not self.raw_text_mappings:
            if self.mapper_fn:
                raise ValueError(
                    f"Must define either a mapper function or raw text mappings for "
                    f"[{self.enum_cls.__name__}] but not both."
                )
            self.raw_text_mappings = {}
        self._check_not_already_mapped(raw_text_value)
        self.raw_text_mappings[raw_text_value] = enum_value
        return self

    def add_mapper_fn(
        self, mapper_fn: Callable[[str], Optional[EnumT]]
    ) -> "StrictEnumParser":
        if self.raw_text_mappings:
            raise ValueError(
                f"Must define either a mapper function or raw text mappings for "
                f"[{self.enum_cls.__name__}] but not both."
            )
        if self.mapper_fn:
            raise ValueError(
                f"Mapper function already defined for [{self.enum_cls.__name__}]"
            )
        self.mapper_fn = mapper_fn
        return self

    def ignore_raw_text_value(self, raw_text_value: str) -> "StrictEnumParser":
        self._check_not_already_mapped(raw_text_value)
        self.ignored_raw_text_values.add(raw_text_value)
        return self

    def _check_not_already_mapped(self, raw_text_value: str) -> None:
        if raw_text_value in self.ignored_raw_text_values or (
            self.raw_text_mappings and raw_text_value in self.raw_text_mappings
        ):
            raise ValueError(f"Raw text value [{raw_text_value}] already mapped.")

    def parse(self, raw_text: Optional[str]) -> Optional[EnumT]:
        """Parses an enum value from raw text, given the mapping information provided to
        this class.

        It does no normalization of the input text before attempting to find a mapping.
        Throws an EnumParsingError if no mapping is found and the mappings do not
        indicate it should be ignored. Returns null if the raw text value should
        just be ignored.
        """
        if not raw_text:
            return None

        if raw_text in self.ignored_raw_text_values:
            return None

        if self.mapper_fn:
            try:
                parsed_enum = self.mapper_fn(raw_text)
            except Exception as e:
                if isinstance(e, EnumParsingError):
                    raise e

                # If a mapper function throws another type of error, convert it to an
                # enum parsing error.
                raise EnumParsingError(self.enum_cls, raw_text) from e

        elif self.raw_text_mappings:
            parsed_enum = self.raw_text_mappings.get(raw_text)
        else:
            raise ValueError(
                f"Cannot parse [{self.enum_cls.__name__}] without defining either a "
                f"mapper function or explicit raw text mappings."
            )

        if parsed_enum is None:
            raise EnumParsingError(self.enum_cls, raw_text)

        if not isinstance(parsed_enum, self.enum_cls):
            raise ValueError(
                f"Unexpected type [{type(parsed_enum)}] for parsed value "
                f"[{parsed_enum}] of raw text [{raw_text}]. "
                f"Expected [{self.enum_cls}]."
            )
        return parsed_enum
