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
import abc
from enum import Enum
from typing import Generic, Optional, Type

import attr

from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumT


class EnumParsingError(Exception):
    """Raised if an enum can't be built from the provided string."""

    def __init__(self, cls: type, string_to_parse: str):
        msg = f"Could not parse {string_to_parse} when building {cls}"
        self.entity_type = cls
        super().__init__(msg)


@attr.s(frozen=True)
class EnumParser(Generic[EnumT]):
    """Interface for a class that parses an enum value from raw text, given a provided
    set of enum mappings.
    """

    @abc.abstractmethod
    def parse(self) -> Optional[EnumT]:
        """Should be overridden by implementing classes to return an enum value parsed
        from raw text. Should throw an EnumParsingError on failure, or null if the
        raw text is marked as "should ignore".
        """


def parse_to_enum(
    enum_cls: Type[EnumT], label: str, enum_overrides: EnumOverrides
) -> Optional[Enum]:
    if enum_overrides.should_ignore(label, enum_cls):
        return None

    try:
        overridden_value = enum_overrides.parse(label, enum_cls)
    except Exception as e:
        if isinstance(e, EnumParsingError):
            raise e

        # If a mapper function throws another type of error, convert it to an enum
        # parsing error.
        raise EnumParsingError(enum_cls, label) from e

    return overridden_value
