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
# =============================================================================

"""Contains logic related to EntityEnums."""

import re
from enum import Enum, EnumMeta
from typing import Dict, Optional

from recidiviz.common.common_utils import normalize


class EnumParsingError(Exception):
    """Raised if an MappableEnum can't be built from the provided string."""

    def __init__(self, cls: type, string_to_parse: str):
        msg = "Could not parse {0} when building {1}".format(string_to_parse,
                                                             cls)
        super().__init__(msg)


class EntityEnumMeta(EnumMeta):
    """Metaclass for mappable enums."""


class EntityEnum(Enum, metaclass=EntityEnumMeta):
    """Enum class that can be mapped from a string.

    When extending this class, you must override: _get_default_map
    """

    @classmethod
    def parse(cls,
              label: str,
              enum_overrides: 'EnumOverrides') -> Optional['EntityEnum']:
        """Attempts to parse |label| using the default map of |cls| and the
        provided |override_map|. Ignores punctuation by treating punctuation as
        a separator, e.g. `(N/A)` will map to the same value as `N A`."""
        label = normalize(label, remove_punctuation=True)
        if enum_overrides.should_ignore(label, cls):
            return None

        overridden_value = enum_overrides.parse(label, cls)
        if isinstance(overridden_value, cls):
            return overridden_value

        return cls._parse_to_enum(label, cls._get_default_map())

    @classmethod
    def can_parse(cls, label: str, enum_overrides: 'EnumOverrides') -> bool:
        """Checks if the given string will parse into this enum.

        Convenience method to be used by a child scraper to tell if a given
        string should be used for this field.
        """
        try:
            cls.parse(label, enum_overrides)
            return True
        except EnumParsingError:
            return False

    @classmethod
    def find_in_string(cls, text: Optional[str]) -> Optional['EntityEnum']:
        if not text:
            return None
        for inst in cls:
            if re.search(inst.value.replace('_', ' '), text, re.I):
                return inst
        return None

    @classmethod
    def _parse_to_enum(cls, label, complete_map):
        try:
            return complete_map[label]
        except KeyError:
            raise EnumParsingError(cls, label)

    @staticmethod
    def _get_default_map() -> Dict[str, 'EntityEnum']:
        raise NotImplementedError
