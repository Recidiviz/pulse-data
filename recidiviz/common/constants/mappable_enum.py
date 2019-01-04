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
"""Contains logic related to MappableEnums"""

from enum import Enum
from typing import Dict


class EnumParsingError(Exception):
    """Raised if an MappableEnum can't be built from the provided string."""

    def __init__(self, cls: type, string: str):
        msg = "Could not parse {0} when building {1}".format(string, cls)
        super().__init__(msg)


class MappableEnum(Enum):
    """Enum class that can be mapped from a string.

    When extending this class, you must override: _get_default_map
    """

    @classmethod
    def from_str(cls,
                 label: str,
                 override_map: Dict[str, 'MappableEnum'] = None) \
            -> 'MappableEnum':

        complete_map = cls._get_default_map()

        if override_map is not None:
            cls_override_map = {k: v for k, v in override_map.items()
                                if isinstance(v, cls)}

            complete_map = complete_map.copy()
            complete_map.update(cls_override_map)

        try:
            return complete_map[label.upper()]
        except KeyError:
            raise EnumParsingError(cls, label.upper())

    @staticmethod
    def _get_default_map() -> Dict[str, 'MappableEnum']:
        raise NotImplementedError
