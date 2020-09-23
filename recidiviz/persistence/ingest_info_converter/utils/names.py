# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utilities for working with names in the ingest info converter.

TODO(#1861): Add unit tests instead of relying on implicit testing elsewhere
"""

import json
from typing import Optional

import attr

from recidiviz.common.str_field_utils import normalize
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import \
    fn


def parse_name(proto) -> Optional[str]:
    """Parses name into a single string."""
    names = Names(
        full_name=fn(normalize, 'full_name', proto),
        given_names=fn(normalize, 'given_names', proto),
        middle_names=fn(normalize, 'middle_names', proto),
        surname=fn(normalize, 'surname', proto),
        name_suffix=fn(normalize, 'name_suffix', proto))
    return names.combine()


@attr.s(auto_attribs=True, frozen=True)
class Names:
    """Holds the various name fields"""
    full_name: Optional[str]
    given_names: Optional[str]
    middle_names: Optional[str]
    surname: Optional[str]
    name_suffix: Optional[str]

    def __attrs_post_init__(self):
        if self.full_name and any((self.given_names, self.middle_names,
                                   self.surname, self.name_suffix)):
            raise ValueError("Cannot have full_name and surname/middle/"
                             "given_names/name_suffix")

        if any((self.middle_names, self.name_suffix)) and \
                not any((self.given_names, self.surname)):
            raise ValueError("Cannot set only middle_names/name_suffix.")

    def combine(self) -> Optional[str]:
        """Writes the names out as a json string, skipping fields that are None.

        Note: We don't have any need for parsing these back into their parts,
        but this gives us other advantages. It handles escaping the names, and
        allows us to add fields in the future without changing the serialization
        of existing names.
        """
        filled_names = attr.asdict(self, filter=lambda a, v: v is not None)
        return json.dumps(filled_names, sort_keys=True) \
            if filled_names else None
