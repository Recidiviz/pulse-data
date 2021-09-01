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
"""Class that parses an EntityEnum value from raw text, given a provided set of enum
mappings.
"""
from enum import Enum
from typing import Optional, Type

import attr

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.enum_parser import (
    EnumParser,
    EnumParsingError,
    parse_to_enum,
)
from recidiviz.common.str_field_utils import normalize


@attr.s(frozen=True)
class DefaultingAndNormalizingEnumParser(EnumParser[Enum]):
    """Class that parses an EntityEnum value from raw text, given a provided set of enum
    mappings.
    """

    raw_text: Optional[str] = attr.ib()
    enum_cls: Type[Enum] = attr.ib()
    enum_overrides: EnumOverrides = attr.ib()

    def parse(self) -> Optional[Enum]:
        """Parses an EntityEnum value from raw text, given a provided set of enum
        mappings. It normalizes the input text to remove punctuation before attempting
        to find a mapping. If no mapping exists, looks in the enum class's
        _get_default_map() to find a mapping.

        Throws an EnumParsingError if no mapping is found and the mappings object does
        not indicate it should be ignored. Returns null if the raw text value should
        just be ignored.
        """
        if not self.raw_text:
            return None
        label = normalize(self.raw_text, remove_punctuation=True)

        parsed_value = parse_to_enum(self.enum_cls, label, self.enum_overrides)

        if self.enum_overrides.should_ignore(label, self.enum_cls):
            return None

        if parsed_value is not None:
            return parsed_value

        # pylint: disable=protected-access
        complete_map = self.enum_cls._get_default_map()  # type: ignore[attr-defined]
        try:
            return complete_map[label]
        except KeyError as e:
            raise EnumParsingError(self.enum_cls, label) from e
