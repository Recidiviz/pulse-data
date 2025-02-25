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
# =============================================================================
"""Util functions shared across multiple types of hooks in the direct
ingest controllers."""
from typing import Dict, List

from recidiviz.common.constants.enum_parser import EnumT


def invert_enum_to_str_mappings(overrides: Dict[EnumT, List[str]]) -> Dict[str, EnumT]:
    """Inverts a given dictionary, that maps a target enum from a list of parseable strings, into another dictionary,
    that maps each parseable string to its target enum."""
    inverted_overrides: Dict[str, EnumT] = {}
    for mapped_enum, text_tokens in overrides.items():
        for text_token in text_tokens:
            if text_token in inverted_overrides:
                raise ValueError(
                    f"Unexpected, text_token [{text_token}] is used for both "
                    f"[{mapped_enum}] and [{inverted_overrides[text_token]}]"
                )
            inverted_overrides[text_token] = mapped_enum

    return inverted_overrides


def invert_str_to_str_mappings(overrides: Dict[str, List[str]]) -> Dict[str, str]:
    """The same as invert_enum_to_str_mappings, but for raw target strings instead of typed enums."""
    inverted_overrides: Dict[str, str] = {}
    for mapped_str, text_tokens in overrides.items():
        for text_token in text_tokens:
            if text_token in inverted_overrides:
                raise ValueError(
                    f"Unexpected, text_token [{text_token}] is used for both "
                    f"[{mapped_str}] and [{inverted_overrides[text_token]}]"
                )
            inverted_overrides[text_token] = mapped_str

    return inverted_overrides
