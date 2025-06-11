# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Utils for working with YAML files."""

# on/off were part of yaml 1.1: https://stackoverflow.com/questions/75853887/yaml-attributeerror-on-for-a-key-named-on-in-yaml-file
YAML_RESERVED_WORDS = frozenset(
    ["y", "yes", "n", "no", "true", "false", "on", "off", "null"]
)
YAML_RESERVED_CHARS = frozenset(
    [
        "#",
        ",",
        "[",
        "]",
        "{",
        "}",
        "&",
        "*",
        "!",
        "|",
        ">",
        "?",
        "'",
        "%",
        "@",
        "`",
        "-",
        ":",
        "~",
        '"',
    ]
)


def _is_number(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def _contains_word_starting_with_reserved_char(value: str) -> bool:
    return any(word[0] in YAML_RESERVED_CHARS for word in value.split())


def get_properly_quoted_yaml_str(value: str, always_quote: bool = False) -> str:
    if (
        not value
        or value.lower() in YAML_RESERVED_WORDS
        or _contains_word_starting_with_reserved_char(value)
        or _is_number(value)
        or always_quote
    ):
        value = value.replace('"', '\\"')
        return f'"{value}"'
    return f"{value}"
