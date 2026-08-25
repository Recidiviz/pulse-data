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
import yaml

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


# Wide enough that no scalar is ever wrapped, because prettier's default
# proseWrap ("preserve") leaves long lines alone but reformats PyYAML's
# wrapped continuation lines.
_NO_LINE_WRAP_WIDTH = 10**9


class PrettierFriendlyDumper(yaml.Dumper):
    """Dumper whose output matches the formatting of the prettier pre-commit
    hook, so files generated with it never produce a prettier diff. Block
    sequence items are indented under their parent key, and strings that cannot
    be emitted plain are quoted following prettier's quote choice."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        # PyYAML's emitter passes indentless by keyword, so renaming the
        # parameter would break the override. Ignore its value so block
        # sequence items are always indented.
        del indentless
        return super().increase_indent(flow, False)


def _represent_str_prettier(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    """Emits a string plain where possible, and quoted following prettier's
    quote choice where not: double quotes by default, single quotes when the
    string contains more double quotes than single quotes (whichever needs
    fewer escapes). Strings with characters that only exist as double-quoted
    escape sequences (newlines, control characters) stay double-quoted."""
    if not yaml.safe_dump(data).startswith(("'", '"')):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=None)
    prefers_single = data.isprintable() and data.count('"') > data.count("'")
    return dumper.represent_scalar(
        "tag:yaml.org,2002:str", data, style="'" if prefers_single else '"'
    )


PrettierFriendlyDumper.add_representer(str, _represent_str_prettier)


def prettier_friendly_yaml_dump(value: object) -> str:
    """Returns the value argument dumped as YAML with formatting that matches
    the prettier pre-commit hook (indented block sequences, prettier's quote
    choice, no line wrapping). Mapping keys keep their insertion order.
    """
    return yaml.dump(
        value,
        Dumper=PrettierFriendlyDumper,
        sort_keys=False,
        width=_NO_LINE_WRAP_WIDTH,
    )
