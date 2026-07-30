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
"""Helpers related to formatting string text"""

import re
from textwrap import dedent, indent
from typing import Iterable


def fix_indent(s: str, *, indent_level: int) -> str:
    """Updates the input string to reset the indentation level to the provided
    |indent_level| while maintaining all relative indents between lines.

    Also strips leading and trailing whitespace.
    """
    return indent(dedent(s).strip(), prefix=" " * indent_level)


SNAKE_CASE_REGEX = re.compile(r"^[a-z][a-z0-9_]*$")


def is_snake_case(s: str) -> bool:
    """Returns whether |s| is a non-empty snake_case string: a lowercase letter
    followed by any number of lowercase letters, digits, and underscores.
    """
    return bool(SNAKE_CASE_REGEX.fullmatch(s))


def collapse_whitespace(s: str) -> str:
    """Returns |s| with every run of whitespace (including newlines) collapsed to
    a single space, and leading/trailing whitespace stripped.
    """
    return " ".join(s.split())


def collapse_blank_lines(s: str) -> str:
    """Returns |s| with every run of two or more blank lines collapsed to a single
    blank line, and leading/trailing whitespace stripped. Useful for tidying a
    template after an optional section is filled in empty.
    """
    return re.sub(r"\n{3,}", "\n\n", s).strip()


def render_list(
    strings: Iterable[str], *, indent_level: int = 0, bullet_char: str = "-"
) -> str:
    """Returns |strings| rendered as a bulleted list, re-indented to
    |indent_level|. Each item's first line is prefixed with "|bullet_char| ", and
    a multi-line item's remaining lines are hanging-indented to align beneath that
    first line (preserving any relative indentation within the item).

    Returns an empty string when |strings| is empty.
    """
    hanging_indent = " " * (len(bullet_char) + 1)
    rendered_items: list[str] = []
    for s in strings:
        first_line, *continuation_lines = s.split("\n")
        item_lines = [f"{bullet_char} {first_line}"]
        item_lines.extend(
            f"{hanging_indent}{line}" if line.strip() else ""
            for line in continuation_lines
        )
        rendered_items.append("\n".join(item_lines))
    return fix_indent("\n".join(rendered_items), indent_level=indent_level)


def truncate_string_if_necessary(
    s: str, *, max_length: int, truncation_message: str | None = None
) -> str:
    """Truncates the provided string |s| so that it is no longer than the provided
    |max_length|. A message indicating that the string was truncated will be added to
    the end of the string (included in the max length).
    """
    if len(s) <= max_length:
        return s

    if truncation_message is None:
        truncation_message = " ... (truncated)"

    if max_length < len(truncation_message):
        raise ValueError(
            "Cannot specify a max length that is less than the truncation message "
            "itself."
        )
    return s[: max_length - len(truncation_message)] + truncation_message
