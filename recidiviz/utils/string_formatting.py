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

from textwrap import dedent, indent


def fix_indent(s: str, *, indent_level: int) -> str:
    """Updates the input string to reset the indentation level to the provided
    |indent_level| while maintaining all relative indents between lines.

    Also strips leading and trailing whitespace.
    """
    return indent(dedent(s).strip(), prefix=" " * indent_level)


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
