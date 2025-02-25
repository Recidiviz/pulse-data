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
