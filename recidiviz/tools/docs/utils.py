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
"""Utils for documentation generation."""
import difflib
import logging
import os
import re
from collections.abc import Sequence
from enum import Enum

import recidiviz
from recidiviz.repo.issue_references import TO_DO_REGEX

DOCS_ROOT_PATH = os.path.normpath(
    os.path.join(os.path.dirname(recidiviz.__file__), "..", "docs")
)
PLACEHOLDER_TO_DO_STRING = "TO" + "DO"
ISSUES_URL = "https://github.com/Recidiviz/pulse-data/issues/"


def hyperlink_todos(text: str) -> str:
    """Given any text, returns that text with todos linked."""

    def _link_todo(match_obj: re.Match) -> str:
        """Adds a markdown link to the found todo."""
        og_text = match_obj.group(0)
        digits = og_text[len(f"{PLACEHOLDER_TO_DO_STRING}(#") : -1]
        return f"[{og_text}]({ISSUES_URL+digits})"

    return re.sub(TO_DO_REGEX, _link_todo, text)


_INTEGER_RE = re.compile(r"^-?\d+$")


def markdown_table(
    headers: list[str], value_matrix: Sequence[Sequence], margin: int = 0
) -> str:
    """Renders a markdown table string from headers and rows.

    This is a performant drop-in replacement for
    pytablewriter.MarkdownTableWriter().dumps(), which passes each cell through many
    layers of processing that we don't need. Headers are center-aligned,
    integer cells are right-aligned, and all other cells are left-aligned. Minimum
    column width is 3.
    """
    if not headers:
        return ""

    def _stringify_cell(c: object) -> str:
        if c is None:
            return ""
        # For (str, Enum) subclasses, str() in Python 3.11+ produces
        # "ClassName.MEMBER" rather than the value — use .value directly.
        if isinstance(c, Enum) and isinstance(c, str):
            text = str(c.value)
        else:
            text = str(c)
        # Strip one layer of surrounding double quotes to match pytablewriter.
        if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
            text = text[1:-1]
        return text.replace("\t", "  ").replace("\n", " ")

    num_cols = len(headers)
    # Pad or truncate each row to match the number of headers.
    str_rows = [
        [_stringify_cell(row[i]) if i < len(row) else "" for i in range(num_cols)]
        for row in value_matrix
    ]
    # Minimum column width of 3 matches pytablewriter behavior.
    col_widths = [max(3, len(headers[i])) for i in range(num_cols)]
    for row in str_rows:
        for i in range(num_cols):
            col_widths[i] = max(col_widths[i], len(row[i]))

    m = " " * margin

    def _center(text: str, width: int) -> str:
        """Center-align with extra padding on the right (matching pytablewriter)."""
        pad = width - len(text)
        left = pad // 2
        return " " * left + text + " " * (pad - left)

    header_line = (
        "|"
        + "|".join(m + _center(headers[i], col_widths[i]) + m for i in range(num_cols))
        + "|"
    )
    # A column is "all-integer" if every non-empty cell is an integer and at
    # least one cell is non-empty. These columns get a trailing colon in the
    # separator (e.g. "---:") to signal right-alignment per the GFM table spec,
    # and their data cells are right-justified.
    col_all_integer = []
    for col_idx in range(num_cols):
        all_int = all(
            _INTEGER_RE.match(row[col_idx]) or row[col_idx] == "" for row in str_rows
        ) and any(_INTEGER_RE.match(row[col_idx]) for row in str_rows)
        col_all_integer.append(all_int)

    # Build the separator line between headers and data. Integer columns end
    # with ":" for right-alignment; others are all dashes.
    sep_parts = []
    for i in range(num_cols):
        if col_all_integer[i]:
            sep_parts.append(m + "-" * (col_widths[i] - 1) + ":" + m)
        else:
            sep_parts.append(m + "-" * col_widths[i] + m)
    sep_line = "|" + "|".join(sep_parts) + "|"

    data_lines = []
    for row in str_rows:
        cells = []
        for i in range(num_cols):
            if _INTEGER_RE.match(row[i]):
                cells.append(m + row[i].rjust(col_widths[i]) + m)
            else:
                cells.append(m + row[i].ljust(col_widths[i]) + m)
        data_lines.append("|" + "|".join(cells) + "|")

    lines = [header_line, sep_line] + data_lines
    return "\n".join(lines) + "\n"


def persist_file_contents(
    documentation: str, markdown_path: str, log_diff: bool = False
) -> bool:
    """Persists contents to the provided path. Returns whether contents at that path
    changed."""
    prior_documentation = None
    if os.path.exists(markdown_path):
        with open(markdown_path, "r", encoding="utf-8") as md_file:
            prior_documentation = md_file.read()

    if prior_documentation != documentation:
        if log_diff and prior_documentation:
            logging.info(
                "\n".join(
                    difflib.ndiff(
                        prior_documentation.split("\n"),
                        documentation.split("\n"),
                    )
                )
            )
        path_dir, _ = os.path.split(markdown_path)
        os.makedirs(path_dir, exist_ok=True)
        with open(markdown_path, "w", encoding="utf-8") as md_file:
            md_file.write(documentation)
            return True
    return False
