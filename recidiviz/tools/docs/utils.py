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
from recidiviz.github.github_constants import RECIDIVIZ_DATA_REPO
from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.issue import UrlIssue
from recidiviz.issue_tracking.issue_parsing import issue_from_todo
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue

DOCS_ROOT_PATH = os.path.normpath(
    os.path.join(os.path.dirname(recidiviz.__file__), "..", "docs")
)

# Assembled from fragments so this placeholder marker is not itself a literal
# to-do reference that the to-do-format lint would flag. Generated config
# skeletons emit it where a real issue reference still needs to be filled in.
PLACEHOLDER_TO_DO_STRING = "TO" + "DO"

_ISSUE_REF_REGEX = re.compile(
    "|".join(
        [
            GithubIssue.todo_regex(),
            LinearIssue.todo_regex(),
            UrlIssue.todo_regex(),
        ]
    )
)


def hyperlink_todos(text: str) -> str:
    """Given any text, returns that text with GitHub, Linear, and URL issue
    references linked."""

    def _link_todo(match_obj: re.Match) -> str:
        """Adds a markdown link to the found issue reference."""
        todo = match_obj.group(0)
        issue = issue_from_todo(todo, default_repo=RECIDIVIZ_DATA_REPO)
        return f"[{todo}]({issue.url})"

    return re.sub(_ISSUE_REF_REGEX, _link_todo, text)


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
