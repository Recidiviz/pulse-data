# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Utilities for parsing the go/github-pii Google Doc.

The go/github-pii doc is a shared Google Doc where PG team members log PII
(client names, state-issued external IDs) associated with bug tickets. Each
ticket's section is delimited by a header line that begins with a ticket
identifier: a GitHub issue number ("#12345", hash required) or a Linear ID
("OBT-36184", hash optional). A header may list more than one — e.g.
"#OBT-36212 and #88494". These utilities parse the Docs API JSON response and
extract the section for a given GitHub issue number and/or Linear ID.
"""
import re

# A section header line, keyed by a GitHub issue number or a Linear ID:
#   "#12345" / "# 12345"          -> GitHub issue number. The leading "#" is
#                                    REQUIRED: bare numbers on their own line in
#                                    a section body (external IDs, dates, counts)
#                                    are not headers, and matching them would
#                                    silently truncate the section.
#   "OBT-36184" / "#OBT-36184"    -> Linear ID. The "#" is optional and a space
#     / "# OBT-36184"                may follow it (the doc uses all three).
# The trailing "(?=\s|$)" requires the identifier to be a whole token, and
# anchoring to the start lets a header carry trailing identifiers/text — e.g.
# "#OBT-36212 and #88494" (only the leading key is used) — while rejecting
# inline references like "see #12345". These shapes were validated against the
# real go/github-pii doc.
#
# The Linear-ID grammar ("[A-Z]+-\d+") is the source-of-truth grammar
# LinearIssue.issue_regex() (recidiviz/issue_tracking/linear/linear_issue.py).
# It is duplicated (not imported) because this module ships standalone in the
# Cloud Build container and is mirrored by the dependency-free skill script, so
# it must not import recidiviz. A test asserts the two stay in agreement.
_TICKET_HEADER_RE = re.compile(
    r"^\s*(?:"
    r"#\s*(?P<linear_hash>[A-Z]+-\d+)"  # "#OBT-…" or "# OBT-…"
    r"|(?P<linear_bare>[A-Z]+-\d+)"  # bare "OBT-…" (unambiguous)
    r"|#\s*(?P<github>\d+)"  # "#<digits>" or "# <digits>"
    r")(?=\s|$)"
)


def _ticket_header_key(line: str) -> str | None:
    """Return the normalized ticket key (GitHub issue number or Linear ID) if
    `line` is a section header, otherwise None."""
    match = _TICKET_HEADER_RE.match(line)
    if match is None:
        return None
    # Exactly one alternative matches, so exactly one group is non-None.
    return next(group for group in match.groups() if group is not None)


def parse_doc(doc: dict) -> list[str]:
    """Parse go/github-pii Docs API JSON into text lines with [IMAGE] placeholders."""
    content = doc.get("body", {}).get("content", [])
    lines: list[str] = []
    current_line = ""
    for elem in content:
        if "paragraph" in elem:
            for run in elem["paragraph"].get("elements", []):
                if "textRun" in run:
                    current_line += run["textRun"]["content"]
                elif "inlineObjectElement" in run:
                    current_line += "[IMAGE]"
            parts = current_line.split("\n")
            for p in parts[:-1]:
                lines.append(p)
            current_line = parts[-1]
    if current_line:
        lines.append(current_line)
    return lines


def find_issue_section(lines: list[str], identifiers: list[str]) -> list[str]:
    """Return the go/github-pii section for any of the given ticket identifiers.

    `identifiers` may contain a GitHub issue number (e.g. "12345") and/or a
    Linear identifier (e.g. "OBT-36184"). In the doc, each ticket's PII entry
    starts with a header line (either flavor) and runs until the next such
    header. Returns the first matching section, or an empty list if none match.
    """
    wanted = {i for i in identifiers if i and i.strip()}
    output: list[str] = []
    for i, line in enumerate(lines):
        if _ticket_header_key(line) in wanted:
            output.append(line)
            for j in range(i + 1, len(lines)):
                if _ticket_header_key(lines[j]) is not None:
                    break
                output.append(lines[j])
            break
    return output
