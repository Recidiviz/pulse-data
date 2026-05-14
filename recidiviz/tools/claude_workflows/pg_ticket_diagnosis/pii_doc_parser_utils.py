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
(client names, state-issued external IDs) associated with GitHub issue tickets.
Each issue's section is delimited by a bare "#<number>" header line. These
utilities parse the Docs API JSON response and extract the section for a given
issue number.
"""
import re

# Matches a line that is a bare ticket header like "#12345" (optionally surrounded
# by whitespace). Used to locate a section start and to detect its boundary without
# matching inline references like "see #12345" or external IDs like "#02636448".
_TICKET_HEADER_RE = re.compile(r"^\s*#(\d+)\s*$")


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


def find_issue_section(lines: list[str], issue_number: str) -> list[str]:
    """Return the go/github-pii section for the given issue number.

    In the doc, each issue's PII entry starts with a bare "#<number>" header
    and runs until the next such header.
    """
    output: list[str] = []
    for i, line in enumerate(lines):
        match = _TICKET_HEADER_RE.match(line)
        if match and match.group(1) == issue_number:
            output.append(line)
            for j in range(i + 1, len(lines)):
                if _TICKET_HEADER_RE.match(lines[j]):
                    break
                output.append(lines[j])
            break
    return output
