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
"""Parses the go/github-pii Google Doc JSON (from the Docs API) and extracts
the PII section for a given GitHub issue number.

Usage:
    curl -s -H "Authorization: Bearer $TOKEN" <docs_api_url> | \
        python3 parse_github_pii_doc.py <ISSUE_NUMBER>
"""
import json
import sys


def parse_doc(doc: dict) -> list[str]:
    """Parse Google Doc JSON into a list of text lines with [IMAGE] placeholders."""
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
    """Find and return the section of lines related to the given issue number."""
    output: list[str] = []
    for i, line in enumerate(lines):
        if issue_number in line:
            for j in range(i, min(i + 30, len(lines))):
                output.append(lines[j])
                if (
                    j > i
                    and any(c.isdigit() for c in lines[j])
                    and (
                        "recidiviz-dashboards" in lines[j]
                        or "pulse-data" in lines[j]
                    )
                ):
                    break
            break
    return output


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: parse_github_pii_doc.py <ISSUE_NUMBER>", file=sys.stderr)
        sys.exit(1)

    issue_number = sys.argv[1]
    doc = json.load(sys.stdin)
    lines = parse_doc(doc)
    output = find_issue_section(lines, issue_number)

    if output:
        print("\n".join(output))
    else:
        print(f"Could not find {issue_number} in the document")


if __name__ == "__main__":
    main()
