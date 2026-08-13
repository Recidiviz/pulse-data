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
"""Parses a PII Google Doc's JSON (from the Docs API) and prints its PII text.

Two modes, matching the two doc shapes:

Per-ticket doc (current) — pass no arguments. Prints the doc's full text, with
[IMAGE] standing in for each pasted screenshot. Read the client IDs out of it
yourself; if there are none, stop and say so rather than investigating without
them:

    curl -s -H "Authorization: Bearer $TOKEN" <docs_api_url> | \
        python3 parse_github_pii_doc.py

Shared go/github-pii doc (legacy) — pass the GitHub issue number and/or the
Linear identifier (e.g. "OBT-36184"); the section is keyed by either, depending
on when the ticket was filed. TODO(OBT-44025): drop this mode, after which the
script takes no arguments at all:

    curl -s -H "Authorization: Bearer $TOKEN" <docs_api_url> | \
        python3 parse_github_pii_doc.py <GITHUB_ISSUE_NUMBER> [<LINEAR_ISSUE_ID>]
"""
import json
import sys

from recidiviz.tools.claude_workflows.pg_ticket_diagnosis.pii_doc_parser_utils import (
    find_issue_section,
    parse_doc,
    section_has_content,
)


def _print_shared_doc_section(lines: list[str], identifiers: list[str]) -> None:
    """Print the legacy shared doc's section for any of `identifiers`.

    TODO(OBT-44025): remove with the rest of the shared-doc path.
    """
    section = find_issue_section(lines, identifiers)
    if not section:
        print(f"Could not find any of {identifiers} in the document")
        return
    if not section_has_content(section):
        print(
            f"WARNING: the entry for {identifiers} is just a header with no PII "
            "under it.",
            file=sys.stderr,
        )
    print("\n".join(section))


def main() -> None:
    identifiers = sys.argv[1:]
    doc = json.load(sys.stdin)

    if "error" in doc:
        code = doc["error"].get("code", "unknown")
        message = doc["error"].get("message", "unknown error")
        print(
            f"ERROR: Google Docs API returned {code}: {message}\n"
            "If this is an auth error, try running: gcloud auth login\n"
            "If this is a 403/404 on a per-ticket doc, you may not have access "
            "to the doc — check that you can open it in the browser.",
            file=sys.stderr,
        )
        sys.exit(1)

    lines = parse_doc(doc)

    if identifiers:
        _print_shared_doc_section(lines, identifiers)
    else:
        print("\n".join(lines))


if __name__ == "__main__":
    main()
