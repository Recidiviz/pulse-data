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
"""Tests for pg_ticket_diagnosis/pii_doc_parser_utils.py."""
import re
import unittest

from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.tools.claude_workflows.pg_ticket_diagnosis.pii_doc_parser_utils import (
    _ticket_header_key,
    find_issue_section,
)


class TestTicketHeaderKey(unittest.TestCase):
    """Tests for _ticket_header_key — which lines count as section headers."""

    def test_github_number_requires_hash(self) -> None:
        self.assertEqual(_ticket_header_key("#88494"), "88494")
        self.assertEqual(_ticket_header_key("# 88494"), "88494")
        # A bare number is an external ID / date / count in a section body, not
        # a header — matching it would silently truncate the enclosing section.
        self.assertIsNone(_ticket_header_key("88494"))

    def test_linear_id_hash_optional_and_space_allowed(self) -> None:
        self.assertEqual(_ticket_header_key("OBT-36184"), "OBT-36184")
        self.assertEqual(_ticket_header_key("#OBT-36184"), "OBT-36184")
        # The hash-space form the doc actually uses (previously missed).
        self.assertEqual(_ticket_header_key("# OBT-18522"), "OBT-18522")

    def test_combined_header_uses_leading_key(self) -> None:
        self.assertEqual(_ticket_header_key("#OBT-36212 and #88494"), "OBT-36212")

    def test_body_lines_are_not_headers(self) -> None:
        # Regression cases from an audit of the real go/github-pii doc: none of
        # these bare-number / date / count lines may be treated as a header.
        for line in [
            "2025-03-25 (in window)",
            "11/03/2022",
            "375054",
            "375054 (shows up in ACIS)",
            "156679 Lastname, Firstname",
            "2-3 clients who aren't being reflected",
            "see #12345 for related context",
        ]:
            with self.subTest(line=line):
                self.assertIsNone(_ticket_header_key(line))


class TestFindIssueSection(unittest.TestCase):
    """Tests for find_issue_section — header matching in the go/github-pii doc."""

    def test_match_by_github_number(self) -> None:
        lines = [
            "#88494",
            "Client: [name]",
            "SID: 12345",
            "#OBT-36212",
            "Client: [other]",
        ]
        self.assertEqual(
            find_issue_section(lines, ["88494"]),
            ["#88494", "Client: [name]", "SID: 12345"],
        )

    def test_match_by_linear_id(self) -> None:
        lines = [
            "#88494",
            "Client: [name]",
            "OBT-36212",
            "Client: [other]",
            "TDCJ: 67890",
        ]
        self.assertEqual(
            find_issue_section(lines, ["OBT-36212"]),
            ["OBT-36212", "Client: [other]", "TDCJ: 67890"],
        )

    def test_match_combined_header_by_leading_identifier(self) -> None:
        # A header may carry trailing identifiers/text; only its leading key
        # ("OBT-36212") is matchable. main() passes both the GitHub number and
        # the Linear ID, so the leading Linear ID resolves the section.
        lines = [
            "#OBT-36212 and #88494",
            "Client: [name]",
            "SID: 12345",
        ]
        self.assertEqual(
            find_issue_section(lines, ["88494", "OBT-36212"]),
            ["#OBT-36212 and #88494", "Client: [name]", "SID: 12345"],
        )

    def test_body_with_bare_numbers_is_not_truncated(self) -> None:
        # A section body full of bare external IDs, dates, and counts must be
        # returned intact — none of those lines end the section early.
        lines = [
            "#88494",
            "Client: [name]",
            "375054",
            "375054 (shows up in ACIS)",
            "2025-03-25 (in window)",
            "2-3 clients affected",
            "#OBT-36212",
            "Client: [other]",
        ]
        self.assertEqual(
            find_issue_section(lines, ["88494"]),
            [
                "#88494",
                "Client: [name]",
                "375054",
                "375054 (shows up in ACIS)",
                "2025-03-25 (in window)",
                "2-3 clients affected",
            ],
        )

    def test_hash_space_linear_header_is_found(self) -> None:
        lines = [
            "# OBT-18522",
            "Client: [name]",
            "SID: 12345",
        ]
        self.assertEqual(
            find_issue_section(lines, ["OBT-18522"]),
            ["# OBT-18522", "Client: [name]", "SID: 12345"],
        )

    def test_inline_reference_is_not_a_header(self) -> None:
        lines = [
            "#OBT-36212",
            "Client: [name]",
            "see #12345 for related context",
        ]
        self.assertEqual(find_issue_section(lines, ["12345"]), [])

    def test_no_match_returns_empty(self) -> None:
        lines = ["#88494", "Client: [name]"]
        self.assertEqual(find_issue_section(lines, ["99999"]), [])


class TestTicketHeaderRegexAgreement(unittest.TestCase):
    """Guards work item D: the Linear-ID grammar embedded in the header regex
    must stay in agreement with the source-of-truth LinearIssue.issue_regex()."""

    def test_every_linear_identifier_is_accepted_as_header_key(self) -> None:
        linear_full_re = re.compile(LinearIssue.issue_regex())
        for identifier in ["OBT-1", "OBT-36212", "ABC-12345", "X-9", "STATE-100"]:
            with self.subTest(identifier=identifier):
                # Sanity check: LinearIssue itself considers this a valid ID.
                self.assertIsNotNone(linear_full_re.fullmatch(identifier))
                # The header regex accepts it — bare, hashed, and hash-space.
                self.assertEqual(_ticket_header_key(identifier), identifier)
                self.assertEqual(_ticket_header_key(f"#{identifier}"), identifier)
                self.assertEqual(_ticket_header_key(f"# {identifier}"), identifier)
