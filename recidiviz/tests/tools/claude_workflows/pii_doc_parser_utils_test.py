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
    extract_pii_doc_id,
    find_issue_section,
    parse_doc,
    section_has_content,
)


def _paragraph(text: str) -> dict:
    return {"paragraph": {"elements": [{"textRun": {"content": f"{text}\n"}}]}}


class TestExtractPiiDocId(unittest.TestCase):
    """Tests for extract_pii_doc_id — finding the per-ticket doc in a ticket body."""

    # The banner as it appears on the GitHub side of a synced Linear ticket.
    GITHUB_BANNER = (
        "> 🔒 **Private PII doc for this issue → [Open PII doc]"
        "(https://docs.google.com/document/d/16Oce011Zebihlmas8xMKmzj6FQEuDg2JtX6vceALeJU/edit)**"
        " · Put officer/client details and screenshots here, not in this issue.\n"
        "\n<!-- pii-doc-linked -->\n\n#### What is the issue?\n"
    )
    DOC_ID = "16Oce011Zebihlmas8xMKmzj6FQEuDg2JtX6vceALeJU"

    def test_github_banner(self) -> None:
        self.assertEqual(extract_pii_doc_id(self.GITHUB_BANNER), self.DOC_ID)

    def test_angle_bracketed_link(self) -> None:
        # Linear wraps URLs in angle brackets in its own description markdown.
        body = (
            "> 🔒 **Private PII doc for this issue → [Open PII doc]"
            f"(<https://docs.google.com/document/d/{self.DOC_ID}/edit>)** · Put "
            "officer/client details here."
        )
        self.assertEqual(extract_pii_doc_id(body), self.DOC_ID)

    def test_link_without_edit_suffix(self) -> None:
        self.assertEqual(
            extract_pii_doc_id(f"https://docs.google.com/document/d/{self.DOC_ID}"),
            self.DOC_ID,
        )

    def test_no_link_returns_none(self) -> None:
        # A pre-cutover ticket: no banner, so the caller falls back to the
        # shared go/github-pii doc.
        self.assertIsNone(
            extract_pii_doc_id("#### What is the issue?\n\nThe task never cleared.")
        )

    def test_first_link_wins(self) -> None:
        body = (
            f"https://docs.google.com/document/d/{self.DOC_ID}/edit\n"
            "https://docs.google.com/document/d/someOtherDocId/edit"
        )
        self.assertEqual(extract_pii_doc_id(body), self.DOC_ID)


class TestParseDoc(unittest.TestCase):
    """Tests for parse_doc — flattening Docs API JSON into lines."""

    def test_inline_images_become_placeholders(self) -> None:
        doc = {
            "body": {
                "content": [
                    {
                        "paragraph": {
                            "elements": [
                                {"textRun": {"content": "SID: "}},
                                {"inlineObjectElement": {"inlineObjectId": "kix.abc"}},
                                {"textRun": {"content": "\n"}},
                            ]
                        }
                    }
                ]
            }
        }
        self.assertEqual(parse_doc(doc), ["SID: [IMAGE]"])

    def test_table_cell_text_is_extracted(self) -> None:
        # A filled-in doc may lay its PII out in a table; ignoring tables would
        # make such a doc look empty.
        doc = {
            "body": {
                "content": [
                    _paragraph("Client / Resident (PII)"),
                    {
                        "table": {
                            "tableRows": [
                                {
                                    "tableCells": [
                                        {"content": [_paragraph("Name")]},
                                        {"content": [_paragraph("TDCJ")]},
                                    ]
                                },
                                {
                                    "tableCells": [
                                        {"content": [_paragraph("Lastname, First")]},
                                        {"content": [_paragraph("TEST-CLIENT-ID-1")]},
                                    ]
                                },
                            ]
                        }
                    },
                ]
            }
        }
        self.assertEqual(
            parse_doc(doc),
            [
                "Client / Resident (PII)",
                "Name",
                "TDCJ",
                "Lastname, First",
                "TEST-CLIENT-ID-1",
            ],
        )


class TestSectionHasContent(unittest.TestCase):
    """Tests for section_has_content — legacy sections that are header-only.

    TODO(OBT-44025): delete this class, TestTicketHeaderKey, TestFindIssueSection
    and TestTicketHeaderRegexAgreement with the shared-doc path they cover.
    """

    def test_header_only_section_has_no_content(self) -> None:
        self.assertFalse(section_has_content(["#88494"]))
        self.assertFalse(section_has_content(["#88494", "", "   "]))

    def test_section_with_body_has_content(self) -> None:
        self.assertTrue(section_has_content(["#88494", "SID: 12345"]))


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

    def test_stacked_identifiers_for_one_ticket_are_one_section(self) -> None:
        # The real go/github-pii doc stacks a ticket's GitHub number and Linear
        # ID on consecutive heading lines. Treating the second as the next
        # entry's header returned a header-only section and lost the PII below
        # it — the bug behind issue #95175's bogus diagnosis.
        lines = [
            "#95175",
            "OBT-42969",
            "User: test-officer",
            "Resident/Client: TEST-CLIENT-ID-1",
            "#95146",
            "Resident/Client: other ticket",
        ]
        self.assertEqual(
            find_issue_section(lines, ["95175", "OBT-42969"]),
            [
                "#95175",
                "OBT-42969",
                "User: test-officer",
                "Resident/Client: TEST-CLIENT-ID-1",
            ],
        )

    def test_another_tickets_header_still_ends_the_section(self) -> None:
        # Only identifiers belonging to THIS ticket are absorbed. An empty entry
        # followed by a different ticket must not swallow that ticket's PII —
        # returning another client's data is far worse than returning none.
        lines = [
            "#95175",
            "#95146",
            "Resident/Client: someone else entirely",
        ]
        self.assertEqual(find_issue_section(lines, ["95175", "OBT-42969"]), ["#95175"])


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
