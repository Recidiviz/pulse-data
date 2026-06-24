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
"""Tests for the Marquis eOMIS parsing helpers."""
import unittest
from datetime import date, datetime

from recidiviz.tools.eomis.parsing import (
    clean_bool,
    clean_date,
    clean_optional,
    extract_selected_offender_id,
    input_values,
    listing_total,
    parse_listing_page_rows,
    parse_listing_table,
    to_eomis_date,
    wrap_comment,
)

HEADER_HTML = (
    '<code class="indicator-id">PID#:&nbsp;'
    '<span id="profile-pid">0379543</span></code>'
)

LISTING_HTML = """
<html><body>
<span>( 1 - 2 of 3 )</span>
<table>
  <thead><tr><th>Some Other Screen</th></tr></thead>
  <tbody><tr><td data-name="Unrelated">x</td></tr></tbody>
</table>
<table>
  <thead><tr><th>Date Referred</th><th>Status</th></tr></thead>
  <tbody>
    <tr data-detailuniqueid="row001">
      <td data-name="ProgramApplicationDate">01/02/2024</td>
      <td data-name="ProgramApplicationStatus">Referred, not Screened</td>
      <td><a href="/eomis/detail?ProgramReferralSeqNbr=001">view</a></td>
    </tr>
    <tr class="no-rows-found"><td>No rows found</td></tr>
  </tbody>
</table>
</body></html>
"""

DETAIL_HTML = """
<html><body><form>
  <input name="dateLastUpdate" value="01/02/2024" />
  <input name="timeLastUpdate" value="10:11:12" />
  <input name="dateLastUpdate" value="should-not-overwrite-first" />
  <input name="noValue" />
  <select name="ProgramReferralPriority">
    <option value="1">High</option>
    <option value="9" selected>Unassigned</option>
  </select>
  <select name="nothingSelected">
    <option value="1">One</option>
  </select>
</form></body></html>
"""


class TestEomisDates(unittest.TestCase):
    """Tests date conversion into the MM/DD/YYYY format eOMIS forms expect."""

    def test_converts_supported_inputs(self) -> None:
        for value in (
            datetime(2024, 1, 2, 3, 4),
            date(2024, 1, 2),
            "01/02/2024",
            "2024-01-02",
            " 2024-01-02 ",
        ):
            self.assertEqual(to_eomis_date(value), "01/02/2024")

    def test_rejects_missing_or_unparseable(self) -> None:
        for value in (None, ""):
            with self.assertRaisesRegex(ValueError, "date is required"):
                to_eomis_date(value)
        with self.assertRaisesRegex(ValueError, "unparseable date"):
            to_eomis_date("02-01-2024")

    def test_clean_date(self) -> None:
        self.assertIsNone(clean_date(None))
        self.assertIsNone(clean_date(""))
        self.assertEqual(clean_date("2024-01-02"), "01/02/2024")


class TestValueCleaners(unittest.TestCase):
    def test_clean_optional(self) -> None:
        self.assertIsNone(clean_optional(None))
        self.assertIsNone(clean_optional(""))
        self.assertEqual(clean_optional(" Completed "), "Completed")

    def test_clean_bool(self) -> None:
        for truthy in (True, "true", "T", "1", "YES", "y"):
            self.assertTrue(clean_bool(truthy))
        for falsy in (None, "", False, "false", "0", "no"):
            self.assertFalse(clean_bool(falsy))


class TestWrapComment(unittest.TestCase):
    def test_wraps_plain_text_in_paragraph(self) -> None:
        self.assertEqual(wrap_comment(" hello "), "<p>hello</p>")

    def test_preserves_existing_markup_and_empty(self) -> None:
        self.assertEqual(wrap_comment("<p>hi</p>"), "<p>hi</p>")
        self.assertEqual(wrap_comment("  "), "")


class TestExtractSelectedOffenderId(unittest.TestCase):
    def test_extracts_pid(self) -> None:
        self.assertEqual(extract_selected_offender_id(HEADER_HTML), "0379543")

    def test_empty_when_absent(self) -> None:
        self.assertEqual(extract_selected_offender_id("<html></html>"), "")


class TestListingParsing(unittest.TestCase):
    """Tests parsing of the listing (browse) screens."""

    def test_finds_table_by_required_headers(self) -> None:
        rows = parse_listing_table(LISTING_HTML, ["Date Referred", "Status"])
        self.assertEqual(
            rows,
            [
                {
                    "ProgramApplicationDate": "01/02/2024",
                    "ProgramApplicationStatus": "Referred, not Screened",
                    "_detailuniqueid": "row001",
                    "_detail_url": "/eomis/detail?ProgramReferralSeqNbr=001",
                }
            ],
        )

    def test_returns_empty_when_no_table_matches(self) -> None:
        self.assertEqual(parse_listing_table(LISTING_HTML, ["Nonexistent Header"]), [])

    def test_listing_total(self) -> None:
        self.assertEqual(listing_total(LISTING_HTML), 3)
        self.assertIsNone(listing_total("<html><body>no counter</body></html>"))

    def test_parse_listing_page_rows(self) -> None:
        page_json = {
            "results": [
                {
                    "detailuniqueid": "row002",
                    "clazz": "even",
                    "ProgramApplicationDate": "<td>02/03/2024</td>",
                    "ProgramApplicationStatus": (
                        '<a href="/eomis/detail?seq=002">Completed successfully</a>'
                    ),
                }
            ]
        }
        self.assertEqual(
            parse_listing_page_rows(page_json),
            [
                {
                    "ProgramApplicationDate": "02/03/2024",
                    "ProgramApplicationStatus": "Completed successfully",
                    "_detail_url": "/eomis/detail?seq=002",
                    "_detailuniqueid": "row002",
                }
            ],
        )


class TestInputValues(unittest.TestCase):
    def test_collects_first_input_and_selected_option(self) -> None:
        values = input_values(DETAIL_HTML)
        self.assertEqual(values["dateLastUpdate"], "01/02/2024")
        self.assertEqual(values["timeLastUpdate"], "10:11:12")
        self.assertEqual(values["noValue"], "")
        self.assertEqual(values["ProgramReferralPriority"], "9")
        self.assertEqual(values["nothingSelected"], "")


if __name__ == "__main__":
    unittest.main()
