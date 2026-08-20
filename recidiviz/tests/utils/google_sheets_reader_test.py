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
"""Tests for GoogleSheetReader, run against a fake Sheets API service."""
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock

import httplib2
from googleapiclient.errors import HttpError

from recidiviz.utils.google_sheets_reader import (
    ADC_SETUP_COMMAND,
    GoogleSheetReader,
    GoogleSheetTable,
    GoogleSheetTableRow,
    GoogleSheetValueRenderOption,
    parse_sheet_uri,
)

_SHEET_URI = "https://docs.google.com/spreadsheets/d/abc123"
_SPREADSHEET_ID = "abc123"


def _sheets_service(
    *, tabs: list[tuple[int, str]], grid: list[list[Any]] | None
) -> MagicMock:
    """Returns a fake Sheets API service whose spreadsheet has |tabs| (as (gid,
    title) pairs) and whose value read returns |grid| — or no `values` key at all
    when |grid| is None, as the API does for a tab with no cells.
    """
    service = MagicMock()
    service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"sheetId": gid, "title": title}} for gid, title in tabs
        ]
    }
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = (
        {} if grid is None else {"values": grid}
    )
    return service


def _failing_sheets_service(*, status: int) -> MagicMock:
    """Returns a fake Sheets API service whose spreadsheet read fails with an
    HTTP |status|.
    """
    service = MagicMock()
    service.spreadsheets.return_value.get.return_value.execute.side_effect = HttpError(
        resp=httplib2.Response({"status": str(status)}),
        content=b'{"error": {"message": "denied"}}',
    )
    return service


class ParseSheetUriTest(TestCase):
    """Tests parsing a Google Sheets URI into its spreadsheet id and pinned gid."""

    def test_fragment_gid(self) -> None:
        self.assertEqual(
            ("1XVxpTA6ymYctzpB2G24V3KAcl8eozihFlA3OjfyjSU4", 1039239798),
            parse_sheet_uri(
                "https://docs.google.com/spreadsheets/d/"
                "1XVxpTA6ymYctzpB2G24V3KAcl8eozihFlA3OjfyjSU4/edit#gid=1039239798"
            ),
        )

    def test_query_gid(self) -> None:
        self.assertEqual(
            ("abc123", 42),
            parse_sheet_uri(
                "https://docs.google.com/spreadsheets/d/abc123/edit?gid=42#gid=42"
            ),
        )

    def test_no_gid(self) -> None:
        self.assertEqual((_SPREADSHEET_ID, None), parse_sheet_uri(_SHEET_URI))

    def test_not_a_sheets_uri(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Sheet URI \[https://example\.com/notasheet\] is not a Google Sheets "
            r"URI: it holds no `/spreadsheets/d/<spreadsheet id>` path\.$",
        ):
            parse_sheet_uri("https://example.com/notasheet")


class GoogleSheetReaderTest(TestCase):
    """Tests reading one tab of a Google Sheet into a header and data rows."""

    def test_reads_tab_pinned_by_gid(self) -> None:
        service = _sheets_service(
            tabs=[(0, "First"), (7, "O'Brien's tab")],
            grid=[["test_type", "test_case"], ["unit", "base_case"]],
        )

        tab = GoogleSheetReader(sheets_service=service).read_tab_as_table(
            sheet_uri=f"{_SHEET_URI}/edit#gid=7"
        )

        self.assertEqual(
            GoogleSheetTable(
                title="O'Brien's tab",
                column_names=["test_type", "test_case"],
                rows=[
                    GoogleSheetTableRow(
                        row_number=2,
                        values_by_column={
                            "test_type": "unit",
                            "test_case": "base_case",
                        },
                    )
                ],
            ),
            tab,
        )
        service.spreadsheets.return_value.values.return_value.get.assert_called_once_with(
            spreadsheetId=_SPREADSHEET_ID,
            range="'O''Brien''s tab'",
            majorDimension="ROWS",
            valueRenderOption="FORMATTED_VALUE",
        )

    def test_pinned_gid_absent_from_spreadsheet_raises(self) -> None:
        service = _sheets_service(tabs=[(0, "First"), (7, "Second")], grid=[[]])

        with self.assertRaisesRegex(
            ValueError,
            r"^Google Sheet \[.*#gid=99\] pins tab gid \[99\], which the "
            r"spreadsheet does not have\. Its tabs are: "
            r"\[\(0, 'First'\), \(7, 'Second'\)\]\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=f"{_SHEET_URI}/edit#gid=99"
            )

    def test_no_pinned_gid_with_multiple_tabs_raises(self) -> None:
        service = _sheets_service(tabs=[(0, "First"), (7, "Second")], grid=[[]])

        with self.assertRaisesRegex(
            ValueError,
            r"^Google Sheet URI \[.*\] pins no tab, but the spreadsheet has "
            r"\[2\] tabs: \[\(0, 'First'\), \(7, 'Second'\)\]\. Pin the tab to read "
            r"by appending `#gid=<tab gid>` to the URI\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_no_pinned_gid_with_one_tab_reads_it(self) -> None:
        service = _sheets_service(
            tabs=[(0, "Only tab")],
            grid=[["test_type"], ["unit"]],
        )

        tab = GoogleSheetReader(sheets_service=service).read_tab_as_table(
            sheet_uri=_SHEET_URI
        )

        self.assertEqual(
            GoogleSheetTable(
                title="Only tab",
                column_names=["test_type"],
                rows=[
                    GoogleSheetTableRow(
                        row_number=2, values_by_column={"test_type": "unit"}
                    )
                ],
            ),
            tab,
        )
        service.spreadsheets.return_value.values.return_value.get.assert_called_once_with(
            spreadsheetId=_SPREADSHEET_ID,
            range="'Only tab'",
            majorDimension="ROWS",
            valueRenderOption="FORMATTED_VALUE",
        )

    def test_unformatted_value_render_option_renders_cells_as_text(self) -> None:
        service = _sheets_service(
            tabs=[(0, "Only tab")],
            grid=[["amount", "flag"], [1000.5, True]],
        )

        tab = GoogleSheetReader(sheets_service=service).read_tab_as_table(
            sheet_uri=_SHEET_URI,
            value_render_option=GoogleSheetValueRenderOption.UNFORMATTED_VALUE,
        )

        self.assertEqual(
            [
                GoogleSheetTableRow(
                    row_number=2,
                    values_by_column={"amount": "1000.5", "flag": "True"},
                )
            ],
            tab.rows,
        )
        service.spreadsheets.return_value.values.return_value.get.assert_called_once_with(
            spreadsheetId=_SPREADSHEET_ID,
            range="'Only tab'",
            majorDimension="ROWS",
            valueRenderOption="UNFORMATTED_VALUE",
        )

    def test_short_rows_padded_to_header_width(self) -> None:
        service = _sheets_service(
            tabs=[(0, "Only tab")],
            grid=[["test_type", "test_case", "notes"], ["unit", "base_case"], ["unit"]],
        )

        tab = GoogleSheetReader(sheets_service=service).read_tab_as_table(
            sheet_uri=_SHEET_URI
        )

        self.assertEqual(
            [
                GoogleSheetTableRow(
                    row_number=2,
                    values_by_column={
                        "test_type": "unit",
                        "test_case": "base_case",
                        "notes": "",
                    },
                ),
                GoogleSheetTableRow(
                    row_number=3,
                    values_by_column={
                        "test_type": "unit",
                        "test_case": "",
                        "notes": "",
                    },
                ),
            ],
            tab.rows,
        )

    def test_shifted_row_raises(self) -> None:
        service = _sheets_service(
            tabs=[(0, "Only tab")],
            grid=[["test_type", "test_case"], ["unit", "base_case", "stray value"]],
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Row \[2\] of Google Sheet \[.*\] holds value\(s\) "
            r"\['stray value'\] beyond its last header column \[test_case\], so its "
            r"values are shifted out of alignment with the header\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_duplicate_header_cell_raises(self) -> None:
        service = _sheets_service(
            tabs=[(0, "Only tab")],
            grid=[["test_type", "test_case", "test_type"], ["unit", "base_case", "x"]],
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Google Sheet \[.*\] has duplicate header cell\(s\): "
            r"\['test_type'\]\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_blank_header_cell_raises(self) -> None:
        service = _sheets_service(
            tabs=[(0, "Only tab")],
            grid=[["test_type", "  ", "test_case"], ["unit", "x", "base_case"]],
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Google Sheet \[.*\] has a blank header cell in column\(s\) \[2\] "
            r"\(1-based\)\. Every column a data row reaches must be named\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_blank_interior_row_skipped(self) -> None:
        service = _sheets_service(
            tabs=[(0, "Only tab")],
            grid=[["test_type"], ["unit"], ["   "], [], ["sample"]],
        )

        with self.assertLogs(level="WARNING") as logs:
            tab = GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

        self.assertEqual(
            [
                GoogleSheetTableRow(
                    row_number=2, values_by_column={"test_type": "unit"}
                ),
                GoogleSheetTableRow(
                    row_number=5, values_by_column={"test_type": "sample"}
                ),
            ],
            tab.rows,
        )
        self.assertEqual(
            2, len([log for log in logs.output if "Skipping blank row" in log])
        )

    def test_empty_grid_raises(self) -> None:
        service = _sheets_service(tabs=[(0, "Only tab")], grid=None)

        with self.assertRaisesRegex(
            ValueError,
            r"^Google Sheet \[.*\] is empty, so it has no header row naming "
            r"its columns\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_empty_header_row_raises(self) -> None:
        service = _sheets_service(tabs=[(0, "Only tab")], grid=[[]])

        with self.assertRaisesRegex(
            ValueError,
            r"^Google Sheet \[.*\] has an empty header row, so it names no "
            r"columns\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_permission_denied_explains_how_to_get_access(self) -> None:
        service = _failing_sheets_service(status=403)

        with self.assertRaisesRegex(
            ValueError,
            r"^Could not read Google Sheet \[.*\] \(HTTP \[403\]\)\. Grant your "
            rf"local credentials access with `{ADC_SETUP_COMMAND}`, or share the "
            r"spreadsheet with the CI service account\.$",
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_not_found_explains_how_to_get_access(self) -> None:
        service = _failing_sheets_service(status=404)

        with self.assertRaisesRegex(
            ValueError, r"^Could not read Google Sheet \[.*\] \(HTTP \[404\]\)\."
        ):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )

    def test_other_http_error_propagates(self) -> None:
        service = _failing_sheets_service(status=500)

        with self.assertRaises(HttpError):
            GoogleSheetReader(sheets_service=service).read_tab_as_table(
                sheet_uri=_SHEET_URI
            )
