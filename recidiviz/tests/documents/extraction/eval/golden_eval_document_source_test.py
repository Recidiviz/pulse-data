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
"""Tests for GoldenEvalGoogleSheetDocumentSource."""
import re
from unittest import TestCase
from unittest.mock import create_autospec

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_document_source import (
    GoldenEvalDocumentSourceError,
    GoldenEvalGoogleSheetDocumentSource,
)
from recidiviz.utils.google_sheets_reader import (
    GoogleSheetReader,
    GoogleSheetTable,
    GoogleSheetTableRow,
)

_STATE_CODE = StateCode.US_XX
_SOURCE_SHEET_URI = "https://docs.google.com/spreadsheets/d/abc123#gid=7"

_COLUMN_NAMES = ["golden_document_id", "document_text"]


def _tab(
    *cell_dicts: dict[str, str], title: str = _STATE_CODE.value
) -> GoogleSheetTable:
    """Returns the tab titled |title| holding |cell_dicts| as its data rows,
    numbered from row 2 as the sheet's first data row.
    """
    return GoogleSheetTable(
        title=title,
        column_names=_COLUMN_NAMES,
        rows=[
            GoogleSheetTableRow(row_number=row_number, values_by_column=cells)
            for row_number, cells in enumerate(cell_dicts, start=2)
        ],
    )


def _row_cells() -> dict[str, str]:
    """Returns one row's cell text keyed by column."""
    return {
        "golden_document_id": "unit_1",
        "document_text": "The record is active.",
    }


class GoldenEvalGoogleSheetDocumentSourceTest(TestCase):
    """Tests reading an extractor's golden eval sheet."""

    def setUp(self) -> None:
        self.sheets_reader = create_autospec(GoogleSheetReader)
        self.source = GoldenEvalGoogleSheetDocumentSource(
            sheets_reader=self.sheets_reader,
            state_code=_STATE_CODE,
            source_sheet_uri=_SOURCE_SHEET_URI,
        )

    def test_read_table_reads_the_configured_sheet(self) -> None:
        tab = _tab(_row_cells())
        self.sheets_reader.read_tab_as_table.return_value = tab

        table = self.source.read_table()

        self.sheets_reader.read_tab_as_table.assert_called_once_with(
            sheet_uri=_SOURCE_SHEET_URI
        )
        self.assertEqual(tab, table)

    def test_read_table_with_no_rows_raises(self) -> None:
        self.sheets_reader.read_tab_as_table.return_value = _tab()

        with self.assertRaisesRegex(
            GoldenEvalDocumentSourceError,
            rf"^Golden eval sheet \[{re.escape(_SOURCE_SHEET_URI)}\] holds no rows, "
            r"so there is nothing to evaluate\.$",
        ):
            self.source.read_table()

    def test_read_table_with_tab_not_titled_for_the_state_raises(self) -> None:
        # The title must be the state code exactly: another state's tab, the same
        # code in another case, and the code inside a longer title all fail.
        for title in ["US_YY", "us_xx", "US_XX golden eval"]:
            with self.subTest(title=title):
                self.sheets_reader.read_tab_as_table.return_value = _tab(
                    _row_cells(), title=title
                )

                with self.assertRaisesRegex(
                    ValueError,
                    rf"^Golden eval sheet \[{re.escape(_SOURCE_SHEET_URI)}\] reads "
                    rf"tab \[{re.escape(title)}\], but it is the eval set for state "
                    r"\[US_XX\]\. A state's eval set lives in the tab named after "
                    r"its state code\.$",
                ):
                    self.source.read_table()

    def test_read_table_checks_the_tab_title_before_emptiness(self) -> None:
        # An empty tab titled for another state is the wrong tab, not an empty
        # eval set.
        self.sheets_reader.read_tab_as_table.return_value = _tab(title="US_YY")

        with self.assertRaisesRegex(
            ValueError, r"reads tab \[US_YY\], but it is the eval set for state"
        ):
            self.source.read_table()
