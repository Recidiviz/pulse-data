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
"""Reader that reads one tab of a Google Sheet as a table — a header row naming
the columns and one record per data row — addressed by the URI a browser copies.
"""
import logging
import re
from enum import Enum
from typing import Any

import attr
import google.auth
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from recidiviz.common import attr_validators
from recidiviz.utils.google_drive_services import get_sheets_service

ADC_SETUP_COMMAND = "gcloud auth login --enable-gdrive-access --update-adc"
"""The command that grants a local developer's application default credentials
access to Google Sheets. Quoted in the error raised when a sheet read is denied.
"""

# The read-only Sheets scope used to read a sheet.
_SHEETS_READONLY_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"

# The spreadsheet id within a Google Sheets URI. Bounded away from `/`, `?` and
# `#` so the id does not swallow the trailing `edit#gid=...` a browser copies.
_SPREADSHEET_ID_REGEX = re.compile(r"/spreadsheets/d/(?P<spreadsheet_id>[^/?#]+)")

# The tab a sheet URI pins, rendered as a fragment (`#gid=`, as older links and
# hand-written links carry it) or as a query parameter (`?gid=`/`&gid=`, as the
# Sheets UI writes it today).
_SHEET_GID_REGEX = re.compile(r"[#?&]gid=(?P<gid>\d+)")

# Read the grid a row at a time, so each returned list is one sheet row.
_SHEET_MAJOR_DIMENSION = "ROWS"

# HTTP statuses indicating that the caller cannot see the sheet.
_SHEET_ACCESS_ERROR_STATUS_CODES = frozenset({403, 404})


class GoogleSheetValueRenderOption(Enum):
    """How the Sheets API renders each cell in a read. Cells always reach the
    caller as text: a value the API returns as a number or boolean is rendered
    with str().
    """

    FORMATTED_VALUE = "FORMATTED_VALUE"
    """Cell text exactly as the sheet displays it, number and date formatting
    included."""

    UNFORMATTED_VALUE = "UNFORMATTED_VALUE"
    """The cell's underlying value with display formatting stripped. A
    date-formatted cell renders as its serial number."""

    FORMULA = "FORMULA"
    """The cell's formula when it has one, its unformatted value otherwise."""


def parse_sheet_uri(sheet_uri: str) -> tuple[str, int | None]:
    """Returns the spreadsheet id and tab gid named in |sheet_uri|. The gid is
    None if the URI does not name a tab.
    """
    spreadsheet_id_match = _SPREADSHEET_ID_REGEX.search(sheet_uri)
    if spreadsheet_id_match is None:
        raise ValueError(
            f"Sheet URI [{sheet_uri}] is not a Google Sheets URI: it holds no "
            f"`/spreadsheets/d/<spreadsheet id>` path."
        )
    gid_match = _SHEET_GID_REGEX.search(sheet_uri)
    return (
        spreadsheet_id_match.group("spreadsheet_id"),
        int(gid_match.group("gid")) if gid_match is not None else None,
    )


@attr.define(frozen=True, kw_only=True)
class GoogleSheetTableRow:
    """One record of a Google Sheet tab read as a table, holding raw cell text
    keyed by column name.
    """

    row_number: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this row in the tab. Row 1 is the header, so data
    rows start at 2."""

    values_by_column: dict[str, str] = attr.ib(
        validator=attr_validators.is_dict_of(str, str)
    )
    """Cell text for every column named in the header, as the sheet displays it.
    A column the row does not reach holds an empty string."""


@attr.define(frozen=True, kw_only=True)
class GoogleSheetTable:
    """One tab of a Google Sheet read as a table. Row 1 names the columns and
    every later non-blank row is one record.

    A tab holding:

        | test_type | test_case |
        | unit      | base_case |
        |           |           |
        | sample    |           |

    reads as column_names ['test_type', 'test_case'] and rows:

        row 2: {'test_type': 'unit',   'test_case': 'base_case'}
        row 4: {'test_type': 'sample', 'test_case': ''}

    Row 3 is blank, so it is skipped, and row numbers stay as the sheet shows
    them — which is why the second record is row 4 and not row 3.
    """

    title: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The tab's title, as the sheet displays it."""

    column_names: list[str] = attr.ib(
        validator=[attr_validators.is_non_empty_list, attr_validators.is_list_of(str)]
    )
    """Header cell text, in column order."""

    rows: list[GoogleSheetTableRow] = attr.ib(
        validator=attr_validators.is_list_of(GoogleSheetTableRow)
    )
    """The tab's non-blank data rows, in sheet order."""


@attr.define(frozen=True, kw_only=True)
class GoogleSheetReader:
    """Reader that reads one tab of a Google Sheet as a table — a header row
    naming the columns and one record per data row — addressed by the URI a
    browser copies.
    """

    sheets_service: Resource = attr.ib()
    """The Sheets API service every read goes through."""

    @classmethod
    def from_application_default_credentials(cls) -> "GoogleSheetReader":
        """Returns a reader backed by application default credentials.

        The scopes argument only takes effect for service account credentials. A
        developer's gcloud ADC has fixed scopes, granted at login by
        ADC_SETUP_COMMAND.
        """
        credentials, _project_id = google.auth.default(scopes=[_SHEETS_READONLY_SCOPE])
        return cls(sheets_service=get_sheets_service(credentials))

    def read_tab_as_table(
        self,
        *,
        sheet_uri: str,
        value_render_option: GoogleSheetValueRenderOption = (
            GoogleSheetValueRenderOption.FORMATTED_VALUE
        ),
    ) -> GoogleSheetTable:
        """Returns the tab of the Google Sheet at |sheet_uri| read as a table.
        The URI must pin the tab with a gid unless the spreadsheet has exactly
        one. Cells are rendered per |value_render_option|.
        """
        spreadsheet_id, gid = parse_sheet_uri(sheet_uri)
        tab_title = self._tab_title(
            spreadsheet_id=spreadsheet_id, sheet_uri=sheet_uri, gid=gid
        )

        response = self._execute_request(
            request=self.sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=self._quoted_tab_range(tab_title),
                majorDimension=_SHEET_MAJOR_DIMENSION,
                valueRenderOption=value_render_option.value,
            ),
            sheet_uri=sheet_uri,
        )
        # The API omits the values key for a tab with no cells.
        return self._parse_grid(
            sheet_uri=sheet_uri,
            tab_title=tab_title,
            grid=response.get("values", []),
        )

    def _tab_title(
        self, *, spreadsheet_id: str, sheet_uri: str, gid: int | None
    ) -> str:
        """Returns the title of the tab identified by |gid| in spreadsheet
        |spreadsheet_id|, or the title of the only tab if |gid| is None.
        """
        response = self._execute_request(
            request=self.sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields="sheets/properties(sheetId,title)",
            ),
            sheet_uri=sheet_uri,
        )
        titles_by_gid = {
            sheet["properties"]["sheetId"]: sheet["properties"]["title"]
            for sheet in response["sheets"]
        }

        if gid is not None:
            if gid not in titles_by_gid:
                raise ValueError(
                    f"Google Sheet [{sheet_uri}] pins tab gid [{gid}], which the "
                    f"spreadsheet does not have. Its tabs are: "
                    f"{sorted(titles_by_gid.items())}."
                )
            return titles_by_gid[gid]

        if len(titles_by_gid) == 1:
            return next(iter(titles_by_gid.values()))

        raise ValueError(
            f"Google Sheet URI [{sheet_uri}] pins no tab, but the spreadsheet has "
            f"[{len(titles_by_gid)}] tabs: {sorted(titles_by_gid.items())}. Pin the "
            f"tab to read by appending `#gid=<tab gid>` to the URI."
        )

    @staticmethod
    def _quoted_tab_range(tab_title: str) -> str:
        """Returns the quoted A1 range covering all of tab |tab_title|. Single
        quotes in the title are doubled, which is how A1 notation escapes them.
        """
        return "'" + tab_title.replace("'", "''") + "'"

    @staticmethod
    def _execute_request(*, request: Any, sheet_uri: str) -> dict[str, Any]:
        """Returns the parsed response to |request|. Translates a permission error
        from the Sheets API into an error explaining how to access the sheet.
        """
        try:
            return request.execute()
        except HttpError as e:
            if e.status_code not in _SHEET_ACCESS_ERROR_STATUS_CODES:
                raise
            raise ValueError(
                f"Could not read Google Sheet [{sheet_uri}] (HTTP "
                f"[{e.status_code}]). Grant your local credentials access with "
                f"`{ADC_SETUP_COMMAND}`, or share the spreadsheet with the CI "
                f"service account."
            ) from e

    @classmethod
    def _parse_grid(
        cls, *, sheet_uri: str, tab_title: str, grid: list[list[Any]]
    ) -> GoogleSheetTable:
        """Returns the tab titled |tab_title| parsed from raw sheet grid |grid|. The
        header is row 1 and data rows start at 2. Blank rows are skipped. A cell
        the API returns as a number or boolean (as it does under
        UNFORMATTED_VALUE) is rendered with str().
        """
        if not grid:
            raise ValueError(
                f"Google Sheet [{sheet_uri}] is empty, so it has no header row "
                f"naming its columns."
            )

        header_cells, *data_cells = [
            [str(cell) for cell in row_cells] for row_cells in grid
        ]
        column_names = [cell.strip() for cell in header_cells]
        cls._verify_header(sheet_uri=sheet_uri, column_names=column_names)

        rows = []
        for row_number, cells in enumerate(data_cells, start=2):
            if not any(cell.strip() for cell in cells):
                logging.warning(
                    "Skipping blank row [%s] of Google Sheet [%s].",
                    row_number,
                    sheet_uri,
                )
                continue
            rows.append(
                GoogleSheetTableRow(
                    row_number=row_number,
                    values_by_column=dict(
                        zip(
                            column_names,
                            cls._padded_cells(
                                sheet_uri=sheet_uri,
                                column_names=column_names,
                                row_number=row_number,
                                cells=cells,
                            ),
                        )
                    ),
                )
            )
        return GoogleSheetTable(title=tab_title, column_names=column_names, rows=rows)

    @staticmethod
    def _verify_header(*, sheet_uri: str, column_names: list[str]) -> None:
        """Verifies the sheet's header row is non-empty and every header cell is
        non-blank and unique.
        """
        if not column_names:
            raise ValueError(
                f"Google Sheet [{sheet_uri}] has an empty header row, so it names "
                f"no columns."
            )
        if blank_column_indices := [
            column_index
            for column_index, column_name in enumerate(column_names, start=1)
            if not column_name
        ]:
            raise ValueError(
                f"Google Sheet [{sheet_uri}] has a blank header cell in column(s) "
                f"{blank_column_indices} (1-based). Every column a data row "
                f"reaches must be named."
            )
        if duplicate_column_names := sorted(
            {name for name in column_names if column_names.count(name) > 1}
        ):
            raise ValueError(
                f"Google Sheet [{sheet_uri}] has duplicate header cell(s): "
                f"{duplicate_column_names}."
            )

    @staticmethod
    def _padded_cells(
        *,
        sheet_uri: str,
        column_names: list[str],
        row_number: int,
        cells: list[str],
    ) -> list[str]:
        """Returns |cells| padded with empty strings to the width of the header."""
        if extra_cells := [cell for cell in cells[len(column_names) :] if cell.strip()]:
            raise ValueError(
                f"Row [{row_number}] of Google Sheet [{sheet_uri}] holds value(s) "
                f"{extra_cells} beyond its last header column [{column_names[-1]}], "
                f"so its values are shifted out of alignment with the header."
            )
        return list(cells[: len(column_names)]) + [""] * (
            len(column_names) - len(cells)
        )
