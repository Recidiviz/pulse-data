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
"""Source that reads the rows of one extractor's golden eval set from its Google
Sheet.
"""
import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.google_sheets_reader import GoogleSheetReader, GoogleSheetTable


class GoldenEvalDocumentSourceError(ValueError):
    """Every problem found across a golden eval set, reported together so an
    author fixes them in one pass.
    """

    def __init__(self, message: str, issues: list[str]) -> None:
        super().__init__(
            "\n".join([f"{message}:", *(f"  - {issue}" for issue in issues)])
            if issues
            else message
        )
        self.issues = issues


@attr.define(frozen=True, kw_only=True)
class GoldenEvalGoogleSheetDocumentSource:
    """Source that reads the rows of one extractor's golden eval set from its
    Google Sheet, verifying the tab read holds the right state's eval set and is
    not empty.
    """

    sheets_reader: GoogleSheetReader = attr.ib(
        validator=attr.validators.instance_of(GoogleSheetReader)
    )
    """The reader the eval sheet is read through."""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    """The state whose eval set this source reads. Each state's eval set has its
    own sheet tab.
    """

    source_sheet_uri: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """URI of the sheet the rows come from."""

    def read_table(self) -> GoogleSheetTable:
        """Returns the sheet's rows as a table, having verified the tab read is
        titled for the state and holds at least one row.
        """
        tab = self.sheets_reader.read_tab_as_table(sheet_uri=self.source_sheet_uri)
        self._verify_tab_is_titled_for_the_state(tab=tab)
        if not tab.rows:
            raise GoldenEvalDocumentSourceError(
                f"Golden eval sheet [{self.source_sheet_uri}] holds no rows, so "
                f"there is nothing to evaluate.",
                [],
            )
        return tab

    def _verify_tab_is_titled_for_the_state(self, *, tab: GoogleSheetTable) -> None:
        """Verifies the tab read holds the eval set of the state being evaluated,
        which its title names.
        """
        if tab.title != self.state_code.value:
            raise ValueError(
                f"Golden eval sheet [{self.source_sheet_uri}] reads tab "
                f"[{tab.title}], but it is the eval set for state "
                f"[{self.state_code.value}]. A state's eval set lives in the tab "
                f"named after its state code."
            )
