# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

from typing import Dict, List, Optional, Type, TypeVar

from google.auth.credentials import Credentials

class Worksheet:
    @property
    def title(self) -> str: ...
    def get_all_records(
        self,
        empty2zero: bool = False,
        head: int = 1,
        default_blank: str = "",
        allow_underscores_in_numeric_literals: bool = False,
        numericise_ignore: Optional[List[int]] = None,
    ) -> List[Dict[str, str]]: ...
    def update_title(self, title: str) -> Dict: ...

class Spreadsheet:
    @property
    def title(self) -> str: ...
    def worksheets(self) -> List[Worksheet]: ...

class Client:
    def open_by_key(self, key: str) -> Spreadsheet: ...
    def import_csv(self, file_id: str, data: str) -> None: ...

ClientT = TypeVar("ClientT")
# For some reason mypy doesn't like the default assignment here.
def authorize(credentials: Credentials, client_class: Type[ClientT] = Client) -> ClientT: ...  # type: ignore[assignment]
