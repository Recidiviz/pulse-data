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
from types import TracebackType
from typing import Any, List, Optional, TextIO

class OperationalError:
    pgcode: str

class Cursor:
    def execute(self, query: str) -> None: ...
    def copy_expert(self, query: str, file: TextIO) -> None: ...
    def fetchall(self) -> List[Any]: ...
    def __enter__(self) -> Cursor: ...
    def __exit__(
        self,
        exc_type: Optional[Any],
        exc_value: Optional[Any],
        traceback: Optional[TracebackType],
    ) -> Cursor: ...

class Connection:
    def cursor(self) -> Cursor: ...
    def execute(self, query: str) -> None: ...
    def set_isolation_level(self, isolation_level: int) -> None: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...

def connect(
    dbname: str,
    host: str,
    user: str,
    password: str,
    sslrootcert: Optional[str] = None,
    sslcert: Optional[str] = None,
    sslkey: Optional[str] = None,
) -> Connection: ...
