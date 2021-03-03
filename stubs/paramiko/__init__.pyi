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
from typing import Optional, MutableMapping, List, Any, Iterator, Union

class SFTPAttributes(object):
    st_mtime: float
    filename: str
    st_mode: int
    def __init__(self) -> None: ...

class RSAKey:
    def __init__(self, data: bytes) -> None: ...

class SSHException(Exception):
    pass

class PKey:
    pass

class HostKeys:
    class SubDict(MutableMapping):
        def __delitem__(self, key: Any) -> None: ...
        def __getitem__(self, key: Any) -> Any: ...
        def __iter__(self) -> Iterator[Any]: ...
        def __len__(self) -> int: ...
        def __setitem__(self, key: Any, val: Any) -> None: ...
    def add(self, name: str, keytype: str, key: Union[PKey, Any]) -> None: ...
    def lookup(self, host: str) -> SubDict: ...

class HostKeyEntry:
    @classmethod
    def from_line(cls, line: str, lineno: Optional[int] = None) -> "HostKeyEntry": ...

class SFTPFile:
    def __enter__(self) -> "SFTPFile": ...
    def __exit__(self, type: Any, value: Any, traceback: Any) -> None: ...
    def readline(self, size: Optional[int] = None) -> bytes: ...
