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
from paramiko import SFTPAttributes, PKey, HostKeys, SFTPFile
from typing import Any, List, Optional, Union, Callable


class CnOpts(object):
    hostkeys: HostKeys

    def __init__(self, knownhosts: Optional[str] = None) -> None: ...

    def get_hostkey(self, host: str) -> List[PKey]: ...


class Connection:
    def __init__(self, host: str, username: Optional[str] = None, private_key: Optional[str] = None, password: Optional[str] = None,
                 port: int = 22, private_key_pass: Optional[str] = None, ciphers: Optional[list] = None, log: Union[bool, str] = False,
                 cnopts: Optional[CnOpts] = None, default_path: Optional[str] = None) -> None: ...

    def __enter__(self) -> 'Connection': ...
    def __exit__(self, etype: Any, value: Any, traceback: Any) -> None: ...
    def get_r(self, remotedir: str, localdir: str, preserve_mtime: bool = False) -> None: ...
    def isdir(self, remotepath: str) -> bool: ...
    def listdir(self, remotepath: str = '.') -> List[str]: ...
    def listdir_attr(self, remotepath: str = '.') -> List[SFTPAttributes]: ...
    def open(self, remote_file: str, mode: str = 'r', bufsize: int = 1) -> SFTPFile: ...

    def walktree(self, remotepath: str, fcallback: Callable[[str], None], dcallback: Callable[[
                 str], None], ucallback: Callable[[str], None], recurse: bool) -> None: ...
