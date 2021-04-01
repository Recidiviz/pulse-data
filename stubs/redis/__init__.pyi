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
from typing import Optional

class Redis:
    def expire(self, key: str, expiry: int) -> None: ...
    def rpush(self, key: str, value: str) -> int: ...
    def setex(self, key: str, expiry: int, value: str) -> None: ...
    def llen(self, key: str) -> int: ...
    def lindex(self, key: str, index: int) -> Optional[bytes]: ...
    def delete(self, key: str) -> int: ...
    def exists(self, key: str) -> bool: ...
    def set(self, key: str, value: str) -> None: ...
