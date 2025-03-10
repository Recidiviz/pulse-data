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
from typing import Any, Generic, TypeVar

T = TypeVar("T")

class Token: ...

class ContextVar(Generic[T]):
    def __init__(self, _name: str, default: Any) -> None: ...
    def __enter__(self) -> "ContextVar": ...
    def get(self) -> "ContextVar": ...
    def set(self, value: Any) -> "ContextVar": ...
    def reset(self, token: ContextVar) -> Token: ...
    def count(self, _: Any) -> int: ...
    def __add__(self, _: Any) -> "ContextVar": ...
