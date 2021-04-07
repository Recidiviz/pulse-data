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
from typing import Any, Callable, TypeVar, Union, overload

Markable = TypeVar("Markable", bound=Union[Callable[..., object], type])

class MarkGenerator:
    def __getattr__(self, name: str) -> MarkDecorator: ...

class MarkDecorator:
    # pytest is doing some weird mypy stuff, so we have to add this ignore
    # https://github.com/pytest-dev/pytest/blob/main/src/_pytest/mark/structures.py#L349
    @overload
    def __call__(self, arg: Markable) -> Markable: ...  # type: ignore[misc]
    @overload
    def __call__(self, *args: Any, **kwargs: Any) -> "MarkDecorator": ...
