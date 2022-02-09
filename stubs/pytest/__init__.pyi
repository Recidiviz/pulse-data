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
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Pattern,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from .approx import ApproxBase
from .mark import MarkGenerator

mark: MarkGenerator

_Scope = Literal["session", "package", "module", "class", "function"]

FixtureFunction = TypeVar("FixtureFunction", bound=Callable[..., object])

E = TypeVar("E", bound=BaseException)

@overload
def fixture(
    fixture_function: FixtureFunction,
    *,
    scope: "Union[_Scope, Callable[[str, Config], _Scope]]" = ...,
    params: Optional[Iterable[object]] = ...,
    autouse: bool = ...,
    ids: Optional[
        Union[
            Iterable[Union[None, str, float, int, bool]],
            Callable[[Any], Optional[object]],
        ]
    ] = ...,
    name: Optional[str] = ...,
) -> FixtureFunction: ...
@overload
def fixture(
    fixture_function: None = ...,
    *,
    scope: "Union[_Scope, Callable[[str, Config], _Scope]]" = ...,
    params: Optional[Iterable[object]] = ...,
    autouse: bool = ...,
    ids: Optional[
        Union[
            Iterable[Union[None, str, float, int, bool]],
            Callable[[Any], Optional[object]],
        ]
    ] = ...,
    name: Optional[str] = None,
) -> FixtureFunctionMarker: ...
@overload
def raises(
    expected_exception: Union[Type[E], Tuple[Type[E], ...]],
    *,
    match: Optional[Union[str, Pattern[str]]] = ...,
) -> RaisesContext[E]: ...
@overload
def raises(
    expected_exception: Union[Type[E], Tuple[Type[E], ...]],
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> ExceptionInfo[E]: ...
def approx(expected: float) -> ApproxBase: ...

class Config:
    def getoption(
        self, name: str, default: Optional[str] = None, skip: Optional[bool] = False
    ) -> str: ...

class FixtureFunctionMarker:
    def __call__(self, function: FixtureFunction) -> FixtureFunction: ...

class ExceptionInfo(Generic[E]):
    @property
    def value(self) -> E: ...

class RaisesContext(Generic[E]):
    def __enter__(self) -> ExceptionInfo[E]: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool: ...

def skip(reason: str = None) -> None: ...

class Item:
    config: Config
    fixturenames: List[str]
    def get_closest_marker(self, name: str, default: Optional[Any] = None) -> Any: ...
