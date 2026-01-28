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
"""General utilities for dealing with strings"""
import operator
import string
from typing import Any, FrozenSet, Iterable, Mapping, Sequence, Set, Union

import attr
import Levenshtein as lev


@attr.s(kw_only=True)
class StrictStringFormatter(string.Formatter):
    """Enforces stricter behavior when formatting strings.

    Can be used instead of default string formatting (i.e. 'foo'.format(**)) to enforce
    that only keyword parameters are provided and that all keywords show up in the
    string. If there are keywords that may not show up in the string, these can be
    provided to `allowed_unused_keywords` to avoid an error for those specific items.
    """

    allowed_unused_keywords: FrozenSet[str] = attr.ib(default=frozenset())

    def check_unused_args(
        self,
        used_args: Set[Union[int, str]],
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
    ) -> None:
        if args:
            raise ValueError(f"Positional arguments not allowed, received: {args}")
        used_kwargs = {arg for arg in used_args if isinstance(arg, str)}
        unused_kwargs = set(kwargs.keys()) - (
            used_kwargs | self.allowed_unused_keywords
        )
        if unused_kwargs:
            raise ValueError(f"Unused kwargs passed: {unused_kwargs}")


def get_closest_string(search: str, within: Iterable[str]) -> str:
    return min(
        ((maybe, lev.distance(search, maybe)) for maybe in within),
        key=operator.itemgetter(1),
    )[0]


def is_meaningful_docstring(docstring: str | None) -> bool:
    """Returns true if the provided docstring gives meaningful information, i.e. it is
    non-empty and does not start with an obvious placeholder.
    """
    if not docstring:
        return False

    stripped_docstring = docstring.strip()
    if not stripped_docstring:
        return False

    return (
        # Split up into TO and DO to avoid lint errors
        not stripped_docstring.startswith("TO" + "DO")
        and not stripped_docstring.startswith("XXX")
    )
