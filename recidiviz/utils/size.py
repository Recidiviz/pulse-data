# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helpers to identify the size of objects"""

import collections
import sys
from collections import deque
from itertools import chain
from typing import Any, Callable, Dict, Iterable, Optional, Type


def _iter_dict(d: Dict[Any, Any]) -> Iterable[Any]:
    return chain.from_iterable(d.items())


def total_size(
    o: Any,
    handlers: Optional[Dict[Type[Any], Callable[[Any], Iterable[Any]]]] = None,
    include_duplicates: bool = False,
) -> int:
    """Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    Based on recipe here: https://docs.python.org/dev/library/sys.html#sys.getsizeof
    """
    all_handlers: Dict[Type[Any], Callable[[Any], Iterable[Any]]] = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: _iter_dict,
        set: iter,
        frozenset: iter,
    }
    all_handlers.update(handlers or {})  # user handlers take precedence
    seen = set()
    default_size = sys.getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(o: Any) -> int:
        if not include_duplicates and id(o) in seen:
            return 0
        seen.add(id(o))
        s = sys.getsizeof(o, default_size)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        else:
            if isinstance(o, collections.Collection) and not isinstance(
                o, (str, bytes, bytearray)
            ):
                raise NotImplementedError(f"No handler for collection: {type(o)}")
        return s

    return sizeof(o)
