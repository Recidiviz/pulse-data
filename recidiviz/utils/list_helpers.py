# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Generic helpers for working with lists"""

from collections import defaultdict
from typing import Callable, Iterable, Mapping, TypeVar

T = TypeVar("T")  # The type of the items
K = TypeVar("K")  # The type of the key
V = TypeVar("V")  # The type of the values


def group_by(items: Iterable[T], key_fn: Callable[[T], K]) -> dict[K, list[T]]:
    """Groups items by the key returned by the key function."""
    grouped: dict[K, list[T]] = defaultdict(list)
    for item in items:
        grouped[key_fn(item)].append(item)
    return dict(grouped)


def flatten_values_for_keys(
    keys: Iterable[K], values_by_key: Mapping[K, list[V]]
) -> list[V]:
    """Concatenates the value lists for keys, in order, raising if any key is
    absent from values_by_key.
    """
    flattened: list[V] = []
    for key in keys:
        if key not in values_by_key:
            raise KeyError(f"No values found for key [{key}]")
        flattened.extend(values_by_key[key])
    return flattened


def index_by(items: Iterable[T], key_fn: Callable[[T], K]) -> dict[K, T]:
    """Indexes items by the key returned by the key function, raising if two items
    share a key.
    """
    indexed: dict[K, T] = {}
    for item in items:
        key = key_fn(item)
        if key in indexed:
            raise ValueError(f"Found more than one item with key [{key}]")
        indexed[key] = item
    return indexed
