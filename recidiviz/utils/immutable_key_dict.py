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
"""Defines a subclass of the basic Python dict class which throws a KeyError if you
attempt to set a key that is already set.
"""
from typing import Any, Generic, Iterable, Mapping, TypeVar

KT = TypeVar("KT")
VT = TypeVar("VT")


class ImmutableKeyDict(Generic[KT, VT], dict[KT, VT]):
    """A subclass of the basic Python dict class which throws a KeyError if you attempt
    to set a key that is already set.

    This class reimplements update() in Python and therefore update() calls will be
    significantly slower for large inputs. This class should generally be used for small
    collections (e.g. less than ~1000).
    """

    def __setitem__(self, key: KT, value: VT) -> None:
        if key in self:
            raise KeyError(f"Key '{key}' already exists and cannot be overwritten.")
        super().__setitem__(key, value)

    def update(self, *args: Any, **kwargs: VT) -> None:
        """The standard Python dict implements update() in C for efficiency reasons, but
        this means __setitem__ is never called. Here we reimplement update() in Python
        so that we can enforce that no duplicate keys are added.
        """
        # Process positional arguments
        if args:
            if len(args) > 1:
                raise TypeError(
                    f"update() takes at most one positional argument ({len(args)} given)"
                )
            arg = args[0]
            if isinstance(arg, Mapping):
                for key, value in arg.items():
                    self[key] = value
            elif isinstance(arg, Iterable):
                for key, value in arg:
                    self[key] = value
            else:
                raise TypeError(
                    f"update() argument must be a mapping or iterable, not {type(arg)}"
                )

        if kwargs:
            raise ValueError("kwargs not supported in update()")
