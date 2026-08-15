# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Metaclass for test-case hierarchies whose abstract base classes opt out of
pytest discovery with __test__ = False.
"""
import abc
from typing import Any


class DiscoveryGuardMeta(abc.ABCMeta):
    """Metaclass for test-case hierarchies whose abstract base classes opt out
    of pytest discovery with __test__ = False.

    Every class in the hierarchy must set __test__ in its own body: True to
    run, False to opt out of discovery. Opting out is only legitimate for a
    base class that leaves at least one abstract method to its subclasses; a
    fully implemented class with __test__ = False is a runnable test that
    would silently never run, so this metaclass rejects it at class creation
    time.
    """

    def __new__(
        mcs, cls_name: str, bases: tuple[type, ...], attributes: dict[str, Any]
    ) -> "DiscoveryGuardMeta":
        if "__test__" not in attributes:
            raise TypeError(
                f"Class [{cls_name}] must explicitly set `__test__` "
                f"(True to run, False to opt out of discovery)."
            )
        cls = super().__new__(mcs, cls_name, bases, attributes)
        # ABCMeta populates __abstractmethods__ as part of super().__new__, so
        # this check runs on the built class.
        if not attributes["__test__"] and not cls.__abstractmethods__:
            raise TypeError(
                f"Class [{cls_name}] sets `__test__ = False` but implements every "
                f"abstract method, so it is a runnable test that would never "
                f"run. Set `__test__ = True`, or leave at least one abstract "
                f"method to subclasses if this is a base class."
            )
        return cls
