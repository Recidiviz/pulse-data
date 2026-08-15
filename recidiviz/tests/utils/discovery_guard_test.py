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
"""Tests for DiscoveryGuardMeta."""
import abc
import unittest

from recidiviz.tests.utils.discovery_guard import DiscoveryGuardMeta


class _GuardedBase(unittest.TestCase, metaclass=DiscoveryGuardMeta):
    """Abstract root of a small guarded hierarchy for exercising the guard."""

    __test__ = False

    @classmethod
    @abc.abstractmethod
    def label(cls) -> str:
        """Returns a value concrete tests must supply."""

    @classmethod
    @abc.abstractmethod
    def value(cls) -> int:
        """Returns a second value concrete tests must supply."""


class DiscoveryGuardTest(unittest.TestCase):
    """Tests that DiscoveryGuardMeta rejects subclasses that would silently
    never run."""

    def test_partial_base_may_opt_out_of_discovery(self) -> None:
        # A base class that leaves an abstract method to its subclasses may set
        # __test__ = False.
        class _PartialBase(  # pylint: disable=abstract-method,unused-variable
            _GuardedBase
        ):
            __test__ = False

            @classmethod
            def label(cls) -> str:
                return "partial"

    def test_fully_implemented_class_may_not_opt_out(self) -> None:
        # A class that implements every abstract method is a runnable test, so
        # opting out of discovery would silently skip it.
        with self.assertRaisesRegex(
            TypeError,
            r"^Class \[_CompleteButHidden\] sets `__test__ = False` but "
            r"implements every abstract method",
        ):

            class _CompleteButHidden(_GuardedBase):  # pylint: disable=unused-variable
                __test__ = False

                @classmethod
                def label(cls) -> str:
                    return "complete"

                @classmethod
                def value(cls) -> int:
                    return 1

    def test_omitting_test_attribute_raises(self) -> None:
        # Every subclass must make its discovery choice explicitly.
        with self.assertRaisesRegex(
            TypeError, r"^Class \[_Undeclared\] must explicitly set `__test__`"
        ):

            class _Undeclared(  # pylint: disable=abstract-method,unused-variable
                _GuardedBase
            ):
                @classmethod
                def label(cls) -> str:
                    return "undeclared"
