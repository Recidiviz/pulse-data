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
"""Unit tests for test_utils.py"""
from unittest import TestCase

from recidiviz.tests.utils.test_utils import assert_group_contains_regex


class TestGroupExceptions(TestCase):
    """Unit tests for assert_group_contains_regex"""

    def test_simple(self) -> None:
        with assert_group_contains_regex(r"testing", [(ValueError, r"a")]):
            raise ExceptionGroup("testing", [ValueError("a")])

        with assert_group_contains_regex(r"testing", [(ValueError, r"aa*")]):
            raise ExceptionGroup("testing", [ValueError("aaaaaaaa")])

    def test_complex(self) -> None:
        with assert_group_contains_regex(
            r"testing",
            [
                (ValueError, r"a"),  # matches two
                (RuntimeError, r"we running it"),  # since is .search can substr match
            ],
        ):
            raise ExceptionGroup(
                "testing",
                [
                    ValueError("a"),
                    ValueError("aa"),
                    RuntimeError("--- we running it ---"),
                ],
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Expected to find \[IndentationError\] with regex \[a\] in the following exceptions but did not: .*",
        ):
            with assert_group_contains_regex(
                r"testing",
                [
                    (IndentationError, r"a"),  # matches two
                    (
                        RuntimeError,
                        r"we running it",
                    ),  # since is .search can substr match
                ],
            ):
                raise ExceptionGroup(
                    "testing",
                    [
                        ValueError("a"),
                        ValueError("a"),
                        RuntimeError("--- we running it ---"),
                    ],
                )

    def test_strict(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Found the following exceptions that had no match in \[\]:.*"
        ):
            with assert_group_contains_regex(r"testing", []):
                raise ExceptionGroup("testing", [ValueError("a")])

        with assert_group_contains_regex(r"testing", [], strict=False):
            raise ExceptionGroup("testing", [ValueError("a")])
