# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for validate source visibility tool."""


import unittest

from recidiviz.tools.validate_source_visibility import (
    check_dependencies_for_entrypoint,
    is_valid_module_dependency,
    make_module_matcher,
)


class ValidateSourceVisibilityTest(unittest.TestCase):
    """Tests for validate source visibility tool."""

    def test_validate_is_valid(self) -> None:
        self.assertTrue(
            check_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                valid_module_prefixes=make_module_matcher(
                    {"recidiviz.common", "recidiviz.tests.tools", "recidiviz.utils"}
                ),
            )
        )

    def test_validate_is_valid_from_init(self) -> None:
        self.assertTrue(
            check_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.a.b.d",
                valid_module_prefixes=make_module_matcher(
                    {
                        "recidiviz.tests.tools.fixtures.a.b.i",  # imported from __init__
                    }
                ),
            )
        )

    def test_validate_is_invalid_only_from_entrypoint(self) -> None:
        self.assertFalse(
            check_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.single_import",
                valid_module_prefixes=make_module_matcher(set()),
            )
        )

    def test_validate_is_valid_only_from_entrypoint(self) -> None:
        self.assertTrue(
            check_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.single_import",
                valid_module_prefixes=make_module_matcher(
                    {"recidiviz.tests.tools.fixtures.no_imports"}
                ),
            )
        )

    def test_validate_is_invalid(self) -> None:
        self.assertFalse(
            check_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                valid_module_prefixes=make_module_matcher(
                    {
                        "recidiviz.common",
                        "recidiviz.tests.tools",
                        "recidiviz.utils.secrets",
                    }
                ),
            )
        )

    # is_valid_module_dependency helper tests
    def test_is_valid_module_dependency_valid(self) -> None:
        prefixes = make_module_matcher(["recidiviz.report"])
        self.assertTrue(is_valid_module_dependency("recidiviz.report.foo", prefixes))
        self.assertTrue(is_valid_module_dependency("recidiviz.report", prefixes))
        self.assertTrue(is_valid_module_dependency("recidiviz", prefixes))

    def test_is_valid_module_dependency_invalid(self) -> None:
        prefixes = make_module_matcher(["recidiviz.report"])
        self.assertFalse(is_valid_module_dependency("recidiviz.pipelines", prefixes))

    def test_is_valid_module_dependency_invalid_but_prefix(self) -> None:
        prefixes = make_module_matcher(["recidiviz.report"])
        self.assertFalse(is_valid_module_dependency("recidiviz.repo", prefixes))
        self.assertFalse(is_valid_module_dependency("recidiviz.repo.foo", prefixes))
        self.assertFalse(is_valid_module_dependency("recidiviz.reporting", prefixes))
        self.assertFalse(
            is_valid_module_dependency("recidiviz.reporting.foo", prefixes)
        )
