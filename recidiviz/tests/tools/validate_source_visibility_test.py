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
    InvalidSourceVisibilityError,
    is_valid_module_dependency,
    make_module_matcher,
    validate_dependencies_for_entrypoint,
)


class ValidateSourceVisibilityTest(unittest.TestCase):
    """Tests for validate source visibility tool."""

    def test_validate_is_valid(self) -> None:
        validate_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
            valid_module_prefixes=make_module_matcher(
                {"recidiviz.common", "recidiviz.tests.tools", "recidiviz.utils"}
            ),
        )

    def test_validate_is_valid_from_init(self) -> None:
        validate_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.a.b.d",
            valid_module_prefixes=make_module_matcher(
                {
                    "recidiviz.tests.tools.fixtures.a.b.i",  # imported from __init__
                }
            ),
        )

    def test_validate_is_invalid_only_from_entrypoint(self) -> None:
        with self.assertRaisesRegex(
            InvalidSourceVisibilityError,
            r"no allowed prefix covers[\s\S]*"
            r"recidiviz\.tests\.tools\.fixtures\.no_imports",
        ):
            validate_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.single_import",
                valid_module_prefixes=make_module_matcher(set()),
            )

    def test_validate_is_valid_only_from_entrypoint(self) -> None:
        validate_dependencies_for_entrypoint(
            "recidiviz.tests.tools.fixtures.single_import",
            valid_module_prefixes=make_module_matcher(
                {"recidiviz.tests.tools.fixtures.no_imports"}
            ),
        )

    def test_validate_is_invalid(self) -> None:
        with self.assertRaisesRegex(
            InvalidSourceVisibilityError,
            r"^Entrypoint \[recidiviz\.tests\.tools\.fixtures\."
            r"example_dependency_entrypoint\] does not match the module prefixes it "
            r"is allowed to depend on\.",
        ):
            validate_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                valid_module_prefixes=make_module_matcher(
                    {
                        "recidiviz.common",
                        "recidiviz.tests.tools",
                        "recidiviz.utils.secrets",
                    }
                ),
            )

    def test_validate_unused_valid_module_prefix(self) -> None:
        with self.assertRaisesRegex(
            InvalidSourceVisibilityError,
            r"prefixes allowed for \[recidiviz\.tests\.tools\.fixtures\."
            r"single_import\] that nothing depends on[\s\S]*"
            r"recidiviz\.tests\.tools\.fixtures\.example_dependency_entrypoint",
        ):
            validate_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.single_import",
                valid_module_prefixes=make_module_matcher(
                    {
                        "recidiviz.tests.tools.fixtures.no_imports",
                        "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                    }
                ),
            )

    def test_disallowed_module_prefixes(self) -> None:
        """Test that disallowed module prefixes raise a ValueError."""
        with self.assertRaises(ValueError) as context:
            validate_dependencies_for_entrypoint(
                "recidiviz.tests.tools.fixtures.example_dependency_entrypoint",
                valid_module_prefixes=make_module_matcher(
                    {"recidiviz.common", "recidiviz.research"}
                ),
            )
        self.assertIn(
            "recidiviz.research is a disallowed module",
            str(context.exception),
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
        self.assertFalse(is_valid_module_dependency("recidiviz.repor", prefixes))
        self.assertFalse(is_valid_module_dependency("recidiviz.repor.foo", prefixes))
        self.assertFalse(is_valid_module_dependency("recidiviz.reporting", prefixes))
        self.assertFalse(
            is_valid_module_dependency("recidiviz.reporting.foo", prefixes)
        )

    def test_is_valid_module_dependency_research(self) -> None:
        prefixes = make_module_matcher(["recidiviz.report"])
        self.assertFalse(is_valid_module_dependency("recidiviz.research", prefixes))
        self.assertFalse(is_valid_module_dependency("recidiviz.research.foo", prefixes))
        self.assertFalse(
            is_valid_module_dependency("recidiviz.research.notebooks", prefixes)
        )
        self.assertFalse(
            is_valid_module_dependency("recidiviz.research.notebooks.foo", prefixes)
        )
