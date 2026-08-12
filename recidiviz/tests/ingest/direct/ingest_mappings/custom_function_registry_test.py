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
"""Tests for CustomFunctionRegistry."""
import unittest

from recidiviz.ingest.direct.ingest_mappings.custom_function_registry import (
    CustomFunctionRegistry,
)
from recidiviz.tests.ingest.direct.ingest_mappings.fixtures.ingest_view_file_parser import (
    custom_python,
)


class CustomFunctionRegistryTest(unittest.TestCase):
    """Tests for CustomFunctionRegistry."""

    def setUp(self) -> None:
        self.registry = CustomFunctionRegistry(
            custom_functions_root_module=custom_python
        )

    def test_pep_604_optional_return_type_unwrapped(self) -> None:
        # normalized_ssn_or_null is annotated with a PEP 604 str | None return
        # type; the registry unwraps it to the inner str the same way it
        # unwraps typing.Optional[str].
        function, return_type = self.registry.get_custom_python_function(
            "fake_custom_parsers.normalized_ssn_or_null",
            expected_kwarg_types={"ssn": str},
            expected_return_type=str,
        )
        self.assertIs(str, return_type)
        self.assertEqual("123456789", function(ssn="  123456789  "))
        self.assertIsNone(function(ssn="   "))

    def test_typing_optional_return_type_unwrapped(self) -> None:
        # normalized_ssn_or_null_typing_optional is annotated with a
        # typing.Optional[str] return type; the registry unwraps it to the inner
        # str the same way it unwraps the PEP 604 str | None form.
        function, return_type = self.registry.get_custom_python_function(
            "fake_custom_parsers.normalized_ssn_or_null_typing_optional",
            expected_kwarg_types={"ssn": str},
            expected_return_type=str,
        )
        self.assertIs(str, return_type)
        self.assertEqual("123456789", function(ssn="  123456789  "))
        self.assertIsNone(function(ssn="   "))

    def test_non_optional_return_type_unchanged(self) -> None:
        # A non-optional str return type is left as-is, the behavior both
        # optional paths must stay consistent with.
        _, return_type = self.registry.get_custom_python_function(
            "fake_custom_parsers.ssn_from_parts",
            expected_kwarg_types={
                "ssn_first_three": str,
                "ssn_next_two": str,
                "ssn_last_four": str,
            },
            expected_return_type=str,
        )
        self.assertIs(str, return_type)

    def test_non_optional_union_return_type_rejected(self) -> None:
        # A union return type that is not an Optional (str | int) is rejected,
        # rather than having one of its members silently chosen.
        with self.assertRaisesRegex(
            ValueError,
            r"Custom functions with Union return types other than Optional are "
            r"not supported",
        ):
            self.registry.get_custom_python_function(
                "fake_custom_parsers.ssn_str_or_int",
                expected_kwarg_types={"ssn": str},
                expected_return_type=str,
            )
