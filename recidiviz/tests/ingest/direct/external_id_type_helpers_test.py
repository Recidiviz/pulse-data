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
"""Tests for external_id_type_helpers."""
import unittest
from unittest import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.external_id_type_helpers import (
    external_id_types_by_state_code,
    get_external_id_types,
    is_valid_external_id_type_shape,
    validate_external_id_types,
)


class IsValidExternalIdTypeShapeTest(unittest.TestCase):
    """Tests for is_valid_external_id_type_shape."""

    def test_all_real_external_id_types_pass(self) -> None:
        for external_id_type in get_external_id_types():
            with self.subTest(external_id_type=external_id_type):
                self.assertTrue(is_valid_external_id_type_shape(external_id_type))

    def test_valid_shapes(self) -> None:
        self.assertTrue(is_valid_external_id_type_shape("US_CO_OFFENDERID"))
        self.assertTrue(is_valid_external_id_type_shape("US_XX"))

    def test_missing_state_code_prefix(self) -> None:
        self.assertFalse(is_valid_external_id_type_shape("OFFENDERID"))
        self.assertFalse(is_valid_external_id_type_shape("CO_OFFENDERID"))

    def test_wrong_case(self) -> None:
        self.assertFalse(is_valid_external_id_type_shape("us_co_offenderid"))
        self.assertFalse(is_valid_external_id_type_shape("US_Co_OffenderId"))

    def test_disallowed_characters(self) -> None:
        self.assertFalse(is_valid_external_id_type_shape(""))
        self.assertFalse(is_valid_external_id_type_shape("US_CO_OFFENDER_ID_2"))
        self.assertFalse(is_valid_external_id_type_shape("US_CO OFFENDERID"))
        self.assertFalse(is_valid_external_id_type_shape("US_CO_OFFENDERID' OR '1'='1"))
        self.assertFalse(is_valid_external_id_type_shape("US_CO_OFFENDERID\nUS_XX_DOC"))


class ExternalIdTypesByStateCodeTest(unittest.TestCase):
    """Tests for external_id_types_by_state_code."""

    def test_all_real_types_owned_by_prefix_state(self) -> None:
        for state_code, id_types in external_id_types_by_state_code().items():
            for id_type in id_types:
                with self.subTest(id_type=id_type):
                    self.assertTrue(id_type.startswith(state_code.value))

    def test_three_letter_code_owns_its_types_not_the_two_letter_prefix(self) -> None:
        with mock.patch(
            "recidiviz.ingest.direct.external_id_type_helpers.get_external_id_types",
            return_value=["US_NY_DOC", "US_NYC_TESTID"],
        ):
            result = external_id_types_by_state_code()

        self.assertEqual({"US_NY_DOC"}, result[StateCode.US_NY])
        self.assertEqual({"US_NYC_TESTID"}, result[StateCode.US_NYC])


class ValidateExternalIdTypesTest(unittest.TestCase):
    """Tests for validate_external_id_types."""

    def test_registered_types_pass(self) -> None:
        validate_external_id_types(
            state_code=StateCode.US_ND,
            external_id_types_to_check=["US_ND_SID", "US_ND_ELITE"],
        )

    def test_empty_input_passes(self) -> None:
        validate_external_id_types(
            state_code=StateCode.US_ND, external_id_types_to_check=[]
        )

    def test_unregistered_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"External id type\(s\) \['US_ND_DOC'\] are not registered for "
            r"\[US_ND\]\.",
        ):
            validate_external_id_types(
                state_code=StateCode.US_ND,
                external_id_types_to_check=["US_ND_DOC", "US_ND_ELITE"],
            )

    def test_wrong_state_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"External id type\(s\) \['US_PA_CONT'\] are not registered for "
            r"\[US_ND\]\.",
        ):
            validate_external_id_types(
                state_code=StateCode.US_ND,
                external_id_types_to_check=["US_PA_CONT"],
            )

    def test_multiple_invalid_types_reported_deduped_and_sorted(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"External id type\(s\) \['US_ND_DOC', 'US_PA_CONT'\] are not "
            r"registered for \[US_ND\]\.",
        ):
            validate_external_id_types(
                state_code=StateCode.US_ND,
                external_id_types_to_check=["US_PA_CONT", "US_ND_DOC", "US_ND_DOC"],
            )
