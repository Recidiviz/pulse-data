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

from recidiviz.ingest.direct.external_id_type_helpers import (
    get_external_id_types,
    is_valid_external_id_type_shape,
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
