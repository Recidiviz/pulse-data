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
"""Tests for primary key generation from external id keys."""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.generate_primary_key import (
    generate_primary_key,
    generate_primary_key_from_external_id_keys,
)


class GeneratePrimaryKeyFromExternalIdKeysTest(unittest.TestCase):
    """Tests for generate_primary_key_from_external_id_keys()."""

    def test_key_is_stable(self) -> None:
        """The derivation is a contract shared by every producer of these keys
        (the activity pipeline, the Identity Service export): if this golden
        value changes, every persisted key changes with it."""
        self.assertEqual(
            50136366673540915,
            generate_primary_key_from_external_id_keys(
                {("A1234", "US_OZ_KDS_PERSON_ID"), ("E99", "US_OZ_LOTR_ID")},
                state_code=StateCode.US_OZ,
            ),
        )

    def test_key_matches_hash_of_sorted_type_pipe_id_form(self) -> None:
        self.assertEqual(
            generate_primary_key(
                "US_OZ_KDS_PERSON_ID|A1234,US_OZ_LOTR_ID|E99",
                state_code=StateCode.US_OZ,
            ),
            generate_primary_key_from_external_id_keys(
                {("E99", "US_OZ_LOTR_ID"), ("A1234", "US_OZ_KDS_PERSON_ID")},
                state_code=StateCode.US_OZ,
            ),
        )

    def test_different_id_sets_get_different_keys(self) -> None:
        self.assertNotEqual(
            generate_primary_key_from_external_id_keys(
                {("A1234", "US_OZ_KDS_PERSON_ID")}, state_code=StateCode.US_OZ
            ),
            generate_primary_key_from_external_id_keys(
                {("A1234", "US_OZ_KDS_PERSON_ID"), ("E99", "US_OZ_LOTR_ID")},
                state_code=StateCode.US_OZ,
            ),
        )

    def test_state_code_masks_the_key(self) -> None:
        keys = {("A1234", "PERSON_ID")}
        self.assertNotEqual(
            generate_primary_key_from_external_id_keys(
                keys, state_code=StateCode.US_ND
            ),
            generate_primary_key_from_external_id_keys(
                keys, state_code=StateCode.US_OZ
            ),
        )
