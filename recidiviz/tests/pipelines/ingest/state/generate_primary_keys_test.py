# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for generating primary keys based on external IDs."""
import unittest
from typing import Set

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.state.generate_primary_keys import generate_primary_key


class TestGeneratePrimaryKey(unittest.TestCase):
    """Tests for generating primary keys based on external IDs."""

    def test_generate_primary_key_consistent(self) -> None:
        external_id_1 = ("ID1", "TYPE1")
        generated_primary_keys: Set[int] = set()
        for _ in range(10000):
            generated_primary_keys.add(
                generate_primary_key({external_id_1}, StateCode.US_DD)
            )
        self.assertEqual(len(generated_primary_keys), 1)

        external_id_2 = ("ID2", "TYPE2")
        external_id_3 = ("ID3", "TYPE3")
        generated_primary_keys.clear()
        for _ in range(2000):
            generated_primary_keys.add(
                generate_primary_key(
                    {external_id_1, external_id_2, external_id_3}, StateCode.US_DD
                )
            )
        self.assertEqual(len(generated_primary_keys), 1)

    def test_generate_primary_key_always_starts_with_fips_mask(self) -> None:
        external_id = ("ID", "TYPE")
        for state_code in StateCode:
            if state_code == StateCode.US_OZ:
                # OZ has a fips mask of 00, so we skip it.
                continue
            primary_key = generate_primary_key({external_id}, state_code)
            self.assertTrue(
                str(primary_key).startswith(str(int(state_code.get_state().fips)))
            )

    def test_generate_primary_key_deterministic(self) -> None:
        external_id = ("ID", "TYPE")
        self.assertEqual(
            generate_primary_key({external_id}, StateCode.US_MO), 2925259285447670540
        )
        self.assertEqual(
            generate_primary_key({external_id}, StateCode.US_PA), 4225259285447670540
        )
