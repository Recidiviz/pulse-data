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
"""Tests for recidiviz.common.external_id_selection."""
import unittest

from recidiviz.common.constants.state.external_id_types import (
    US_IX_STAFF_ID,
    US_MI_COMPAS_USER,
    US_MI_OMNI_USER,
    US_ND_ELITE,
    US_PA_INMATE,
    US_TX_TDCJ,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.external_id_selection import (
    has_shared_selection_strategy,
    select_display_external_id,
    select_stable_external_id,
)


class TestExternalIdSelection(unittest.TestCase):
    """Tests for recidiviz.common.external_id_selection."""

    def test_alphabetical_display_is_alphabetically_highest(self) -> None:
        self.assertEqual(
            "C",
            select_display_external_id(
                tenant=Tenant.US_ND,
                id_type=US_ND_ELITE,
                external_ids=["B", "A", "C"],
            ),
        )

    def test_alphabetical_stable_is_alphabetically_lowest(self) -> None:
        self.assertEqual(
            "A",
            select_stable_external_id(
                tenant=Tenant.US_ND,
                id_type=US_ND_ELITE,
                external_ids=["B", "A", "C"],
            ),
        )

    def test_numeric_lpad_display_is_numerically_highest(self) -> None:
        # Alphabetically "1237" < "429" < "88", so an alphabetical selector would pick
        # "88"; numerically 88 < 429 < 1237, so the numeric selector picks "1237".
        self.assertEqual(
            "1237",
            select_display_external_id(
                tenant=Tenant.US_MI,
                id_type=US_MI_COMPAS_USER,
                external_ids=["429", "1237", "88"],
            ),
        )

    def test_numeric_lpad_stable_is_numerically_lowest(self) -> None:
        self.assertEqual(
            "88",
            select_stable_external_id(
                tenant=Tenant.US_MI,
                id_type=US_MI_COMPAS_USER,
                external_ids=["429", "1237", "88"],
            ),
        )

    def test_has_shared_selection_strategy_true_for_registered_pair(self) -> None:
        self.assertTrue(
            has_shared_selection_strategy(tenant=Tenant.US_ND, id_type=US_ND_ELITE)
        )

    def test_has_shared_selection_strategy_false_for_unregistered_pair(self) -> None:
        self.assertFalse(
            has_shared_selection_strategy(tenant=Tenant.US_XX, id_type="US_XX_FAKE_ID")
        )

    def test_raises_for_unregistered_tenant_and_id_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^No shared display/stable selection strategy for tenant \[US_XX\] and "
            r"id_type \[US_XX_FAKE_ID\]\.",
        ):
            select_display_external_id(
                tenant=Tenant.US_XX,
                id_type="US_XX_FAKE_ID",
                external_ids=["A", "B"],
            )

    def test_raises_for_empty_external_ids(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Cannot select a display/stable external id for tenant \[US_ND\] and "
            r"id_type \[US_ND_ELITE\] from an empty list\.$",
        ):
            select_display_external_id(
                tenant=Tenant.US_ND, id_type=US_ND_ELITE, external_ids=[]
            )

    def test_selection_per_tenant_and_id_type(self) -> None:
        """Pins the display/stable choice for a representative (tenant, id_type) of each
        strategy, so a bad edit to the strategy registry flips a value here.
        """
        # (tenant, id_type, external_ids, expected_display, expected_stable)
        cases = [
            (Tenant.US_ND, US_ND_ELITE, ["eB", "eA", "eC"], "eC", "eA"),
            (Tenant.US_PA, US_PA_INMATE, ["p3", "p1", "p2"], "p3", "p1"),
            (Tenant.US_TX, US_TX_TDCJ, ["t2", "t1"], "t2", "t1"),
            (Tenant.US_IX, US_IX_STAFF_ID, ["x2", "x1"], "x2", "x1"),
            # US_MI OMNI/COMPAS staff ids compare numerically, so "1237" beats "429".
            (Tenant.US_MI, US_MI_COMPAS_USER, ["429", "1237", "88"], "1237", "88"),
            (Tenant.US_MI, US_MI_OMNI_USER, ["429", "1237", "88"], "1237", "88"),
        ]
        for tenant, id_type, external_ids, expected_display, expected_stable in cases:
            with self.subTest(tenant=tenant.value, id_type=id_type):
                self.assertEqual(
                    expected_display,
                    select_display_external_id(
                        tenant=tenant, id_type=id_type, external_ids=external_ids
                    ),
                )
                self.assertEqual(
                    expected_stable,
                    select_stable_external_id(
                        tenant=tenant, id_type=id_type, external_ids=external_ids
                    ),
                )

    def test_raises_for_duplicate_external_ids(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found external ids \[\['A', 'A'\]\] of id_type \[US_ND_ELITE\] for "
            r"tenant \[US_ND\] that the selection strategy considers identical",
        ):
            select_stable_external_id(
                tenant=Tenant.US_ND,
                id_type=US_ND_ELITE,
                external_ids=["A", "A", "B"],
            )

    def test_raises_for_numeric_ids_differing_only_in_zero_padding(self) -> None:
        """0123 and 123 are distinct strings but equal once zero-padded, so a
        NUMERIC_LPAD selection between them would be arbitrary."""
        with self.assertRaisesRegex(
            ValueError,
            r"^Found external ids \[\['0123', '123'\]\] of id_type "
            r"\[US_MI_COMPAS_USER\] for tenant \[US_MI\] that the selection strategy "
            r"considers identical",
        ):
            select_display_external_id(
                tenant=Tenant.US_MI,
                id_type=US_MI_COMPAS_USER,
                external_ids=["0123", "123", "88"],
            )

    def test_alphabetical_ids_differing_in_zero_padding_are_distinct(self) -> None:
        """Under ALPHABETICAL the sort key is the id itself, so 0123 and 123 are
        ordinary distinct candidates."""
        self.assertEqual(
            "123",
            select_display_external_id(
                tenant=Tenant.US_ND,
                id_type=US_ND_ELITE,
                external_ids=["0123", "123"],
            ),
        )
