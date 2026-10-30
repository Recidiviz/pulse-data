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
"""Tests for the display/stable external id flag resolver."""
import uuid
from unittest import TestCase

from recidiviz.common.constants.identity import IdentifierType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.services.identity.import_processing.external_id_flag_resolver import (
    resolve_display_and_stable_id_flags,
)

_RECIDIVIZ_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")


def _row(
    *,
    external_id: str,
    id_type: IdentifierType,
    is_active: bool = True,
    is_current_display_id_for_type: bool = False,
    is_stable_id_for_type: bool = False,
) -> schema.ExternalId:
    return schema.ExternalId(
        recidiviz_id=_RECIDIVIZ_ID,
        external_id=external_id,
        id_type=id_type,
        is_active=is_active,
        is_current_display_id_for_type=is_current_display_id_for_type,
        is_stable_id_for_type=is_stable_id_for_type,
    )


def _flags(row: schema.ExternalId) -> tuple[bool, bool]:
    return (row.is_current_display_id_for_type, row.is_stable_id_for_type)


class ResolveDisplayAndStableIdFlagsTest(TestCase):
    """Tests for resolve_display_and_stable_id_flags."""

    def test_single_active_id_of_type_gets_both_flags_without_a_strategy(
        self,
    ) -> None:
        """(US_OZ, US_OZ_LOTR_ID) has no registered selection strategy, so a
        raise here would mean the trivial single-id case wrongly consults the
        registry."""
        row = _row(external_id="A1", id_type=IdentifierType.US_OZ_LOTR_ID)

        resolve_display_and_stable_id_flags(tenant=Tenant.US_OZ, external_id_rows=[row])

        self.assertEqual((True, True), _flags(row))

    def test_single_ids_of_two_types_each_get_both_flags(self) -> None:
        lotr_row = _row(external_id="A1", id_type=IdentifierType.US_OZ_LOTR_ID)
        kds_row = _row(external_id="K1", id_type=IdentifierType.US_OZ_KDS_PERSON_ID)

        resolve_display_and_stable_id_flags(
            tenant=Tenant.US_OZ, external_id_rows=[lotr_row, kds_row]
        )

        self.assertEqual((True, True), _flags(lotr_row))
        self.assertEqual((True, True), _flags(kds_row))

    def test_multiple_ids_of_type_use_shared_selection_strategy(self) -> None:
        """US_OZ identity person ids select alphabetically: the highest is the
        display id and the lowest the stable id."""
        low_row = _row(
            external_id="P001", id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID
        )
        high_row = _row(
            external_id="P002", id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID
        )

        resolve_display_and_stable_id_flags(
            tenant=Tenant.US_OZ, external_id_rows=[low_row, high_row]
        )

        self.assertEqual((True, False), _flags(high_row))
        self.assertEqual((False, True), _flags(low_row))

    def test_multiple_ids_of_type_without_strategy_raise(self) -> None:
        rows = [
            _row(external_id="A1", id_type=IdentifierType.US_OZ_LOTR_ID),
            _row(external_id="B2", id_type=IdentifierType.US_OZ_LOTR_ID),
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"^No shared display/stable selection strategy for tenant \[US_OZ\] "
            r"and id_type \[US_OZ_LOTR_ID\]",
        ):
            resolve_display_and_stable_id_flags(
                tenant=Tenant.US_OZ, external_id_rows=rows
            )

    def test_inactive_ids_are_never_flagged_and_do_not_compete(self) -> None:
        """The inactive P009 would win the alphabetical display selection if it
        were considered; the active P001 must win both flags instead."""
        active_row = _row(
            external_id="P001", id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID
        )
        inactive_row = _row(
            external_id="P009",
            id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID,
            is_active=False,
        )

        resolve_display_and_stable_id_flags(
            tenant=Tenant.US_OZ, external_id_rows=[active_row, inactive_row]
        )

        self.assertEqual((True, True), _flags(active_row))
        self.assertEqual((False, False), _flags(inactive_row))

    def test_resolving_clears_flags_a_new_id_displaces(self) -> None:
        """P001 arrived alone and carried both flags; once P002 joins, P002
        takes the display flag and P001 keeps only the stable flag."""
        old_row = _row(
            external_id="P001",
            id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID,
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
        )
        new_row = _row(
            external_id="P002", id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID
        )

        resolve_display_and_stable_id_flags(
            tenant=Tenant.US_OZ, external_id_rows=[old_row, new_row]
        )

        self.assertEqual((False, True), _flags(old_row))
        self.assertEqual((True, False), _flags(new_row))

    def test_inactive_id_carrying_stale_flags_is_cleared(self) -> None:
        """A split deactivated P009 while it carried both flags; resolving
        moves them to the remaining active id."""
        inactive_row = _row(
            external_id="P009",
            id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID,
            is_active=False,
            is_current_display_id_for_type=True,
            is_stable_id_for_type=True,
        )
        active_row = _row(
            external_id="P001", id_type=IdentifierType.US_OZ_IDENTITY_PERSON_ID
        )

        resolve_display_and_stable_id_flags(
            tenant=Tenant.US_OZ, external_id_rows=[inactive_row, active_row]
        )

        self.assertEqual((False, False), _flags(inactive_row))
        self.assertEqual((True, True), _flags(active_row))
