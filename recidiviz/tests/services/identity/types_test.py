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
"""Tests for the Identity Service domain types and their field validators."""
import datetime
import unittest
import uuid

import attr

from recidiviz.common.constants.identity import IdentityStatus, PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.services.identity.types import Identity

_UTC = datetime.timezone.utc
_TS = datetime.datetime(2026, 1, 1, 12, 0, tzinfo=_UTC)
_RECIDIVIZ_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
_MERGED_INTO_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")


def _active_identity() -> Identity:
    return Identity(
        recidiviz_id=_RECIDIVIZ_ID,
        tenant=Tenant.US_OZ,
        person_type=PersonType.JII,
        status=IdentityStatus.ACTIVE,
        merged_into=None,
        created_utc=_TS,
        last_updated_utc=_TS,
    )


class IdentityDomainTypesTest(unittest.TestCase):
    """Tests that the Identity domain type validates its fields and invariants."""

    def test_active_identity_constructs(self) -> None:
        identity = _active_identity()
        self.assertEqual(_RECIDIVIZ_ID, identity.recidiviz_id)
        self.assertIsNone(identity.merged_into)
        # The UTC contract holds through to the domain object.
        self.assertIs(datetime.timezone.utc, identity.created_utc.tzinfo)

    def test_retired_identity_constructs(self) -> None:
        identity = attr.evolve(
            _active_identity(),
            status=IdentityStatus.RETIRED,
            merged_into=_MERGED_INTO_ID,
        )
        self.assertIs(IdentityStatus.RETIRED, identity.status)
        self.assertEqual(_MERGED_INTO_ID, identity.merged_into)

    def test_rejects_naive_datetime(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Expected timezone value to not be empty"
        ):
            attr.evolve(
                _active_identity(),
                created_utc=datetime.datetime(2026, 1, 1, 12, 0),
            )

    def test_rejects_non_utc_datetime(self) -> None:
        plus_five = datetime.timezone(datetime.timedelta(hours=5))
        with self.assertRaisesRegex(ValueError, r"Expected timezone value to be UTC"):
            attr.evolve(
                _active_identity(),
                created_utc=datetime.datetime(2026, 1, 1, 12, 0, tzinfo=plus_five),
            )

    def test_rejects_wrong_typed_enum_field(self) -> None:
        # A bare string where an IdentityStatus is required must be rejected
        with self.assertRaises(TypeError):
            attr.evolve(_active_identity(), status="ACTIVE")  # type: ignore[arg-type]

    def test_rejects_retired_without_merged_into(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"has status RETIRED but no merged_into"
        ):
            attr.evolve(_active_identity(), status=IdentityStatus.RETIRED)

    def test_rejects_merged_into_on_non_retired_identity(self) -> None:
        with self.assertRaisesRegex(ValueError, r"is not RETIRED"):
            attr.evolve(_active_identity(), merged_into=_MERGED_INTO_ID)
