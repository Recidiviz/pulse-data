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
"""Tests for the Tenant enum."""
import unittest

from recidiviz.common.constants.states import StateCode, _FakeStateCode, _RealStateCode
from recidiviz.common.constants.tenants import Tenant, _FakeTenant, _RealTenant


class TestTenant(unittest.TestCase):
    """Tests for the Tenant enum."""

    def test_real_tenants_cover_real_state_codes(self) -> None:
        """Every _RealStateCode must have a corresponding _RealTenant.

        If this test fails: add the missing value to _RealTenant (and
        _FakeTenant, and the TYPE_CHECKING stub).
        """
        missing = {sc.value for sc in _RealStateCode} - {t.value for t in _RealTenant}
        self.assertEqual(set(), missing)

    def test_fake_tenants_cover_fake_state_codes(self) -> None:
        """Every _FakeStateCode must have a corresponding _FakeTenant
        (including the US_DD/US_LL/US_WW/US_XX/US_YY test codes)."""
        missing = {sc.value for sc in _FakeStateCode} - {t.value for t in _FakeTenant}
        self.assertEqual(set(), missing)

    def test_to_state_code_succeeds_for_state_tenants(self) -> None:
        for tenant in Tenant:
            if not StateCode.is_state_code(tenant.value):
                continue
            self.assertEqual(tenant.value, tenant.to_state_code().value)

    def test_to_state_code_raises_for_non_state_tenants(self) -> None:
        for tenant in Tenant:
            if StateCode.is_state_code(tenant.value):
                continue
            with self.assertRaises(ValueError):
                tenant.to_state_code()

    def test_from_state_code_round_trips(self) -> None:
        for state_code in StateCode:
            self.assertEqual(
                state_code, Tenant.from_state_code(state_code).to_state_code()
            )
