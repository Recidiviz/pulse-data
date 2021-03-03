# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for converting state bonds."""
import unittest
from datetime import date

import attr

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import state_bond

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateBondConverterTest(unittest.TestCase):
    """Tests for converting state bonds."""

    def testParseStateBond(self) -> None:
        # Arrange
        ingest_bond = ingest_info_pb2.StateBond(
            status="ACTIVE",
            bond_type="CASH",
            state_bond_id="BOND_ID",
            date_paid="1/2/2111",
            state_code="us_nd",
            county_code="BISMARCK",
            bond_agent="AGENT",
            amount="$125.00",
        )

        # Act
        result = state_bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StateBond(
            status=BondStatus.SET,
            status_raw_text="ACTIVE",
            bond_type=BondType.CASH,
            bond_type_raw_text="CASH",
            external_id="BOND_ID",
            date_paid=date(year=2111, month=1, day=2),
            state_code="US_ND",
            county_code="BISMARCK",
            bond_agent="AGENT",
            amount_dollars=125,
        )

        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsNoBond_UsesAmountAsType(self) -> None:
        # Arrange
        ingest_bond = ingest_info_pb2.StateBond(state_code="us_xx", amount="No Bond")

        # Act
        result = state_bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StateBond.new_with_defaults(
            state_code="US_XX", bond_type=BondType.DENIED, status=BondStatus.SET
        )
        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsBondDenied_UsesAmountAsType(self) -> None:
        # Arrange
        ingest_bond = ingest_info_pb2.StateBond(
            state_code="us_xx", amount="Bond Denied"
        )

        # Act
        result = state_bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StateBond.new_with_defaults(
            state_code="US_XX", bond_type=BondType.DENIED, status=BondStatus.SET
        )
        self.assertEqual(result, expected_result)

    def testParseBond_MapStatusToType(self) -> None:
        # Arrange
        ingest_bond = ingest_info_pb2.StateBond(
            state_code="us_xx", bond_type="bond revoked"
        )
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("BOND REVOKED", BondStatus.REVOKED, BondType)
        overrides = overrides_builder.build()

        # Act
        result = state_bond.convert(
            ingest_bond, attr.evolve(_EMPTY_METADATA, enum_overrides=overrides)
        )

        # Assert
        expected_result = entities.StateBond.new_with_defaults(
            state_code="US_XX",
            bond_type_raw_text="BOND REVOKED",
            status=BondStatus.REVOKED,
        )
        self.assertEqual(result, expected_result)

    def testParseBond_OnlyAmount_InfersCash(self) -> None:
        # Arrange
        ingest_bond = ingest_info_pb2.StateBond(state_code="us_xx", amount="1,500")

        # Act
        result = state_bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StateBond.new_with_defaults(
            state_code="US_XX",
            bond_type=BondType.CASH,
            status=BondStatus.PRESENT_WITHOUT_INFO,
            amount_dollars=1500,
        )
        self.assertEqual(result, expected_result)
