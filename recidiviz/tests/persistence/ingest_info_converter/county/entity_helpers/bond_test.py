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
"""Tests for converting bonds."""
import unittest

import attr

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.ingest_info_converter.county.entity_helpers import bond
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_EMPTY_METADATA = FakeIngestMetadata.for_county("us_xx_county")


class BondConverterTest(unittest.TestCase):
    """Tests for converting bonds."""

    def testParseBond(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(
            bond_id="BOND_ID",
            bond_type="CASH",
            amount="$125.00",
            status="ACTIVE",
            bond_agent="AGENT",
        )

        # Act
        result = bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Bond(
            external_id="BOND_ID",
            bond_type=BondType.CASH,
            bond_type_raw_text="CASH",
            amount_dollars=125,
            status=BondStatus.SET,
            status_raw_text="ACTIVE",
            bond_agent="AGENT",
        )

        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsNoBond_UsesAmountAsType(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(amount="No Bond")

        # Act
        result = bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Bond.new_with_defaults(
            bond_type=BondType.DENIED, status=BondStatus.SET
        )
        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsBondDenied_UsesAmountAsType(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(amount="Bond Denied")

        # Act
        result = bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Bond.new_with_defaults(
            bond_type=BondType.DENIED, status=BondStatus.SET
        )
        self.assertEqual(result, expected_result)

    def testParseBond_MapStatusToType(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(bond_type="bond revoked")
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add("BOND REVOKED", BondStatus.REVOKED, BondType)
        overrides = overrides_builder.build()

        # Act
        result = bond.convert(
            ingest_bond, attr.evolve(_EMPTY_METADATA, enum_overrides=overrides)
        )

        # Assert
        expected_result = entities.Bond.new_with_defaults(
            bond_type_raw_text="BOND REVOKED", status=BondStatus.REVOKED
        )
        self.assertEqual(result, expected_result)

    def testParseBond_OnlyAmount_InfersCash(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(amount="1,500")

        # Act
        result = bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Bond.new_with_defaults(
            bond_type=BondType.CASH,
            status=BondStatus.PRESENT_WITHOUT_INFO,
            amount_dollars=1500,
        )
        self.assertEqual(result, expected_result)
