# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import bond


class BondConverterTest(unittest.TestCase):
    """Tests for converting bonds."""

    def testParseBond(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(
            bond_id='BOND_ID',
            bond_type='CASH',
            amount='$125.00',
            status='ACTIVE'
        )

        # Act
        result = bond.convert(ingest_bond)

        # Assert
        expected_result = entities.Bond(
            external_id='BOND_ID',
            bond_type=BondType.CASH,
            amount_dollars=125,
            status=BondStatus.ACTIVE
        )

        self.assertEqual(result, expected_result)

    def testParseBond_SetsDefaults(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond()

        # Act
        result = bond.convert(ingest_bond)

        # Assert
        expected_result = entities.Bond(status=BondStatus.POSTED)
        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsNoBond_SetsAmountToZero(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(amount='No Bond')

        # Act
        result = bond.convert(ingest_bond)

        # Assert
        expected_result = entities.Bond(
            amount_dollars=0,
            status=BondStatus.POSTED
        )
        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsBondDenied_SetsAmountToZero(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(amount='Bond Denied')

        # Act
        result = bond.convert(ingest_bond)

        # Assert
        expected_result = entities.Bond(
            amount_dollars=0,
            status=BondStatus.POSTED
        )
        self.assertEqual(result, expected_result)
