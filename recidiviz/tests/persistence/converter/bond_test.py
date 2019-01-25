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
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import bond

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


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
        result = bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Bond(
            external_id='BOND_ID',
            bond_type=BondType.CASH,
            bond_type_raw_text='CASH',
            amount_dollars=125,
            status=BondStatus.SET,
            status_raw_text='ACTIVE',
            # TODO(745): include this in conversion when field is added to proto
            bond_agent=None,
        )

        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsNoBond_SetsAmountToZero(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(amount='No Bond')

        # Act
        result = bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Bond.new_with_defaults(
            bond_type=BondType.NO_BOND,
            status=BondStatus.UNKNOWN_FOUND_IN_SOURCE
        )
        self.assertEqual(result, expected_result)

    def testParseBond_AmountIsBondDenied_SetsAmountToZero(self):
        # Arrange
        ingest_bond = ingest_info_pb2.Bond(amount='Bond Denied')

        # Act
        result = bond.convert(ingest_bond, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Bond.new_with_defaults(
            bond_type=BondType.NO_BOND,
            status=BondStatus.DENIED
        )
        self.assertEqual(result, expected_result)
