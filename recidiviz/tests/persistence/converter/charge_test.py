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
"""Tests for converting charges."""

import unittest

from recidiviz.common.constants.charge import ChargeDegree, ChargeClass, \
    CourtType, ChargeStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import charge


_EMPTY_METADATA = IngestMetadata.new_with_none_defaults()


class ChargeConverterTest(unittest.TestCase):
    """Tests for converting charges."""

    def testParseCharge(self):
        # Arrange
        ingest_charge = ingest_info_pb2.Charge(
            charge_id='CHARGE_ID',
            attempted='True',
            degree='FIRST',
            charge_class='FELONY',
            status='DROPPED',
            court_type='DISTRICT',
        )

        # Act
        result = charge.convert(ingest_charge, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Charge(
            external_id='CHARGE_ID',
            attempted=True,
            degree=ChargeDegree.FIRST,
            charge_class=ChargeClass.FELONY,
            status=ChargeStatus.DROPPED,
            court_type=CourtType.DISTRICT,
        )

        self.assertEqual(result, expected_result)

    def testParseCharge_SetsDefaults(self):
        # Arrange
        ingest_charge = ingest_info_pb2.Charge()

        # Act
        result = charge.convert(ingest_charge, _EMPTY_METADATA)

        # Assert
        expected_result = entities.Charge(status=ChargeStatus.PENDING)
        self.assertEqual(result, expected_result)
