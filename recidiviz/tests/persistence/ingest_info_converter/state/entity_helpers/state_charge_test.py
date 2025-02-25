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
"""Tests for converting state charges."""

import unittest
from datetime import date

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_charge import \
    StateChargeClassificationType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_charge

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateChargeConverterTest(unittest.TestCase):
    """Tests for converting charges."""

    def testParseStateCharge(self):
        # Arrange
        ingest_charge = ingest_info_pb2.StateCharge(
            status='ACQUITTED',
            classification_type='FELONY',
            classification_subtype='AA',
            state_charge_id='CHARGE_ID',
            offense_date='1/2/2111',
            date_charged='1/10/2111',
            state_code='us_nd',
            statute='ab54.21c',
            description='CONSPIRACY',
            attempted='False',
            counts='4',
            charge_notes="Have I told you about that time I saw Janelle Monae "
                         "open for of Montreal at the 9:30 Club?",
            is_controlling='True',
            charging_entity='SCOTUS'
        )

        # Act
        charge_builder = entities.StateCharge.builder()
        state_charge.copy_fields_to_builder(
            charge_builder, ingest_charge, _EMPTY_METADATA)
        result = charge_builder.build()

        # Assert
        expected_result = entities.StateCharge.new_with_defaults(
            status=ChargeStatus.ACQUITTED,
            status_raw_text='ACQUITTED',
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text='FELONY',
            classification_subtype='AA',
            external_id='CHARGE_ID',
            offense_date=date(year=2111, month=1, day=2),
            date_charged=date(year=2111, month=1, day=10),
            state_code='US_ND',
            statute='AB54.21C',
            description='CONSPIRACY',
            attempted=False,
            counts=4,
            charge_notes="HAVE I TOLD YOU ABOUT THAT TIME I SAW JANELLE MONAE "
                         "OPEN FOR OF MONTREAL AT THE 9:30 CLUB?",
            is_controlling=True,
            charging_entity='SCOTUS'
        )

        self.assertEqual(result, expected_result)
