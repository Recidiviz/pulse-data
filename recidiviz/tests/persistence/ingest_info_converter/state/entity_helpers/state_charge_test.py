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

from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
)
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateChargeFactory,
)
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_charge,
)
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateIngestMetadata,
)

_EMPTY_METADATA = FakeLegacyStateIngestMetadata.for_state("us_nd")


class StateChargeConverterTest(unittest.TestCase):
    """Tests for converting charges."""

    def testParseStateCharge(self) -> None:
        # Arrange
        ingest_charge = ingest_info_pb2.StateCharge(
            status="ACQUITTED",
            classification_type="FELONY",
            classification_subtype="AA",
            offense_type="OTHER",
            is_violent="False",
            is_sex_offense="False",
            state_charge_id="CHARGE_ID",
            offense_date="1/2/2111",
            date_charged="1/10/2111",
            state_code="us_nd",
            ncic_code="4801",
            statute="ab54.21c",
            description="CONSPIRACY",
            attempted="False",
            counts="4",
            charge_notes="Have I told you about that time I saw Janelle Monae "
            "open for of Montreal at the 9:30 Club?",
            is_controlling="True",
            charging_entity="SCOTUS",
        )

        # Act
        charge_builder = entities.StateCharge.builder()
        state_charge.copy_fields_to_builder(
            charge_builder, ingest_charge, _EMPTY_METADATA
        )
        result = charge_builder.build(StateChargeFactory.deserialize)

        # Assert
        expected_result = entities.StateCharge.new_with_defaults(
            status=StateChargeStatus.ACQUITTED,
            status_raw_text="ACQUITTED",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="FELONY",
            classification_subtype="AA",
            offense_type="OTHER",
            is_violent=False,
            is_sex_offense=False,
            external_id="CHARGE_ID",
            offense_date=date(year=2111, month=1, day=2),
            date_charged=date(year=2111, month=1, day=10),
            state_code="US_ND",
            ncic_code="4801",
            statute="AB54.21C",
            description="CONSPIRACY",
            attempted=False,
            counts=4,
            charge_notes="HAVE I TOLD YOU ABOUT THAT TIME I SAW JANELLE MONAE "
            "OPEN FOR OF MONTREAL AT THE 9:30 CLUB?",
            is_controlling=True,
            charging_entity="SCOTUS",
        )

        self.assertEqual(result, expected_result)

    def testParseStateCharge_ncicCodeButNoDescription(self) -> None:
        # Arrange
        ingest_charge = ingest_info_pb2.StateCharge(
            status="ACQUITTED",
            classification_type="FELONY",
            classification_subtype="AA",
            offense_type="VIOLENT",
            is_violent="True",
            is_sex_offense="False",
            state_charge_id="CHARGE_ID",
            offense_date="1/2/2111",
            date_charged="1/10/2111",
            state_code="us_nd",
            ncic_code="4801",
            attempted="False",
        )

        # Act
        charge_builder = entities.StateCharge.builder()
        state_charge.copy_fields_to_builder(
            charge_builder, ingest_charge, _EMPTY_METADATA
        )
        result = charge_builder.build(StateChargeFactory.deserialize)

        # Assert
        expected_result = entities.StateCharge.new_with_defaults(
            status=StateChargeStatus.ACQUITTED,
            status_raw_text="ACQUITTED",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="FELONY",
            classification_subtype="AA",
            offense_type="VIOLENT",
            is_violent=True,
            is_sex_offense=False,
            external_id="CHARGE_ID",
            offense_date=date(year=2111, month=1, day=2),
            date_charged=date(year=2111, month=1, day=10),
            state_code="US_ND",
            ncic_code="4801",
            description=None,
            attempted=False,
        )

        self.assertEqual(result, expected_result)

    def testParseStateCharge_ncicCodeButNoDescription_codeNotFound(self) -> None:
        # Arrange
        ingest_charge = ingest_info_pb2.StateCharge(
            status="ACQUITTED",
            classification_type="FELONY",
            classification_subtype="AA",
            state_charge_id="CHARGE_ID",
            offense_date="1/2/2111",
            date_charged="1/10/2111",
            state_code="us_nd",
            ncic_code="9999",
            attempted="False",
        )

        # Act
        charge_builder = entities.StateCharge.builder()
        state_charge.copy_fields_to_builder(
            charge_builder, ingest_charge, _EMPTY_METADATA
        )
        result = charge_builder.build(StateChargeFactory.deserialize)

        # Assert
        expected_result = entities.StateCharge.new_with_defaults(
            status=StateChargeStatus.ACQUITTED,
            status_raw_text="ACQUITTED",
            classification_type=StateChargeClassificationType.FELONY,
            classification_type_raw_text="FELONY",
            classification_subtype="AA",
            external_id="CHARGE_ID",
            offense_date=date(year=2111, month=1, day=2),
            date_charged=date(year=2111, month=1, day=10),
            state_code="US_ND",
            ncic_code="9999",
            attempted=False,
        )

        self.assertEqual(result, expected_result)
