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

"""Tests for converting state incarceration sentences."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateIncarcerationSentenceFactory,
)
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_incarceration_sentence,
)
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

METADATA = FakeIngestMetadata.for_state(region="us_nd")


class StateIncarcerationSentenceConverterTest(unittest.TestCase):
    """Tests for converting state incarceration sentences."""

    def testParseStateIncarcerationSentence(self) -> None:
        # Arrange
        ingest_incarceration = ingest_info_pb2.StateIncarcerationSentence(
            status="SUSPENDED",
            incarceration_type="STATE_PRISON",
            state_incarceration_sentence_id="INCARCERATION_ID",
            date_imposed="7/2/2006",
            start_date="1/2/2006",
            projected_min_release_date="4/2/2111",
            projected_max_release_date="7/2/2111",
            parole_eligibility_date="4/2/2111",
            county_code="CO",
            min_length="90D",
            max_length="180D",
            is_life="False",
            is_capital_punishment="False",
            parole_possible="true",
            initial_time_served="60",  # Units in days
            good_time="365",  # Units in days
            earned_time=None,
        )

        # Act
        incarceration_builder = entities.StateIncarcerationSentence.builder()
        state_incarceration_sentence.copy_fields_to_builder(
            incarceration_builder, ingest_incarceration, METADATA
        )
        result = incarceration_builder.build(
            StateIncarcerationSentenceFactory.deserialize
        )

        # Assert
        expected_result = entities.StateIncarcerationSentence(
            status=StateSentenceStatus.SUSPENDED,
            status_raw_text="SUSPENDED",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            incarceration_type_raw_text="STATE_PRISON",
            external_id="INCARCERATION_ID",
            date_imposed=date(year=2006, month=7, day=2),
            start_date=date(year=2006, month=1, day=2),
            projected_min_release_date=date(year=2111, month=4, day=2),
            projected_max_release_date=date(year=2111, month=7, day=2),
            parole_eligibility_date=date(year=2111, month=4, day=2),
            completion_date=None,
            state_code="US_ND",
            county_code="CO",
            min_length_days=90,
            max_length_days=180,
            is_life=False,
            is_capital_punishment=False,
            parole_possible=True,
            initial_time_served_days=60,
            good_time_days=365,
            earned_time_days=None,
        )

        self.assertEqual(result, expected_result)
