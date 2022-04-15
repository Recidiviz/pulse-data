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
"""Tests for converting state person races."""

import unittest

from recidiviz.common.constants.state.state_person import StateRace
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_person_race,
)
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateAndJailsIngestMetadata,
)

_EMPTY_METADATA = FakeLegacyStateAndJailsIngestMetadata.for_state(
    region="us_nd",
)


class StatePersonRaceConverterTest(unittest.TestCase):
    """Tests for converting state person races."""

    def testParseStatePersonRace(self):
        # Arrange
        ingest_person_race = ingest_info_pb2.StatePersonRace(
            race="SAMOAN",
            state_code="US_ND",
            state_person_race_id="123",
        )

        # Act
        result = state_person_race.convert(ingest_person_race, _EMPTY_METADATA)

        # Assert
        expected_result = entities.StatePersonRace(
            race=StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER,
            race_raw_text="SAMOAN",
            state_code="US_ND",
        )

        self.assertEqual(result, expected_result)
