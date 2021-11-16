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
"""Tests for converting state person ethnicities."""

import unittest

from recidiviz.common.constants.person_characteristics import Ethnicity
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_person_ethnicity,
)
from recidiviz.tests.persistence.database.database_test_utils import (
    FakeLegacyStateAndJailsIngestMetadata,
)

_EMPTY_METADATA = FakeLegacyStateAndJailsIngestMetadata.for_state(region="us_nd")


class StatePersonEthnicityConverterTest(unittest.TestCase):
    """Tests for converting state person ethnicities."""

    def testParseStatePersonEthnicity(self) -> None:
        # Arrange
        ingest_person_ethnicity = ingest_info_pb2.StatePersonEthnicity(
            ethnicity="HISPANIC",
            state_code="US_ND",
            state_person_ethnicity_id="123",
        )

        # Act
        result = state_person_ethnicity.convert(
            ingest_person_ethnicity, _EMPTY_METADATA
        )

        # Assert
        expected_result = entities.StatePersonEthnicity(
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text="HISPANIC",
            state_code="US_ND",
        )

        self.assertEqual(result, expected_result)
