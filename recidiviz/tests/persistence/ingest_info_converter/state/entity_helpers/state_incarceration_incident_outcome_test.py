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
"""Tests for converting StateIncarcerationIncidentOutcomes."""

import unittest
from datetime import date

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_incarceration_incident_outcome,
)


_ENUM_OVERRIDES = (
    EnumOverrides.Builder()
    .add("LCP", StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS)
    .build()
)
_METADATA_WITH_OVERRIDES = IngestMetadata.new_with_defaults(
    enum_overrides=_ENUM_OVERRIDES
)


class StateIncarcerationIncidentOutcomeConverterTest(unittest.TestCase):
    """Tests for converting StateIncarcerationIncidentOutcomes."""

    def testParseStateIncarcerationIncident(self):
        # Arrange
        ingest_incident_outcome = ingest_info_pb2.StateIncarcerationIncidentOutcome(
            state_incarceration_incident_outcome_id="INCIDENT_OUTCOME_ID",
            outcome_type="LCP",
            date_effective="1/2/2019",
            hearing_date="12/29/2018",
            report_date="12/30/2019",
            state_code="us_ca",
            outcome_description="Loss of Commissary Privileges",
            punishment_length_days="45",
        )

        # Act
        result = state_incarceration_incident_outcome.convert(
            ingest_incident_outcome, _METADATA_WITH_OVERRIDES
        )

        # Assert
        expected_result = entities.StateIncarcerationIncidentOutcome(
            external_id="INCIDENT_OUTCOME_ID",
            outcome_type=StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
            outcome_type_raw_text="LCP",
            date_effective=date(year=2019, month=1, day=2),
            hearing_date=date(year=2018, month=12, day=29),
            report_date=date(year=2019, month=12, day=30),
            state_code="US_CA",
            outcome_description="LOSS OF COMMISSARY PRIVILEGES",
            punishment_length_days=45,
        )

        print(result)
        print(expected_result)
        self.assertEqual(result, expected_result)
