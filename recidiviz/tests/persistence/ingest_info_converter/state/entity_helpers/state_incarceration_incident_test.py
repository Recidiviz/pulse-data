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
"""Tests for converting state incarceration incidents."""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentType,
)
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import (
    state_incarceration_incident,
)
from recidiviz.tests.persistence.database.database_test_utils import FakeIngestMetadata

_EMPTY_METADATA = FakeIngestMetadata.for_state("us_ca")


class StateIncarcerationIncidentConverterTest(unittest.TestCase):
    """Tests for converting incidents."""

    def testParseStateIncarcerationIncident(self):
        # Arrange
        ingest_incident = ingest_info_pb2.StateIncarcerationIncident(
            state_incarceration_incident_id="INCIDENT_ID",
            incident_type="CONTRABAND",
            incident_date="1/2/1111",
            state_code="us_ca",
            facility="Alcatraz",
            location_within_facility="13B",
            incident_details="Inmate was told to be quiet and would not comply",
        )

        # Act
        incident_builder = entities.StateIncarcerationIncident.builder()
        state_incarceration_incident.copy_fields_to_builder(
            incident_builder, ingest_incident, _EMPTY_METADATA
        )
        result = incident_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationIncident(
            external_id="INCIDENT_ID",
            incident_type=StateIncarcerationIncidentType.CONTRABAND,
            incident_type_raw_text="CONTRABAND",
            incident_date=date(year=1111, month=1, day=2),
            state_code="US_CA",
            facility="ALCATRAZ",
            location_within_facility="13B",
            incident_details="INMATE WAS TOLD TO BE QUIET AND WOULD NOT COMPLY",
        )

        self.assertEqual(result, expected_result)
