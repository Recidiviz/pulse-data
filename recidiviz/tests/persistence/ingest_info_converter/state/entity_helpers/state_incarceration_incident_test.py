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

from recidiviz.common.constants.state.state_incarceration_incident import \
    StateIncarcerationIncidentOffense, StateIncarcerationIncidentOutcome
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.state.entity_helpers import \
    state_incarceration_incident

_EMPTY_METADATA = IngestMetadata.new_with_defaults()


class StateIncarcerationIncidentConverterTest(unittest.TestCase):
    """Tests for converting incidents."""

    def testParseStateIncarcerationIncident(self):
        # Arrange
        ingest_incident = ingest_info_pb2.StateIncarcerationIncident(
            offense='CONTRABAND',
            outcome='WARNING',
            state_incarceration_incident_id='INCIDENT_ID',
            incident_date='1/2/1111',
            state_code='us_nd',
            county_code='089',
            location_within_facility='KITCHEN',
        )

        # Act
        incident_builder = entities.StateIncarcerationIncident.builder()
        state_incarceration_incident.copy_fields_to_builder(
            incident_builder, ingest_incident, _EMPTY_METADATA)
        result = incident_builder.build()

        # Assert
        expected_result = entities.StateIncarcerationIncident(
            offense=StateIncarcerationIncidentOffense.CONTRABAND,
            offense_raw_text='CONTRABAND',
            outcome=StateIncarcerationIncidentOutcome.WARNING,
            outcome_raw_text='WARNING',
            external_id='INCIDENT_ID',
            incident_date=date(year=1111, month=1, day=2),
            state_code='US_ND',
            county_code='089',
            location_within_facility='KITCHEN',
        )

        self.assertEqual(result, expected_result)
