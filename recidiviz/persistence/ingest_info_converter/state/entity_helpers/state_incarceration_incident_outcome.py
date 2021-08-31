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
"""Converts an ingest_info proto StateIncarcerationIncidentOutcome to a
persistence entity.
"""
from recidiviz.common import common_utils
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationIncidentOutcome
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateIncarcerationIncidentOutcomeFactory,
)


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def convert(
    proto: StateIncarcerationIncidentOutcome, metadata: IngestMetadata
) -> entities.StateIncarcerationIncidentOutcome:
    """Converts an ingest_info proto StateIncarcerationIncidentOutcome to a
    persistence entity.
    """
    new = entities.StateIncarcerationIncidentOutcome.builder()

    # enum values
    new.outcome_type = EnumParser(
        getattr(proto, "outcome_type"),
        StateIncarcerationIncidentOutcomeType,
        metadata.enum_overrides,
    )
    new.outcome_type_raw_text = getattr(proto, "outcome_type")

    # 1-to-1 mappings
    state_incarceration_incident_outcome_id = getattr(
        proto, "state_incarceration_incident_outcome_id"
    )
    new.external_id = (
        None
        if common_utils.is_generated_id(state_incarceration_incident_outcome_id)
        else state_incarceration_incident_outcome_id
    )
    new.date_effective = getattr(proto, "date_effective")
    new.hearing_date = getattr(proto, "hearing_date")
    new.report_date = getattr(proto, "report_date")
    new.state_code = metadata.region
    new.outcome_description = getattr(proto, "outcome_description")
    new.punishment_length_days = getattr(proto, "punishment_length_days")

    return new.build(StateIncarcerationIncidentOutcomeFactory.deserialize)
