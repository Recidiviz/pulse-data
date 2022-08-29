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

"""Converts an ingest_info proto StateIncarcerationIncident to a
persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentType,
)
from recidiviz.common.ingest_metadata import LegacyStateIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationIncident
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    state_incarceration_incident_builder: entities.StateIncarcerationIncident.Builder,
    proto: StateIncarcerationIncident,
    metadata: LegacyStateIngestMetadata,
) -> None:
    """Mutates the provided |state_incarceration_incident_builder| by converting
    an ingest_info proto StateIncarcerationIncident.

    Note: This will not copy children into the Builder!
    """
    new = state_incarceration_incident_builder

    # enum values
    new.incident_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "incident_type"),
        StateIncarcerationIncidentType,
        metadata.enum_overrides,
    )
    new.incident_type_raw_text = getattr(proto, "incident_type")

    # 1-to-1 mappings

    state_incarceration_incident_id = getattr(proto, "state_incarceration_incident_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_incarceration_incident_id)
        else state_incarceration_incident_id
    )
    new.incident_date = getattr(proto, "incident_date")
    new.state_code = metadata.region
    new.facility = getattr(proto, "facility")
    new.location_within_facility = getattr(proto, "location_within_facility")
    new.incident_details = getattr(proto, "incident_details")
