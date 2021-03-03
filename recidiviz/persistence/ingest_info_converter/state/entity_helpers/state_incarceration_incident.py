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

from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_date

from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationIncident
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


def copy_fields_to_builder(
    state_incarceration_incident_builder: entities.StateIncarcerationIncident.Builder,
    proto: StateIncarcerationIncident,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |state_incarceration_incident_builder| by converting
    an ingest_info proto StateIncarcerationIncident.

    Note: This will not copy children into the Builder!
    """
    new = state_incarceration_incident_builder

    enum_fields = {
        "incident_type": StateIncarcerationIncidentType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.incident_type = enum_mappings.get(StateIncarcerationIncidentType)
    new.incident_type_raw_text = fn(normalize, "incident_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_incarceration_incident_id", proto)
    new.incident_date = fn(parse_date, "incident_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.facility = fn(normalize, "facility", proto)
    new.location_within_facility = fn(normalize, "location_within_facility", proto)
    new.incident_details = fn(normalize, "incident_details", proto)
