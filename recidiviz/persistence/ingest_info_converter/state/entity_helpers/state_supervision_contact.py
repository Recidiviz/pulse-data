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

"""Converts an ingest_info proto StateSupervisionContact to a persistence entity."""

from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_bool, parse_date
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionContact
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    supervision_contact_builder: entities.StateSupervisionContact.Builder,
    proto: StateSupervisionContact,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |supervision_contact_builder| by converting an ingest_info proto StateSupervisionContact.

    Note: This will not copy children into the Builder!
    """
    new = supervision_contact_builder

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_supervision_contact_id", proto)

    new.contact_date = fn(parse_date, "contact_date", proto)
    new.resulted_in_arrest = fn(parse_bool, "resulted_in_arrest", proto)
    new.verified_employment = fn(parse_bool, "verified_employment", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)

    enum_fields = {
        "status": StateSupervisionContactStatus,
        "contact_reason": StateSupervisionContactReason,
        "contact_type": StateSupervisionContactType,
        "location": StateSupervisionContactLocation,
        "contact_method": StateSupervisionContactMethod,
    }

    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.status = enum_mappings.get(StateSupervisionContactStatus)
    new.status_raw_text = fn(normalize, "status", proto)
    new.contact_type = enum_mappings.get(StateSupervisionContactType)
    new.contact_type_raw_text = fn(normalize, "contact_type", proto)
    new.contact_reason = enum_mappings.get(StateSupervisionContactReason)
    new.contact_reason_raw_text = fn(normalize, "contact_reason", proto)
    new.location = enum_mappings.get(StateSupervisionContactLocation)
    new.location_raw_text = fn(normalize, "location", proto)
    new.contact_method = enum_mappings.get(StateSupervisionContactMethod)
    new.contact_method_raw_text = fn(normalize, "contact_method", proto)
