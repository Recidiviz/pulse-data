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

"""Converts an ingest_info proto StateEarlyDischarge to a persistence entity."""
from recidiviz.common.constants.state.shared_enums import StateActingBodyType
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_date
from recidiviz.ingest.models.ingest_info_pb2 import StateEarlyDischarge
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
    early_discharge_builder: entities.StateEarlyDischarge.Builder,
    proto: StateEarlyDischarge,
    metadata: IngestMetadata,
) -> None:
    """Converts an ingest_info proto StateEarlyDischarge to a early discharge entity."""
    new = early_discharge_builder

    enum_fields = {
        "decision": StateEarlyDischargeDecision,
        "decision_status": StateEarlyDischargeDecisionStatus,
        "deciding_body_type": StateActingBodyType,
        "requesting_body_type": StateActingBodyType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.decision = enum_mappings.get(StateEarlyDischargeDecision)
    new.decision_raw_text = fn(normalize, "decision", proto)
    new.decision_status = enum_mappings.get(StateEarlyDischargeDecisionStatus)
    new.decision_status_raw_text = fn(normalize, "decision_status", proto)
    new.deciding_body_type = enum_mappings.get(
        StateActingBodyType, field_name="deciding_body_type"
    )
    new.deciding_body_type_raw_text = fn(normalize, "deciding_body_type", proto)
    new.requesting_body_type = enum_mappings.get(
        StateActingBodyType, field_name="requesting_body_type"
    )
    new.requesting_body_type_raw_text = fn(normalize, "requesting_body_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_early_discharge_id", proto)
    new.request_date = fn(parse_date, "request_date", proto)
    new.decision_date = fn(parse_date, "decision_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
