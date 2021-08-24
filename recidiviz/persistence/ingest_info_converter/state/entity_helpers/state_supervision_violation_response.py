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
# ============================================================================

"""Converts an ingest_info proto StateSupervisionViolationResponse to a
persistence entity."""

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseRevocationType,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_bool, parse_date
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionViolationResponse
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
    supervision_violation_response_builder: entities.StateSupervisionViolationResponse.Builder,
    proto: StateSupervisionViolationResponse,
    metadata: IngestMetadata,
) -> None:
    """Converts an ingest_info proto StateSupervisionViolationResponse to a
    persistence entity."""
    new = supervision_violation_response_builder

    enum_fields = {
        "response_type": StateSupervisionViolationResponseType,
        "decision": StateSupervisionViolationResponseDecision,
        "revocation_type": StateSupervisionViolationResponseRevocationType,
        "deciding_body_type": StateSupervisionViolationResponseDecidingBodyType,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    new.response_type = enum_mappings.get(StateSupervisionViolationResponseType)
    new.response_type_raw_text = fn(normalize, "response_type", proto)
    new.response_subtype = fn(normalize, "response_subtype", proto)
    new.decision = enum_mappings.get(StateSupervisionViolationResponseDecision)
    new.decision_raw_text = fn(normalize, "decision", proto)
    new.revocation_type = enum_mappings.get(
        StateSupervisionViolationResponseRevocationType
    )
    new.revocation_type_raw_text = fn(normalize, "revocation_type", proto)
    new.deciding_body_type = enum_mappings.get(
        StateSupervisionViolationResponseDecidingBodyType
    )
    new.deciding_body_type_raw_text = fn(normalize, "deciding_body_type", proto)

    # 1-to-1 mappings
    new.external_id = fn(
        parse_external_id, "state_supervision_violation_response_id", proto
    )
    new.response_date = fn(parse_date, "response_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.is_draft = fn(parse_bool, "is_draft", proto)
