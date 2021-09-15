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
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionViolationResponse
from recidiviz.persistence.entity.state import entities


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

    # enum values
    new.response_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "response_type"),
        StateSupervisionViolationResponseType,
        metadata.enum_overrides,
    )
    new.response_type_raw_text = getattr(proto, "response_type")
    new.response_subtype = getattr(proto, "response_subtype")
    new.deciding_body_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "deciding_body_type"),
        StateSupervisionViolationResponseDecidingBodyType,
        metadata.enum_overrides,
    )
    new.deciding_body_type_raw_text = getattr(proto, "deciding_body_type")

    # 1-to-1 mappings

    state_supervision_violation_response_id = getattr(
        proto, "state_supervision_violation_response_id"
    )
    new.external_id = (
        None
        if common_utils.is_generated_id(state_supervision_violation_response_id)
        else state_supervision_violation_response_id
    )
    new.response_date = getattr(proto, "response_date")
    new.state_code = metadata.region
    new.is_draft = getattr(proto, "is_draft")
