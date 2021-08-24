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

"""Converts an ingest_info proto StateProgramAssignment to a persistence
entity.
"""

from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_date
from recidiviz.ingest.models.ingest_info_pb2 import StateProgramAssignment
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
    state_program_assignment_builder: entities.StateProgramAssignment.Builder,
    proto: StateProgramAssignment,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |state_program_assignment_builder| by converting an
    ingest_info proto StateProgramAssignment.

    Note: This will not copy children into the Builder!
    """

    new = state_program_assignment_builder

    enum_fields = {
        "participation_status": StateProgramAssignmentParticipationStatus,
        "discharge_reason": StateProgramAssignmentDischargeReason,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum mappings
    new.participation_status = enum_mappings.get(
        StateProgramAssignmentParticipationStatus,
        default=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
    )
    new.participation_status_raw_text = fn(normalize, "participation_status", proto)
    new.discharge_reason = enum_mappings.get(StateProgramAssignmentDischargeReason)
    new.discharge_reason_raw_text = fn(normalize, "discharge_reason", proto)

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_program_assignment_id", proto)
    new.referral_date = fn(parse_date, "referral_date", proto)
    new.start_date = fn(parse_date, "start_date", proto)
    new.discharge_date = fn(parse_date, "discharge_date", proto)
    new.program_id = fn(normalize, "program_id", proto)
    new.program_location_id = fn(normalize, "program_location_id", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.referral_metadata = fn(normalize, "referral_metadata", proto)
