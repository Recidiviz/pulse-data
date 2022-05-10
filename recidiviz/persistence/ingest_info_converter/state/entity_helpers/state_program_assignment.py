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
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.ingest_metadata import LegacyStateAndJailsIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateProgramAssignment
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    state_program_assignment_builder: entities.StateProgramAssignment.Builder,
    proto: StateProgramAssignment,
    metadata: LegacyStateAndJailsIngestMetadata,
) -> None:
    """Mutates the provided |state_program_assignment_builder| by converting an
    ingest_info proto StateProgramAssignment.

    Note: This will not copy children into the Builder!
    """

    new = state_program_assignment_builder

    # Enum mappings
    new.participation_status = DefaultingAndNormalizingEnumParser(
        getattr(proto, "participation_status"),
        StateProgramAssignmentParticipationStatus,
        metadata.enum_overrides,
    )
    new.participation_status_raw_text = getattr(proto, "participation_status")

    # 1-to-1 mappings
    state_program_assignment_id = getattr(proto, "state_program_assignment_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_program_assignment_id)
        else state_program_assignment_id
    )
    new.referral_date = getattr(proto, "referral_date")
    new.start_date = getattr(proto, "start_date")
    new.discharge_date = getattr(proto, "discharge_date")
    new.program_id = getattr(proto, "program_id")
    new.program_location_id = getattr(proto, "program_location_id")
    new.state_code = metadata.region
    new.referral_metadata = getattr(proto, "referral_metadata")
