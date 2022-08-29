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
"""Converts an ingest_info proto
StateSupervisionViolationResponseDecisionEntry to a persistence entity.
"""
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
)
from recidiviz.common.ingest_metadata import LegacyStateIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import (
    StateSupervisionViolationResponseDecisionEntry,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateSupervisionViolationResponseDecisionEntryFactory,
)


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def convert(
    proto: StateSupervisionViolationResponseDecisionEntry,
    metadata: LegacyStateIngestMetadata,
) -> entities.StateSupervisionViolationResponseDecisionEntry:
    """Converts an ingest_info proto
    StateSupervisionViolationResponseDecisionEntry to a persistence entity.
    """
    new = entities.StateSupervisionViolationResponseDecisionEntry.builder()

    # Enum mappings
    new.decision = DefaultingAndNormalizingEnumParser(
        getattr(proto, "decision"),
        StateSupervisionViolationResponseDecision,
        metadata.enum_overrides,
    )
    new.decision_raw_text = getattr(proto, "decision")

    # 1-to-1 mappings
    new.state_code = metadata.region

    return new.build(StateSupervisionViolationResponseDecisionEntryFactory.deserialize)
