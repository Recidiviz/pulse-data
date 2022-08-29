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
"""Converts an ingest_info proto StateSupervisionViolatedConditionEntry to a
persistence entity."""

from recidiviz.common.ingest_metadata import LegacyStateIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import (
    StateSupervisionViolatedConditionEntry,
)
from recidiviz.persistence.entity.state import entities

# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateSupervisionViolatedConditionEntryFactory,
)


def convert(
    proto: StateSupervisionViolatedConditionEntry,
    metadata: LegacyStateIngestMetadata,
) -> entities.StateSupervisionViolatedConditionEntry:
    """Converts an ingest_info proto StateSupervisionViolatedConditionEntry to a
    persistence entity."""
    new = entities.StateSupervisionViolatedConditionEntry.builder()

    # 1-to-1 mappings
    new.condition = getattr(proto, "condition")
    new.state_code = metadata.region

    return new.build(StateSupervisionViolatedConditionEntryFactory.deserialize)
