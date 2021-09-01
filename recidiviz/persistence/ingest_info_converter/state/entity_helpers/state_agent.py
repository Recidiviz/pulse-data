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

"""Converts an ingest_info proto StateAgent to a persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateAgent
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.deserialize_entity_factories import (
    StateAgentFactory,
)
from recidiviz.persistence.ingest_info_converter.utils.names import parse_name


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def convert(proto: StateAgent, metadata: IngestMetadata) -> entities.StateAgent:
    """Converts an ingest_info proto StateAgent to a persistence entity."""
    new = entities.StateAgent.builder()

    # enum values
    new.agent_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "agent_type"), StateAgentType, metadata.enum_overrides
    )
    new.agent_type_raw_text = getattr(proto, "agent_type")

    # 1-to-1 mappings
    state_agent_id = getattr(proto, "state_agent_id")
    new.external_id = (
        None if common_utils.is_generated_id(state_agent_id) else state_agent_id
    )
    new.state_code = metadata.region
    new.full_name = parse_name(proto)

    return new.build(StateAgentFactory.deserialize)
