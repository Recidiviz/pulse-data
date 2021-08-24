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

"""Converts an ingest_info proto StateParoleDecision to a persistence entity."""
from recidiviz.common.constants.state.state_parole_decision import (
    StateParoleDecisionOutcome,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_date
from recidiviz.ingest.models.ingest_info_pb2 import StateParoleDecision
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
    state_parole_decision_builder: entities.StateParoleDecision.Builder,
    proto: StateParoleDecision,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |state_parole_decision_builder| by converting an
    ingest_info proto StateParoleDecision.

    Note: This will not copy children into the Builder!
    """
    new = state_parole_decision_builder

    enum_fields = {
        "decision_outcome": StateParoleDecisionOutcome,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # Enum mappings
    new.decision_outcome = enum_mappings.get(StateParoleDecisionOutcome)
    new.decision_outcome_raw_text = fn(normalize, "decision_outcome", proto)

    new.external_id = fn(parse_external_id, "state_parole_decision_id", proto)
    new.decision_date = fn(parse_date, "decision_date", proto)
    new.corrective_action_deadline = fn(parse_date, "corrective_action_deadline", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
    new.decision_reasoning = fn(normalize, "decision_reasoning", proto)
    new.corrective_action = fn(normalize, "corrective_action", proto)
