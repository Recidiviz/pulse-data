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
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_parole_decision import (
    StateParoleDecisionOutcome,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateParoleDecision
from recidiviz.persistence.entity.state import entities


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

    # Enum mappings
    new.decision_outcome = DefaultingAndNormalizingEnumParser(
        getattr(proto, "decision_outcome"),
        StateParoleDecisionOutcome,
        metadata.enum_overrides,
    )
    new.decision_outcome_raw_text = getattr(proto, "decision_outcome")

    state_parole_decision_id = getattr(proto, "state_parole_decision_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_parole_decision_id)
        else state_parole_decision_id
    )
    new.decision_date = getattr(proto, "decision_date")
    new.corrective_action_deadline = getattr(proto, "corrective_action_deadline")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.decision_reasoning = getattr(proto, "decision_reasoning")
    new.corrective_action = getattr(proto, "corrective_action")
