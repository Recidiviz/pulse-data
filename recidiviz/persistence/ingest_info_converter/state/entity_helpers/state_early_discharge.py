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
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.shared_enums import StateActingBodyType
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.ingest_metadata import LegacyStateAndJailsIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateEarlyDischarge
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    early_discharge_builder: entities.StateEarlyDischarge.Builder,
    proto: StateEarlyDischarge,
    metadata: LegacyStateAndJailsIngestMetadata,
) -> None:
    """Converts an ingest_info proto StateEarlyDischarge to a early discharge entity."""
    new = early_discharge_builder

    # enum values
    new.decision = DefaultingAndNormalizingEnumParser(
        getattr(proto, "decision"),
        StateEarlyDischargeDecision,
        metadata.enum_overrides,
    )
    new.decision_raw_text = getattr(proto, "decision")
    new.decision_status = DefaultingAndNormalizingEnumParser(
        getattr(proto, "decision_status"),
        StateEarlyDischargeDecisionStatus,
        metadata.enum_overrides,
    )
    new.decision_status_raw_text = getattr(proto, "decision_status")
    new.deciding_body_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "deciding_body_type"),
        StateActingBodyType,
        metadata.enum_overrides,
    )
    new.deciding_body_type_raw_text = getattr(proto, "deciding_body_type")
    new.requesting_body_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "requesting_body_type"),
        StateActingBodyType,
        metadata.enum_overrides,
    )
    new.requesting_body_type_raw_text = getattr(proto, "requesting_body_type")

    # 1-to-1 mappings

    state_early_discharge_id = getattr(proto, "state_early_discharge_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_early_discharge_id)
        else state_early_discharge_id
    )
    new.request_date = getattr(proto, "request_date")
    new.decision_date = getattr(proto, "decision_date")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
