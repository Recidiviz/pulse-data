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

"""Converts an ingest_info proto StateSupervisionPeriod to a persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.ingest_metadata import LegacyStateAndJailsIngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionPeriod
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    supervision_period_builder: entities.StateSupervisionPeriod.Builder,
    proto: StateSupervisionPeriod,
    metadata: LegacyStateAndJailsIngestMetadata,
) -> None:
    """Mutates the provided |supervision_period_builder| by converting an ingest_info proto StateSupervisionPeriod.

    Note: This will not copy children into the Builder!
    """
    new = supervision_period_builder

    # 1-to-1 mappings
    state_supervision_period_id = getattr(proto, "state_supervision_period_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_supervision_period_id)
        else state_supervision_period_id
    )

    new.start_date = getattr(proto, "start_date")
    new.termination_date = getattr(proto, "termination_date")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.supervision_site = getattr(proto, "supervision_site")
    new.conditions = getattr(proto, "conditions")

    # enum values
    new.supervision_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "supervision_type"),
        StateSupervisionPeriodSupervisionType,
        metadata.enum_overrides,
    )
    new.supervision_type_raw_text = getattr(proto, "supervision_type")
    new.admission_reason = DefaultingAndNormalizingEnumParser(
        getattr(proto, "admission_reason"),
        StateSupervisionPeriodAdmissionReason,
        metadata.enum_overrides,
    )
    new.admission_reason_raw_text = getattr(proto, "admission_reason")
    new.termination_reason = DefaultingAndNormalizingEnumParser(
        getattr(proto, "termination_reason"),
        StateSupervisionPeriodTerminationReason,
        metadata.enum_overrides,
    )
    new.termination_reason_raw_text = getattr(proto, "termination_reason")
    new.supervision_level = DefaultingAndNormalizingEnumParser(
        getattr(proto, "supervision_level"),
        StateSupervisionLevel,
        metadata.enum_overrides,
    )
    new.supervision_level_raw_text = getattr(proto, "supervision_level")
    new.custodial_authority = DefaultingAndNormalizingEnumParser(
        getattr(proto, "custodial_authority"),
        StateCustodialAuthority,
        metadata.enum_overrides,
    )
    new.custodial_authority_raw_text = getattr(proto, "custodial_authority")
