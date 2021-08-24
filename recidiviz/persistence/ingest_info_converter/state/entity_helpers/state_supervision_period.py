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
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import normalize, parse_date
from recidiviz.ingest.models.ingest_info_pb2 import StateSupervisionPeriod
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import (
    create_comma_separated_list,
    fn,
    parse_external_id,
    parse_region_code_with_override,
)
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import EnumMappings


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    supervision_period_builder: entities.StateSupervisionPeriod.Builder,
    proto: StateSupervisionPeriod,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |supervision_period_builder| by converting an ingest_info proto StateSupervisionPeriod.

    Note: This will not copy children into the Builder!
    """
    new = supervision_period_builder

    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, "state_supervision_period_id", proto)

    new.start_date = fn(parse_date, "start_date", proto)
    new.termination_date = fn(parse_date, "termination_date", proto)
    new.state_code = parse_region_code_with_override(proto, "state_code", metadata)
    new.county_code = fn(normalize, "county_code", proto)
    new.supervision_site = fn(normalize, "supervision_site", proto)
    if proto.conditions:
        new.conditions = create_comma_separated_list(proto, "conditions")

    enum_fields = {
        "status": StateSupervisionPeriodStatus,
        "supervision_type": StateSupervisionType,
        "supervision_period_supervision_type": StateSupervisionPeriodSupervisionType,
        "admission_reason": StateSupervisionPeriodAdmissionReason,
        "termination_reason": StateSupervisionPeriodTerminationReason,
        "supervision_level": StateSupervisionLevel,
        "custodial_authority": StateCustodialAuthority,
    }
    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    # Status default based on presence of admission/release dates
    if new.termination_date:
        status_default = StateSupervisionPeriodStatus.TERMINATED
    elif new.start_date and not new.termination_date:
        status_default = StateSupervisionPeriodStatus.UNDER_SUPERVISION
    else:
        status_default = StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
    new.status = enum_mappings.get(StateSupervisionPeriodStatus, default=status_default)

    new.status_raw_text = fn(normalize, "status", proto)
    new.supervision_type = enum_mappings.get(StateSupervisionType)
    new.supervision_type_raw_text = fn(normalize, "supervision_type", proto)
    new.supervision_period_supervision_type = enum_mappings.get(
        StateSupervisionPeriodSupervisionType
    )
    new.supervision_period_supervision_type_raw_text = fn(
        normalize, "supervision_period_supervision_type", proto
    )
    new.admission_reason = enum_mappings.get(StateSupervisionPeriodAdmissionReason)
    new.admission_reason_raw_text = fn(normalize, "admission_reason", proto)
    new.termination_reason = enum_mappings.get(StateSupervisionPeriodTerminationReason)
    new.termination_reason_raw_text = fn(normalize, "termination_reason", proto)
    new.supervision_level = enum_mappings.get(StateSupervisionLevel)
    new.supervision_level_raw_text = fn(normalize, "supervision_level", proto)
    new.custodial_authority = enum_mappings.get(StateCustodialAuthority)
    new.custodial_authority_raw_text = fn(normalize, "custodial_authority", proto)
