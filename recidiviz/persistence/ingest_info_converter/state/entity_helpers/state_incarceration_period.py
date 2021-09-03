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

"""Converts an ingest_info proto StateIncarcerationPeriod to a
persistence entity."""
from recidiviz.common import common_utils
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationFacilitySecurityLevel,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationPeriod
from recidiviz.persistence.entity.state import entities


# TODO(#8905): Delete this file once all states have been migrated to v2 ingest
#  mappings.
def copy_fields_to_builder(
    incarceration_period_builder: entities.StateIncarcerationPeriod.Builder,
    proto: StateIncarcerationPeriod,
    metadata: IngestMetadata,
) -> None:
    """Mutates the provided |incarceration_period_builder| by converting an ingest_info proto StateIncarcerationPeriod.

    Note: This will not copy children into the Builder!
    """
    new = incarceration_period_builder

    # 1-to-1 mappings

    state_incarceration_period_id = getattr(proto, "state_incarceration_period_id")
    new.external_id = (
        None
        if common_utils.is_generated_id(state_incarceration_period_id)
        else state_incarceration_period_id
    )

    new.admission_date = getattr(proto, "admission_date")
    new.release_date = getattr(proto, "release_date")
    new.state_code = metadata.region
    new.county_code = getattr(proto, "county_code")
    new.facility = getattr(proto, "facility")
    new.housing_unit = getattr(proto, "housing_unit")

    # enum values
    new.status = DefaultingAndNormalizingEnumParser(
        getattr(proto, "status"),
        StateIncarcerationPeriodStatus,
        metadata.enum_overrides,
    )
    new.status_raw_text = getattr(proto, "status")
    new.incarceration_type = DefaultingAndNormalizingEnumParser(
        getattr(proto, "incarceration_type"),
        StateIncarcerationType,
        metadata.enum_overrides,
    )
    new.incarceration_type_raw_text = getattr(proto, "incarceration_type")
    new.facility_security_level = DefaultingAndNormalizingEnumParser(
        getattr(proto, "facility_security_level"),
        StateIncarcerationFacilitySecurityLevel,
        metadata.enum_overrides,
    )
    new.facility_security_level_raw_text = getattr(proto, "facility_security_level")
    new.admission_reason = DefaultingAndNormalizingEnumParser(
        getattr(proto, "admission_reason"),
        StateIncarcerationPeriodAdmissionReason,
        metadata.enum_overrides,
    )
    new.admission_reason_raw_text = getattr(proto, "admission_reason")
    new.projected_release_reason = DefaultingAndNormalizingEnumParser(
        getattr(proto, "projected_release_reason"),
        StateIncarcerationPeriodReleaseReason,
        metadata.enum_overrides,
    )
    new.projected_release_reason_raw_text = getattr(proto, "projected_release_reason")
    new.release_reason = DefaultingAndNormalizingEnumParser(
        getattr(proto, "release_reason"),
        StateIncarcerationPeriodReleaseReason,
        metadata.enum_overrides,
    )

    new.release_reason_raw_text = getattr(proto, "release_reason")
    new.specialized_purpose_for_incarceration = DefaultingAndNormalizingEnumParser(
        getattr(proto, "specialized_purpose_for_incarceration"),
        StateSpecializedPurposeForIncarceration,
        metadata.enum_overrides,
    )
    new.specialized_purpose_for_incarceration_raw_text = getattr(
        proto, "specialized_purpose_for_incarceration"
    )

    new.custodial_authority = DefaultingAndNormalizingEnumParser(
        getattr(proto, "custodial_authority"),
        StateCustodialAuthority,
        metadata.enum_overrides,
    )
    new.custodial_authority_raw_text = getattr(proto, "custodial_authority")
