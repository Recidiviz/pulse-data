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

from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationFacilitySecurityLevel, \
    StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateSpecializedPurposeForIncarceration
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.common.str_field_utils import parse_date, normalize
from recidiviz.ingest.models.ingest_info_pb2 import StateIncarcerationPeriod
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.ingest_info_converter.utils.converter_utils import \
    fn, parse_external_id, parse_region_code_with_override
from recidiviz.persistence.ingest_info_converter.utils.enum_mappings import \
    EnumMappings


def copy_fields_to_builder(
        incarceration_period_builder: entities.StateIncarcerationPeriod.Builder,
        proto: StateIncarcerationPeriod,
        metadata: IngestMetadata) -> None:
    """Mutates the provided |incarceration_period_builder| by converting an ingest_info proto StateIncarcerationPeriod.

    Note: This will not copy children into the Builder!
    """
    new = incarceration_period_builder


    # 1-to-1 mappings
    new.external_id = fn(parse_external_id, 'state_incarceration_period_id', proto)

    new.admission_date = fn(parse_date, 'admission_date', proto)
    new.release_date = fn(parse_date, 'release_date', proto)
    new.state_code = parse_region_code_with_override(proto, 'state_code', metadata)
    new.county_code = fn(normalize, 'county_code', proto)
    new.facility = fn(normalize, 'facility', proto)
    new.housing_unit = fn(normalize, 'housing_unit', proto)

    enum_fields = {
        'status': StateIncarcerationPeriodStatus,
        'incarceration_type': StateIncarcerationType,
        'facility_security_level': StateIncarcerationFacilitySecurityLevel,
        'admission_reason': StateIncarcerationPeriodAdmissionReason,
        'projected_release_reason': StateIncarcerationPeriodReleaseReason,
        'release_reason': StateIncarcerationPeriodReleaseReason,
        'specialized_purpose_for_incarceration': StateSpecializedPurposeForIncarceration,
    }

    enum_mappings = EnumMappings(proto, enum_fields, metadata.enum_overrides)

    # enum values
    # Status default based on presence of admission/release dates
    if new.release_date:
        status_default = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY
    elif new.admission_date and not new.release_date:
        status_default = StateIncarcerationPeriodStatus.IN_CUSTODY
    else:
        status_default = StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
    new.status = enum_mappings.get(StateIncarcerationPeriodStatus, default=status_default)

    new.status_raw_text = fn(normalize, 'status', proto)
    new.incarceration_type = enum_mappings.get(StateIncarcerationType, default=StateIncarcerationType.STATE_PRISON)
    new.incarceration_type_raw_text = fn(normalize, 'incarceration_type', proto)
    new.facility_security_level = enum_mappings.get(StateIncarcerationFacilitySecurityLevel)
    new.facility_security_level_raw_text = fn(normalize, 'facility_security_level', proto)
    new.admission_reason = enum_mappings.get(StateIncarcerationPeriodAdmissionReason)
    new.admission_reason_raw_text = fn(normalize, 'admission_reason', proto)
    new.projected_release_reason = enum_mappings.get(StateIncarcerationPeriodReleaseReason,
                                                     field_name='projected_release_reason')
    new.projected_release_reason_raw_text = fn(normalize, 'projected_release_reason', proto)
    new.release_reason = enum_mappings.get(StateIncarcerationPeriodReleaseReason, field_name='release_reason')

    # Assumes that incarceration periods with either a admission or release reason of TEMPORARY_CUSTODY represent a
    # temporary hold period. Temporary hold periods must:
    # - start with an admission_reason of either TRANSFER or TEMPORARY_CUSTODY
    # - if a release_reason is present, end with a release_reason of either TRANSFER or RELEASE_FROM_TEMPORARY_CUSTODY.
    if new.admission_reason == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY:
        if new.release_reason and new.release_reason not in (
                StateIncarcerationPeriodReleaseReason.TRANSFER,
                StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY):
            new.release_reason = StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY

    if new.release_reason == StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY:
        if new.admission_reason not in (
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY):
            new.admission_reason = StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY

    new.release_reason_raw_text = fn(normalize, 'release_reason', proto)

    new.specialized_purpose_for_incarceration = enum_mappings.get(
        StateSpecializedPurposeForIncarceration, field_name='specialized_purpose_for_incarceration')
    new.specialized_purpose_for_incarceration_raw_text = fn(normalize, 'specialized_purpose_for_incarceration', proto)
