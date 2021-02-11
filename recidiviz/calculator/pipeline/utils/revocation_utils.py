# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Utils for calculations regarding revocations."""
from datetime import date
from typing import Optional, List, Dict, Any, NamedTuple, Tuple

from recidiviz.calculator.pipeline.utils.incarceration_period_index import IncarcerationPeriodIndex
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    get_supervising_officer_and_location_info_from_supervision_period, state_specific_revocation_type_and_subtype, \
    incarceration_period_is_from_revocation
from recidiviz.calculator.pipeline.utils.violation_utils import identify_most_severe_violation_type_and_subtype
from recidiviz.common.constants.state.state_incarceration_period import StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolationResponse


RevocationDetails = NamedTuple('RevocationDetails', [
    ('revocation_type', Optional[StateSupervisionViolationResponseRevocationType]),
    ('revocation_type_subtype', Optional[str]),
    ('source_violation_type', Optional[StateSupervisionViolationType]),
    ('supervising_officer_external_id', Optional[str]),
    ('level_1_supervision_location_external_id', Optional[str]),
    ('level_2_supervision_location_external_id', Optional[str])])


def get_revocation_details(incarceration_period: StateIncarcerationPeriod,
                           supervision_period: Optional[StateSupervisionPeriod],
                           violation_responses: List[StateSupervisionViolationResponse],
                           supervision_period_to_agent_associations:
                           Optional[Dict[int, Dict[Any, Any]]]) -> RevocationDetails:
    """Identifies the attributes of the revocation return from the supervision period that was revoked, if available,
     or the |source_supervision_violation_response| on the |incarceration_period|, if it is available.
    """
    source_violation_type = None
    supervising_officer_external_id = None
    level_1_supervision_location_external_id = None
    level_2_supervision_location_external_id = None

    if supervision_period and supervision_period_to_agent_associations:
        (supervising_officer_external_id,
         level_1_supervision_location_external_id,
         level_2_supervision_location_external_id) = \
            get_supervising_officer_and_location_info_from_supervision_period(
                supervision_period, supervision_period_to_agent_associations)

    source_violation_response = incarceration_period.source_supervision_violation_response

    if source_violation_response:
        source_violation = source_violation_response.supervision_violation
        if source_violation:
            source_violation_type, _ = identify_most_severe_violation_type_and_subtype([source_violation])

    revocation_type, revocation_type_subtype = state_specific_revocation_type_and_subtype(
        incarceration_period.state_code, incarceration_period, violation_responses,
        _identify_revocation_type_and_subtype)

    revocation_details_result = RevocationDetails(
        revocation_type,
        revocation_type_subtype,
        source_violation_type,
        supervising_officer_external_id,
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id)

    return revocation_details_result


def has_revocation_admission_on_date(
        date_in_month: date,
        incarceration_period_index: IncarcerationPeriodIndex) -> bool:
    """Returns whether or not a revocation admission occurred on the |date_in_month|."""
    incarceration_periods_by_admission_date = incarceration_period_index.incarceration_periods_by_admission_date
    if date_in_month not in incarceration_periods_by_admission_date:
        return False

    # An admission to prison happened on this day
    incarceration_periods = incarceration_periods_by_admission_date[date_in_month]
    for incarceration_period in incarceration_periods:
        # Determine if the admission to this incarceration period was due to a revocation
        ip_index = incarceration_period_index.incarceration_periods.index(incarceration_period)
        preceding_incarceration_period = None

        if ip_index > 0:
            preceding_incarceration_period = incarceration_period_index.incarceration_periods[ip_index - 1]

        if incarceration_period_is_from_revocation(incarceration_period, preceding_incarceration_period):
            return True
    return False


def _identify_revocation_type_and_subtype(incarceration_period: StateIncarcerationPeriod) -> \
        Tuple[Optional[StateSupervisionViolationResponseRevocationType], Optional[str]]:
    """Determines the revocation_type and, if applicable, the revocation_type_subtype of the revocation admission to the
    given incarceration_period."""
    specialized_purpose_for_incarceration = incarceration_period.specialized_purpose_for_incarceration

    revocation_type = None

    if specialized_purpose_for_incarceration == StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON:
        revocation_type = StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON
    elif specialized_purpose_for_incarceration == StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION:
        revocation_type = StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
    elif (specialized_purpose_for_incarceration == StateSpecializedPurposeForIncarceration.GENERAL
          or specialized_purpose_for_incarceration is None):
        # Assume no specialized_purpose_for_incarceration is a reincarceration
        revocation_type = StateSupervisionViolationResponseRevocationType.REINCARCERATION
    elif specialized_purpose_for_incarceration not in (
            StateSpecializedPurposeForIncarceration.EXTERNAL_UNKNOWN,
            StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN,
            StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
    ):
        raise ValueError("Unhandled StateSpecializedPurposeForIncarceration: "
                         f"[{specialized_purpose_for_incarceration}]")

    # For now, all non-state-specific revocation_type_subtypes are None
    return revocation_type, None
