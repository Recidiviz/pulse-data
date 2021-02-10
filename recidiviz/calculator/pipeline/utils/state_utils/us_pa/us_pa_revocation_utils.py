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
"""Utils for state-specific logic related to revocations in US_PA."""
from typing import Optional, Tuple, List

from recidiviz.calculator.pipeline.utils.supervision_period_utils import \
    get_relevant_supervision_periods_before_admission_date
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod, StateIncarcerationPeriod


def us_pa_get_pre_revocation_supervision_type(
        revoked_supervision_period: Optional[StateSupervisionPeriod]) -> \
        Optional[StateSupervisionPeriodSupervisionType]:
    """Returns the supervision_period_supervision_type associated with the given revoked_supervision_period,
    if present. If not, returns None."""
    if revoked_supervision_period:
        return revoked_supervision_period.supervision_period_supervision_type
    return None


def us_pa_is_revocation_admission(incarceration_period: StateIncarcerationPeriod) -> bool:
    """Determines whether the admission to incarceration is due to a revocation of supervision using the
    admission_reason on the |incarceration_period|."""
    # TODO(#4821): Remove this logic for US_PA once Lantern is ready for PVC revocations
    if incarceration_period.specialized_purpose_for_incarceration_raw_text == 'CCIS-26':
        return False

    # These are the only two valid revocation admission reasons in US_PA
    return (incarceration_period.admission_reason in
            (StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
             StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION))


def us_pa_revoked_supervision_periods_if_revocation_occurred(incarceration_period: StateIncarcerationPeriod,
                                                             supervision_periods: List[StateSupervisionPeriod]) -> \
        Tuple[bool, List[StateSupervisionPeriod]]:
    """Determines whether the incarceration_period started because of a revocation of supervision. If a revocation did
    occur, finds the supervision period(s) that was revoked. If a revocation did not occur, returns False and an empty
    list.
    """
    admission_is_revocation = us_pa_is_revocation_admission(incarceration_period)

    if not admission_is_revocation:
        return False, []

    revoked_periods = get_relevant_supervision_periods_before_admission_date(incarceration_period.admission_date,
                                                                             supervision_periods)

    return admission_is_revocation, revoked_periods
