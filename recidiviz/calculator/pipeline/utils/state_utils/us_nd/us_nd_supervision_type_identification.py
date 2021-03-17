# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for determining supervision type information for US_ND."""
from typing import Optional, Union, List

from recidiviz.calculator.pipeline.utils.incarceration_period_index import (
    IncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.supervision_period_index import (
    SupervisionPeriodIndex,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)

# Limit the search for previous incarceration/supervision periods.
LOOKBACK_MONTHS_LIMIT = 1

RELEASE_REASON_RAW_TEXT_TO_SUPERVISION_TYPE = {
    "NPROB": StateSupervisionPeriodSupervisionType.PROBATION,
    "NPRB": StateSupervisionPeriodSupervisionType.PROBATION,
    "PRB": StateSupervisionPeriodSupervisionType.PROBATION,
    "RPRB": StateSupervisionPeriodSupervisionType.PROBATION,
    "PARL": StateSupervisionPeriodSupervisionType.PAROLE,
    "PV": StateSupervisionPeriodSupervisionType.PAROLE,
    "RPAR": StateSupervisionPeriodSupervisionType.PAROLE,
}


def us_nd_get_post_incarceration_supervision_type(
    incarceration_period: StateIncarcerationPeriod,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the post-incarceration supervision type for US_ND by evaluating the raw text fields associated
    with the release_reason.
    """
    if not incarceration_period.release_date:
        raise ValueError(
            f"No release date for incarceration period {incarceration_period.incarceration_period_id}"
        )

    if not incarceration_period.release_reason:
        raise ValueError(
            f"No release reason for incarceraation period {incarceration_period.incarceration_period_id}"
        )

    # Releases to supervision are always classified as a CONDITIONAL_RELEASE
    if (
        incarceration_period.release_reason
        != StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
    ):
        return None

    release_reason_raw_text = incarceration_period.release_reason_raw_text

    if not release_reason_raw_text:
        raise ValueError(
            f"Unexpected empty release_reason_raw_text value for incarceration period "
            f"{incarceration_period.incarceration_period_id}."
        )

    supervision_type = RELEASE_REASON_RAW_TEXT_TO_SUPERVISION_TYPE.get(
        release_reason_raw_text
    )

    if not supervision_type:
        raise ValueError(
            f"Unexpected release_reason_raw_text value {release_reason_raw_text} being mapped to"
            f" {StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE}."
        )

    return supervision_type


def us_nd_infer_supervision_period_admission(
    supervision_period: StateSupervisionPeriod,
    supervision_period_index: SupervisionPeriodIndex,
    incarceration_period_index: IncarcerationPeriodIndex,
) -> Optional[StateSupervisionPeriodAdmissionReason]:
    """Looks at the provided |supervision_period|, all supervision periods for this person via the
    |supervision_period_index|, and all incarceration periods via |incarceration_period_index| and returns the
    admission reason for this |supervision period|.
    This is necessary because we do not currently have a way to ingest ND admission reasons for supervision periods.
    """
    if not supervision_period.start_date:
        raise ValueError(
            "Found null supervision_period.start_date while inferring admission reason."
        )

    all_periods: List[Union[StateIncarcerationPeriod, StateSupervisionPeriod]] = []
    all_periods.extend(supervision_period_index.supervision_periods)
    all_periods.extend(incarceration_period_index.incarceration_periods)

    most_recent_previous_period = find_last_terminated_period_before_date(
        upper_bound_date=supervision_period.start_date,
        periods=all_periods,
        maximum_months_proximity=LOOKBACK_MONTHS_LIMIT,
    )

    if not most_recent_previous_period:
        if supervision_period.supervision_type == StateSupervisionType.PAROLE:
            # If there was not a previous period and the person is under parole supervision, the current admission
            # reason should be CONDITIONAL_RELEASE
            return StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
        if supervision_period.supervision_type == StateSupervisionType.PROBATION:
            # If there was not a previous period and the person is under PROBATION supervision, the current admission
            # reason should be COURT_SENTENCE
            return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE

    if isinstance(most_recent_previous_period, StateIncarcerationPeriod):
        if (
            most_recent_previous_period.release_reason
            == StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        ):
            # If the most recent previous incarceration period terminated in CONDITIONAL_RELEASE, set that as the
            # admission reason.
            return StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE
    if isinstance(most_recent_previous_period, StateSupervisionPeriod):
        if (
            most_recent_previous_period.termination_reason
            == StateSupervisionPeriodTerminationReason.ABSCONSION
        ):
            # If the most recent previous supervision period was an absconsion, the current supervision period's
            # admission reason should be ABSCONSION.
            return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION
        if (
            most_recent_previous_period.termination_reason
            == StateSupervisionPeriodTerminationReason.REVOCATION
        ):
            # If the most recent previous supervision period was a REVOCATION, the current supervision period's
            # admission reason should be COURT_SENTENCE.
            return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
        if (
            most_recent_previous_period.supervision_type
            == StateSupervisionType.HALFWAY_HOUSE
            and supervision_period.supervision_type == StateSupervisionType.PAROLE
        ):
            # If the supervision type transitioned from HALFWAY_HOUSE to PAROLE, the current supervision period's
            # admission reason should be TRANSFER_WITHIN_STATE.
            return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
        if (
            most_recent_previous_period.supervision_type == StateSupervisionType.PAROLE
            and supervision_period.supervision_type == StateSupervisionType.PROBATION
        ):
            # If the supervision type transitioned from PAROLE to PROBATION, the admission reason should be
            # COURT_SENTENCE.
            return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
        if (
            most_recent_previous_period.supervising_officer
            and supervision_period.supervising_officer
            and most_recent_previous_period.supervising_officer
            != supervision_period.supervising_officer
        ):
            # If the supervision officer changed between the previous and current supervision period, the admission
            # reason should be TRANSFER_WITHIN_STATE.
            return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE
        if (
            most_recent_previous_period.supervision_type
            == StateSupervisionType.PROBATION
            and supervision_period.supervision_type == StateSupervisionType.PAROLE
        ):
            # If the supervision type transitioned from PROBATION to PAROLE, the admission reason should be
            # INTERNAL_UNKNOWN, since this should be extremely rare.
            return StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN

    return None
