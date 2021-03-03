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
import datetime
from typing import List, Optional, Tuple, Set

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    responses_on_most_recent_response_date,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseRevocationType as RevocationType,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    get_relevant_supervision_periods_before_admission_date,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
)

PURPOSE_FOR_INCARCERATION_PVC = "CCIS-26"
SHOCK_INCARCERATION_12_MONTHS = "RESCR12"
SHOCK_INCARCERATION_9_MONTHS = "RESCR9"
SHOCK_INCARCERATION_6_MONTHS = "RESCR6"
SHOCK_INCARCERATION_UNDER_6_MONTHS = "RESCR"
SHOCK_INCARCERATION_PVC = "PVC"

# We look for revocation decisions from the parole board that occurred within 30 days of the revocation admission
BOARD_DECISION_WINDOW_DAYS = 30

# Ranked from most to least severe
REVOCATION_TYPE_SUBTYPE_SEVERITY_ORDER = [
    SHOCK_INCARCERATION_12_MONTHS,
    SHOCK_INCARCERATION_9_MONTHS,
    SHOCK_INCARCERATION_6_MONTHS,
    SHOCK_INCARCERATION_UNDER_6_MONTHS,
    SHOCK_INCARCERATION_PVC,
]

# Right now, all of the supported revocation type subtypes listed in REVOCATION_TYPE_SUBTYPE_SEVERITY_ORDER indicate a
# revocation_type of SHOCK_INCARCERATION
SHOCK_INCARCERATION_SUBTYPES = REVOCATION_TYPE_SUBTYPE_SEVERITY_ORDER


def us_pa_get_pre_revocation_supervision_type(
    revoked_supervision_period: Optional[StateSupervisionPeriod],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Returns the supervision_period_supervision_type associated with the given revoked_supervision_period,
    if present. If not, returns None."""
    if revoked_supervision_period:
        return revoked_supervision_period.supervision_period_supervision_type
    return None


def us_pa_is_revocation_admission(
    incarceration_period: StateIncarcerationPeriod,
) -> bool:
    """Determines whether the admission to incarceration is due to a revocation of supervision using the
    admission_reason on the |incarceration_period|."""
    # These are the only two valid revocation admission reasons in US_PA
    return incarceration_period.admission_reason in (
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    )


def us_pa_revoked_supervision_periods_if_revocation_occurred(
    incarceration_period: StateIncarcerationPeriod,
    supervision_periods: List[StateSupervisionPeriod],
) -> Tuple[bool, List[StateSupervisionPeriod]]:
    """Determines whether the incarceration_period started because of a revocation of supervision. If a revocation did
    occur, finds the supervision period(s) that was revoked. If a revocation did not occur, returns False and an empty
    list.
    """
    admission_is_revocation = us_pa_is_revocation_admission(incarceration_period)

    if not admission_is_revocation:
        return False, []

    revoked_periods = get_relevant_supervision_periods_before_admission_date(
        incarceration_period.admission_date, supervision_periods
    )

    return admission_is_revocation, revoked_periods


def us_pa_revocation_type_and_subtype(
    incarceration_period: StateIncarcerationPeriod,
    violation_responses: List[StateSupervisionViolationResponse],
) -> Tuple[RevocationType, Optional[str]]:
    """Determines the kind of revocation that happened and, if applicable, the subtype of the revocation_type. The
    subtypes indicate whether someone was revoked to a Parole Violator Center (PVC), or for how long they were
    revoked for shock incarceration. For shock incarceration revocations, the subtype is determined by the
    revocation_type_raw_text values on the decisions made by the parole board preceding the revocation admission."""
    if not incarceration_period.admission_date:
        raise ValueError(
            "Unexpected StateIncarcerationPeriod without a set admission_date."
        )

    if (
        incarceration_period.specialized_purpose_for_incarceration
        == StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
    ):
        return RevocationType.TREATMENT_IN_PRISON, None

    revocation_type_subtype = _revocation_type_subtype(
        incarceration_period.admission_date,
        incarceration_period.specialized_purpose_for_incarceration_raw_text,
        violation_responses,
    )

    # We know a revocation was for shock incarceration from the specialized_purpose_for_incarceration and/or the
    # parole decisions
    is_shock_incarceration_revocation = (
        revocation_type_subtype in SHOCK_INCARCERATION_SUBTYPES
        or incarceration_period.specialized_purpose_for_incarceration
        == StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
    )

    if is_shock_incarceration_revocation and revocation_type_subtype is None:
        # We know that this person was revoked for shock incarceration, but we don't know how the specific length of
        # shock incarceration. Default to less than 6 months.
        revocation_type_subtype = SHOCK_INCARCERATION_UNDER_6_MONTHS

    revocation_type = (
        RevocationType.SHOCK_INCARCERATION
        if is_shock_incarceration_revocation
        # We are using REINCARCERATION revocation type in this case to indicate a legal revocation
        else RevocationType.REINCARCERATION
    )

    return revocation_type, revocation_type_subtype


def _revocation_type_subtype(
    revocation_admission_date: datetime.date,
    specialized_purpose_for_incarceration_raw_text: Optional[str],
    violation_responses: List[StateSupervisionViolationResponse],
) -> Optional[str]:
    """Determines the revocation_type_subtype using either the raw text of the specialized_purpose_for_incarceration or
    the decisions made by the parole board.

    When evaluating the decisions made by the parole board, returns the most severe revocation type decision made by the
    parole board on or before the |revocation_admission_date|, where the date of the decision is within
    BOARD_DECISION_WINDOW_DAYS of the |revocation_admission_date|."""

    if specialized_purpose_for_incarceration_raw_text == PURPOSE_FOR_INCARCERATION_PVC:
        # Indicates a revocation to a Parole Violator Center (PVC)
        return SHOCK_INCARCERATION_PVC

    lower_bound_date = revocation_admission_date - relativedelta(
        days=BOARD_DECISION_WINDOW_DAYS
    )

    responses_in_window = [
        response
        for response in violation_responses
        # Revocation decisions by the parole board are always a PERMANENT_DECISION response
        if response.response_type
        == StateSupervisionViolationResponseType.PERMANENT_DECISION
        # We only want to look at the decisions made by the parole board
        and response.deciding_body_type
        == StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD
        and response.response_date is not None
        and lower_bound_date <= response.response_date <= revocation_admission_date
    ]

    most_recent_responses = responses_on_most_recent_response_date(responses_in_window)

    return _most_severe_revocation_type_subtype(most_recent_responses)


def _most_severe_revocation_type_subtype(
    violation_responses: List[StateSupervisionViolationResponse],
) -> Optional[str]:
    """Returns the most severe revocation_type_subtype listed on all of the violation_responses (stored in the
    revocation_type_raw_text), according to the REVOCATION_TYPE_SUBTYPE_SEVERITY_ORDER ranking."""
    revocation_type_subtypes: Set[str] = set()

    for response in violation_responses:
        response_decisions = response.supervision_violation_response_decisions

        for response_decision in response_decisions:
            if response_decision.revocation_type_raw_text:
                revocation_type_subtypes.add(response_decision.revocation_type_raw_text)

    for revocation_type_subtype in REVOCATION_TYPE_SUBTYPE_SEVERITY_ORDER:
        if revocation_type_subtype in revocation_type_subtypes:
            return revocation_type_subtype

    return None
