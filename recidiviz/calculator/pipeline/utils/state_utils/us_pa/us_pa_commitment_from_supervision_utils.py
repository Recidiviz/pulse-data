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
"""Utils for state-specific logic related to incarceration commitments from
 supervision in US_PA."""
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
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    get_relevant_supervision_periods_for_commitment_to_supervision,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
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

# We look for commitment decisions from the parole board that occurred within 30 days of
# the commitment admission
BOARD_DECISION_WINDOW_DAYS = 30

# Ranked from most to least severe
SPFI_SUBTYPE_SEVERITY_ORDER = [
    SHOCK_INCARCERATION_12_MONTHS,
    SHOCK_INCARCERATION_9_MONTHS,
    SHOCK_INCARCERATION_6_MONTHS,
    SHOCK_INCARCERATION_UNDER_6_MONTHS,
    SHOCK_INCARCERATION_PVC,
]

# Right now, all of the supported SPFI subtypes listed in SPFI_SUBTYPE_SEVERITY_ORDER
# indicate a specialized_purpose_for_incarceration of SHOCK_INCARCERATION
SHOCK_INCARCERATION_SUBTYPES = SPFI_SUBTYPE_SEVERITY_ORDER


def _us_pa_admission_is_commitment_from_supervision(
    incarceration_period: StateIncarcerationPeriod,
) -> bool:
    """Determines whether the admission to incarceration is due to a commitment from
    supervision using the admission_reason on the |incarceration_period|."""
    # These are the only two admission reasons in US_PA that indicate a commitment
    # from supervision
    return incarceration_period.admission_reason in (
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
    )


# TODO(#7140): Change the return type to Tuple[bool, Optional[StateSupervisionPeriod]]
#  once this function deterministically chooses only one pre-commitment SP
def us_pa_pre_commitment_supervision_periods_if_commitment(
    incarceration_period: StateIncarcerationPeriod,
    supervision_periods: List[StateSupervisionPeriod],
) -> Tuple[bool, List[StateSupervisionPeriod]]:
    """Determines whether the incarceration_period started because of a commitment from
    supervision. If a commitment from supervision did occur, finds the supervision
    period(s) that preceded the commitment. If a commitment did not occur, returns False
    and an empty list.
    """
    admission_is_commitment = _us_pa_admission_is_commitment_from_supervision(
        incarceration_period
    )

    if not admission_is_commitment:
        return False, []

    admission_date = incarceration_period.admission_date

    if not admission_date:
        raise ValueError(
            "Unexpected null admission_date on incarceration_period: "
            f"[{incarceration_period}]"
        )

    pre_commitment_supervision_periods = (
        get_relevant_supervision_periods_for_commitment_to_supervision(
            admission_date,
            supervision_periods,
            prioritize_overlapping_periods=True,
        )
    )

    return admission_is_commitment, pre_commitment_supervision_periods


def us_pa_purpose_for_incarceration_and_subtype(
    incarceration_period: StateIncarcerationPeriod,
    violation_responses: List[StateSupervisionViolationResponse],
) -> Tuple[Optional[StateSpecializedPurposeForIncarceration], Optional[str]]:
    """Determines the correct purpose_for_incarceration for the given
    incarceration_period and, if applicable, the subtype of the
    purpose_for_incarceration. The subtypes indicate whether someone was admitted to a
    Parole Violator Center (PVC), or for how long they were mandated to be in prison for
    a shock incarceration. For shock incarceration commitments, the subtype is
    determined by the decision_raw_text values on the decisions made by the
    parole board preceding the commitment admission.
    """
    if not incarceration_period.admission_date:
        raise ValueError(
            "Unexpected StateIncarcerationPeriod without a set admission_date."
        )

    if (
        incarceration_period.specialized_purpose_for_incarceration
        == StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
    ):
        return StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON, None

    purpose_for_incarceration_subtype = _purpose_for_incarceration_subtype(
        incarceration_period.admission_date,
        incarceration_period.specialized_purpose_for_incarceration_raw_text,
        violation_responses,
    )

    # We know a commitment was for shock incarceration from the
    # specialized_purpose_for_incarceration and/or the parole decisions
    is_shock_incarceration_commitment = (
        purpose_for_incarceration_subtype in SHOCK_INCARCERATION_SUBTYPES
        or incarceration_period.specialized_purpose_for_incarceration
        == StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
    )

    if is_shock_incarceration_commitment and purpose_for_incarceration_subtype is None:
        # We know that this person was admitted for shock incarceration, but we don't
        # know how the specific length of shock incarceration. Default to less than 6
        # months.
        purpose_for_incarceration_subtype = SHOCK_INCARCERATION_UNDER_6_MONTHS

    purpose_for_incarceration = (
        StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        if is_shock_incarceration_commitment
        # The GENERAL purpose_for_incarceration indicates a legal revocation
        else StateSpecializedPurposeForIncarceration.GENERAL
    )

    return purpose_for_incarceration, purpose_for_incarceration_subtype


def _purpose_for_incarceration_subtype(
    commitment_admission_date: datetime.date,
    specialized_purpose_for_incarceration_raw_text: Optional[str],
    violation_responses: List[StateSupervisionViolationResponse],
) -> Optional[str]:
    """Determines the purpose_for_incarceration_subtype using either the raw text of the
    specialized_purpose_for_incarceration or the decisions made by the parole board.

    When evaluating the decisions made by the parole board, returns the most severe
    commitment decision made by the parole board on or before the
    |commitment_admission_date|, where the date of the decision is within
    BOARD_DECISION_WINDOW_DAYS of the |commitment_admission_date|."""

    if specialized_purpose_for_incarceration_raw_text == PURPOSE_FOR_INCARCERATION_PVC:
        # Indicates a commitment to a Parole Violator Center (PVC)
        return SHOCK_INCARCERATION_PVC

    lower_bound_date = commitment_admission_date - relativedelta(
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
        and lower_bound_date <= response.response_date <= commitment_admission_date
    ]

    most_recent_responses = responses_on_most_recent_response_date(responses_in_window)

    return _most_severe_purpose_for_incarceration_subtype(most_recent_responses)


def _most_severe_purpose_for_incarceration_subtype(
    violation_responses: List[StateSupervisionViolationResponse],
) -> Optional[str]:
    """Returns the most severe purpose_for_incarceration_subtype listed on all of the
    violation_responses (stored in the decision_raw_text), according to the
    SPFI_SUBTYPE_SEVERITY_ORDER ranking."""
    commitment_type_subtypes: Set[str] = set()

    for response in violation_responses:
        response_decisions = response.supervision_violation_response_decisions

        for response_decision in response_decisions:
            if response_decision.decision_raw_text:
                commitment_type_subtypes.add(response_decision.decision_raw_text)

    for purpose_for_incarceration_subtype in SPFI_SUBTYPE_SEVERITY_ORDER:
        if purpose_for_incarceration_subtype in commitment_type_subtypes:
            return purpose_for_incarceration_subtype

    return None
