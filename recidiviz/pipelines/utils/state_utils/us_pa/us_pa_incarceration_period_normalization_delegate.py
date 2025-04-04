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
"""Contains state-specific logic for certain aspects of normalization US_PA
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
import datetime
from typing import List, Optional, Set

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    PurposeForIncarcerationInfo,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.incarceration_period_utils import (
    legacy_standardize_purpose_for_incarceration_values,
)
from recidiviz.pipelines.utils.violation_response_utils import (
    responses_on_most_recent_response_date,
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
PFI_SUBTYPE_SEVERITY_ORDER = [
    SHOCK_INCARCERATION_12_MONTHS,
    SHOCK_INCARCERATION_9_MONTHS,
    SHOCK_INCARCERATION_6_MONTHS,
    SHOCK_INCARCERATION_UNDER_6_MONTHS,
    SHOCK_INCARCERATION_PVC,
]

# Right now, all of the supported PFI subtypes listed in PFI_SUBTYPE_SEVERITY_ORDER
# indicate a specialized_purpose_for_incarceration of SHOCK_INCARCERATION
SHOCK_INCARCERATION_SUBTYPES = PFI_SUBTYPE_SEVERITY_ORDER


class UsPaIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_PA implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def get_pfi_info_for_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        violation_responses: Optional[
            List[NormalizedStateSupervisionViolationResponse]
        ],
    ) -> PurposeForIncarcerationInfo:
        """Commitment from supervision admissions in US_PA sometimes require updated
        pfi information."""
        return _us_pa_get_pfi_info_for_incarceration_period(
            incarceration_period_list_index,
            sorted_incarceration_periods,
            violation_responses,
        )

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Standardizing PFI using the legacy _standardize_purpose_for_incarceration_values function
        for US_PA since this was previously the default normalization behavior
        and there hasn't been a use case for skipping this inferrence yet"""

        return legacy_standardize_purpose_for_incarceration_values(
            incarceration_periods
        )


def _us_pa_get_pfi_info_for_incarceration_period(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    violation_responses: Optional[List[NormalizedStateSupervisionViolationResponse]],
) -> PurposeForIncarcerationInfo:
    """Determines the correct purpose_for_incarceration for the given
    incarceration_period and, if applicable, the subtype of the
    purpose_for_incarceration. The subtypes indicate whether someone was admitted to
    a Parole Violator Center (PVC), or for how long they were mandated to be in
    prison for a shock incarceration. For shock incarceration commitments,
    the subtype is determined by the decision_raw_text values on the decisions made
    by the parole board preceding the commitment admission.

    Returns a PurposeForIncarcerationInfo object with the correct values for this
    period.
    """
    if violation_responses is None:
        raise ValueError(
            "IP normalization relies on violation responses for US_PA. "
            "Expected non-null violation_responses."
        )

    incarceration_period = sorted_incarceration_periods[incarceration_period_list_index]

    if not incarceration_period.admission_date:
        raise ValueError(
            "Unexpected StateIncarcerationPeriod without a set admission_date."
        )

    pfi = incarceration_period.specialized_purpose_for_incarceration

    # TODO(#16703): Remove the PFI check once we have updated the ingest mappings to be conditional
    #  on the program id value so that all non-transfers are cast as INTERNAL_UNKNOWN
    #  for ccis periods with program IDs not in 26,46,51

    if (
        not is_commitment_from_supervision(
            incarceration_period.admission_reason, allow_ingest_only_enum_values=True
        )
        or pfi == StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
    ):
        # This period does not require an updated purpose_for_incarceration value,
        # and does not have a pfi_subtype
        return PurposeForIncarcerationInfo(
            purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            purpose_for_incarceration_subtype=None,
        )

    if pfi not in [
        StateSpecializedPurposeForIncarceration.GENERAL,
        StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
    ]:
        raise ValueError(
            "Unexpected specialized_purpose_for_incarceration value "
            "associated with a commitment from supervision admission "
            f"for US_PA: {pfi}"
        )

    if pfi == StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON:
        # There are no pfi subtypes for treatment in prison periods
        return PurposeForIncarcerationInfo(
            purpose_for_incarceration=pfi, purpose_for_incarceration_subtype=None
        )

    purpose_for_incarceration_subtype = _purpose_for_incarceration_subtype(
        incarceration_period.admission_date,
        incarceration_period.specialized_purpose_for_incarceration_raw_text,
        violation_responses,
    )

    pfi_override: Optional[StateSpecializedPurposeForIncarceration] = None

    if purpose_for_incarceration_subtype is None:
        if pfi == StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION:
            # We know that this person was admitted for shock incarceration, but we
            # don't know the specific length of shock incarceration. Default to less
            # than 6 months.
            purpose_for_incarceration_subtype = SHOCK_INCARCERATION_UNDER_6_MONTHS
        elif pfi == StateSpecializedPurposeForIncarceration.GENERAL:
            # An undefined purpose_for_incarceration_subtype with the GENERAL
            # purpose_for_incarceration indicates a legal revocation
            pass
    elif purpose_for_incarceration_subtype in SHOCK_INCARCERATION_SUBTYPES:
        # We know a commitment was for shock incarceration from the
        # specialized_purpose_for_incarceration and/or the parole decisions
        pfi_override = StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
    else:
        # We should never reach this point, since all defined subtypes are
        # SHOCK_INCARCERATION_SUBTYPES
        raise ValueError("Unhandled pfi type and subtype scenario for US_PA.")

    return PurposeForIncarcerationInfo(
        purpose_for_incarceration=(pfi_override or pfi),
        purpose_for_incarceration_subtype=purpose_for_incarceration_subtype,
    )


def _purpose_for_incarceration_subtype(
    commitment_admission_date: datetime.date,
    specialized_purpose_for_incarceration_raw_text: Optional[str],
    violation_responses: List[NormalizedStateSupervisionViolationResponse],
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
    violation_responses: List[NormalizedStateSupervisionViolationResponse],
) -> Optional[str]:
    """Returns the most severe purpose_for_incarceration_subtype listed on all of the
    violation_responses (stored in the decision_raw_text), according to the
    PFI_SUBTYPE_SEVERITY_ORDER ranking."""
    commitment_type_subtypes: Set[str] = set()

    for response in violation_responses:
        response_decisions = response.supervision_violation_response_decisions

        for response_decision in response_decisions:
            if response_decision.decision_raw_text:
                commitment_type_subtypes.add(response_decision.decision_raw_text)

    for purpose_for_incarceration_subtype in PFI_SUBTYPE_SEVERITY_ORDER:
        if purpose_for_incarceration_subtype in commitment_type_subtypes:
            return purpose_for_incarceration_subtype

    return None
