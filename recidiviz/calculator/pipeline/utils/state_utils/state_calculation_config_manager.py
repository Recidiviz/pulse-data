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
"""Manages state-specific methodology decisions made throughout the calculation pipelines."""
# TODO(#2995): Make a state config file for every state and every one of these state-specific calculation methodologies
import sys
from datetime import date
import logging
from typing import List, Optional, Tuple, Callable, Union, Dict, Any

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import (
    NonRevocationReturnSupervisionTimeBucket,
    RevocationReturnSupervisionTimeBucket,
)
from recidiviz.calculator.pipeline.utils.calculator_utils import safe_list_index
from recidiviz.calculator.pipeline.utils.incarceration_period_index import (
    IncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    period_edges_are_valid_transfer,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_utils import (
    us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_revocation_utils import (
    us_pa_is_revocation_admission,
    us_pa_revoked_supervision_periods_if_revocation_occurred,
    us_pa_get_pre_revocation_supervision_type,
)
from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_revocation_utils import (
    us_id_filter_supervision_periods_for_revocation_identification,
    us_id_get_pre_revocation_supervision_type,
    us_id_is_revocation_admission,
    us_id_revoked_supervision_period_if_revocation_occurred,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    UsIdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_type_identification import (
    us_id_get_pre_incarceration_supervision_type,
    us_id_get_post_incarceration_supervision_type,
    us_id_get_supervision_period_admission_override,
    us_id_supervision_period_is_out_of_state,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo import us_mo_violation_utils
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    UsNdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_type_identification import (
    us_nd_get_post_incarceration_supervision_type,
    us_nd_infer_supervision_period_admission,
    us_nd_get_pre_commitment_supervision_type,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa import (
    us_pa_violation_utils,
    us_pa_revocation_utils,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    UsPaSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.supervision_period_index import (
    SupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    get_relevant_supervision_periods_before_admission_date,
)
from recidiviz.calculator.pipeline.utils.supervision_type_identification import (
    get_month_supervision_type_default,
    get_pre_incarceration_supervision_type_from_incarceration_period,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_type_identification import (
    us_mo_get_month_supervision_type,
    us_mo_get_pre_incarceration_supervision_type,
    us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day,
    us_mo_get_post_incarceration_supervision_type,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_utils import (
    us_mo_filter_violation_responses,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    is_commitment_from_supervision,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseRevocationType,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateRange, DateRangeDiff
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionSentence,
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
    StateAssessment,
    StateSupervisionContact,
    StateSupervisionViolation,
)


def supervision_types_mutually_exclusive_for_state(state_code: str) -> bool:
    """For some states, we want to track people on DUAL supervision as mutually exclusive from the groups of people on
    either PAROLE and PROBATION. For others, a person can be on multiple types of supervision simultaneously
    and contribute to counts for both types.
        - US_ID: True
        - US_MO: True
        - US_ND: False
        - US_PA: False

    Returns whether our calculations should consider supervision types as distinct for the given state_code.
    """
    return state_code.upper() in ("US_ID", "US_MO")


def drop_temporary_custody_periods(state_code: str) -> bool:
    """Whether or not incarceration periods with an admission_reason of TEMPORARY_CUSTODY
    should be dropped from calculations.
    - US_ID: False
    - US_MO: False
    - US_ND: True
    - US_PA: False
    """
    return state_code.upper() == "US_ND"


def drop_non_state_prison_incarceration_type_periods(state_code: str) -> bool:
    """Whether or not incarceration periods that aren't in a STATE_PRISON
    should be dropped from calculations.
    - US_ID: False
    - US_MO: True
    - US_ND: False
    - US_PA: False
    """
    return state_code.upper() == "US_MO"


def investigation_periods_in_supervision_population(_state_code: str) -> bool:
    """Whether or not supervision periods that have a supervision_period_supervision_type of INVESTIGATION should be
    counted in the supervision calculations.
        - US_ID: False
        - US_MO: False
        - US_ND: False
        - US_PA: False
    """
    return False


def should_collapse_transfers_different_purposes_for_incarceration(
    state_code: str,
) -> bool:
    """Whether or not incarceration periods that are connected by a TRANSFER release and a TRANSFER admission should
    be collapsed into one period if they have different specialized_purpose_for_incarceration values.
        - US_ID: False
            We need the dates of transfers from parole board holds and treatment custody to identify revocations
            in US_ID.
        - US_MO: True
        - US_ND: True
        - US_PA: True
    """
    return state_code.upper() != "US_ID"


def filter_out_federal_and_other_country_supervision_periods(state_code: str) -> bool:
    """Whether or not only to filter supervision periods whose custodial authority is out of the country or in federal
    prison.
        - US_ID: True
        - US_MO: False
        - US_ND: False
        - US_PA: False
    """
    return state_code.upper() == "US_ID"


def include_decisions_on_follow_up_responses_for_most_severe_response(
    state_code: str,
) -> bool:
    """Some StateSupervisionViolationResponses are a 'follow-up' type of response, which is a state-defined response
    that is related to a previously submitted response. This returns whether or not the decision entries on
    follow-up responses should be considered in the calculation of the most severe response decision.
        - US_ID: False
        - US_MO: True
        - US_ND: False
        - US_PA: False
    """
    return state_code.upper() == "US_MO"


def second_assessment_on_supervision_is_more_reliable(_state_code: str) -> bool:
    """Some states rely on the first-reassessment (the second assessment) instead of the first assessment when comparing
    terminating assessment scores to a score at the beginning of someone's supervision.
        - US_ID: True
        - US_MO: True
        - US_ND: True
        - US_PA: True
    """
    # TODO(#2782): Investigate whether to update this logic
    return True


def get_month_supervision_type(
    any_date_in_month: date,
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_period: StateSupervisionPeriod,
) -> StateSupervisionPeriodSupervisionType:
    """Supervision type can change over time even if the period does not change. This function calculates the
    supervision type that a given supervision period represents during the month that |any_date_in_month| falls in. The
    objects / info we use to determine supervision type may be state-specific.

    Args:
    any_date_in_month: (date) Any day in the month to consider
    supervision_period: (StateSupervisionPeriod) The supervision period we want to associate a supervision type with
    supervision_sentences: (List[StateSupervisionSentence]) All supervision sentences for a given person.
    """

    if supervision_period.state_code.upper() == "US_MO":
        return us_mo_get_month_supervision_type(
            any_date_in_month,
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
        )

    if supervision_period.state_code.upper() in ("US_ID", "US_PA"):
        return (
            supervision_period.supervision_period_supervision_type
            if supervision_period.supervision_period_supervision_type
            else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

    return get_month_supervision_type_default(
        any_date_in_month,
        supervision_sentences,
        incarceration_sentences,
        supervision_period,
    )


def get_pre_incarceration_supervision_type(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was reincarcerated after a period of supervision, returns the type of supervision they were on
    right before the reincarceration.

    Args:
        incarceration_sentences: (List[StateIncarcerationSentence]) All IncarcerationSentences associated with this
            person.
        supervision_sentences: (List[StateSupervisionSentence]) All SupervisionSentences associated with this person.
        incarceration_period: (StateIncarcerationPeriod) The incarceration period where the person was first
            reincarcerated.
    """

    state_code = incarceration_period.state_code

    if state_code.upper() == "US_MO":
        return us_mo_get_pre_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )

    if state_code.upper() == "US_ID":
        return us_id_get_pre_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )

    # TODO(#2938): Decide if we want date matching/supervision period lookback logic for US_ND
    return get_pre_incarceration_supervision_type_from_incarceration_period(
        incarceration_period
    )


def get_post_incarceration_supervision_type(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was released from incarceration onto some form of supervision, returns the type of supervision
    they were released to. This function must be implemented for each state for which we need this output. There is not
    a default way to determine the supervision type someone is released onto.

    Args:
        incarceration_sentences: (List[StateIncarcerationSentence]) All IncarcerationSentences associated with this
            person.
        supervision_sentences: (List[StateSupervisionSentence]) All SupervisionSentences associated with this person.
        incarceration_period: (StateIncarcerationPeriod) The incarceration period the person was released from.
    """
    state_code = incarceration_period.state_code

    if state_code.upper() == "US_ID":
        return us_id_get_post_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )
    if state_code.upper() == "US_MO":
        return us_mo_get_post_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )
    if state_code.upper() == "US_ND":
        return us_nd_get_post_incarceration_supervision_type(incarceration_period)

    logging.warning(
        "get_post_incarceration_supervision_type not implemented for state: %s",
        state_code,
    )
    return None


# TODO(#6985): Change all revocation language to commitment to supervision
def get_pre_revocation_supervision_type(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
    revoked_supervision_period: Optional[StateSupervisionPeriod],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Returns the supervision type the person was on before they had their supervision revoked."""
    if incarceration_period.state_code.upper() == "US_ID":
        return us_id_get_pre_revocation_supervision_type(revoked_supervision_period)
    if incarceration_period.state_code.upper() == "US_ND":
        return us_nd_get_pre_commitment_supervision_type(
            incarceration_period, revoked_supervision_period
        )
    if incarceration_period.state_code.upper() == "US_PA":
        return us_pa_get_pre_revocation_supervision_type(revoked_supervision_period)

    return get_pre_incarceration_supervision_type(
        incarceration_sentences, supervision_sentences, incarceration_period
    )


def terminating_supervision_period_supervision_type(
    supervision_period: StateSupervisionPeriod,
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_sentences: List[StateIncarcerationSentence],
) -> StateSupervisionPeriodSupervisionType:
    """Calculates the supervision type that should be associated with a terminated supervision period. In some cases,
    the supervision period will be terminated long after the person has been incarcerated (e.g. in the case of a board
    hold, someone might remain assigned to a PO until their parole is revoked), so we do a lookback to see the most
    recent supervision period supervision type we can associate with this termination.
    """

    if not supervision_period.termination_date:
        raise ValueError(
            f"Expected a terminated supervision period for period "
            f"[{supervision_period.supervision_period_id}]"
        )

    if supervision_period.state_code.upper() == "US_MO":
        supervision_type = us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
            upper_bound_exclusive_date=supervision_period.termination_date,
            lower_bound_inclusive_date=supervision_period.start_date,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
        )

        return (
            supervision_type
            if supervision_type
            else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

    if supervision_period.state_code.upper() in ("US_ID", "US_PA"):
        return (
            supervision_period.supervision_period_supervision_type
            if supervision_period.supervision_period_supervision_type
            else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

    return get_month_supervision_type_default(
        supervision_period.termination_date,
        supervision_sentences,
        incarceration_sentences,
        supervision_period,
    )


def state_specific_filter_of_violation_responses(
    violation_responses: List[StateSupervisionViolationResponse],
    include_follow_up_responses: bool,
) -> List[StateSupervisionViolationResponse]:
    """State-specific filtering of the violation responses that should be included analysis."""
    if violation_responses:
        state_code = violation_responses[0].state_code
        if state_code.upper() == "US_MO":
            return us_mo_filter_violation_responses(
                violation_responses, include_follow_up_responses
            )
    return violation_responses


# TODO(#6985): Change all "revocation" language to "commitment from supervision"
def filter_supervision_periods_for_revocation_identification(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """State-specific filtering of supervision periods that should be included in pre-revocation analysis."""
    if supervision_periods:
        if supervision_periods[0].state_code.upper() == "US_ID":
            return us_id_filter_supervision_periods_for_revocation_identification(
                supervision_periods
            )
    return supervision_periods


# TODO(#6985): Change all "revocation" language to "commitment from supervision"
def incarceration_period_is_from_revocation(
    incarceration_period: StateIncarcerationPeriod,
    preceding_incarceration_period: Optional[StateIncarcerationPeriod],
) -> bool:
    """Determines if the sequence of incarceration periods represents a revocation."""
    if incarceration_period.state_code.upper() == "US_ID":
        return us_id_is_revocation_admission(
            incarceration_period, preceding_incarceration_period
        )
    if incarceration_period.state_code.upper() == "US_PA":
        return us_pa_is_revocation_admission(incarceration_period)
    return is_commitment_from_supervision(incarceration_period.admission_reason)


def should_produce_supervision_time_bucket_for_period(
    supervision_period: StateSupervisionPeriod,
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
) -> bool:
    """Whether or not any SupervisionTimeBuckets should be created using the supervision_period. In some cases,
    supervision period pre-processing will not drop periods entirely because we need them for context in some of the
    calculations, but we do not want to create metrics using the periods.

    If this returns True, it does not necessarily mean they should be counted towards the supervision population for
    any part of this period. It just means that a person was actively assigned to supervision at this time and various
    characteristics of this period may be relevant for generating metrics (such as the termination reason / date) even
    if we may not count this person towards the supervision population during the period time span (e.g. if they are
    incarcerated the whole time).
    """
    if supervision_period.state_code == "US_MO":
        # If no days of this supervision_period should count towards any metrics, we can drop this period entirely
        sp_range = supervision_period.duration

        return (
            us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=sp_range.upper_bound_exclusive_date,
                lower_bound_inclusive_date=sp_range.lower_bound_inclusive_date,
                incarceration_sentences=incarceration_sentences,
                supervision_sentences=supervision_sentences,
            )
            is not None
        )

    if (
        supervision_period.supervision_period_supervision_type
        == StateSupervisionPeriodSupervisionType.INVESTIGATION
        # TODO(#2891): Remove this check when we remove supervision_type from StateSupervisionPeriods
        or supervision_period.supervision_type == StateSupervisionType.PRE_CONFINEMENT
    ) and not investigation_periods_in_supervision_population(
        supervision_period.state_code
    ):
        return False
    return True


def supervision_period_counts_towards_supervision_population_in_date_range_state_specific(
    date_range: DateRange,
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_period: StateSupervisionPeriod,
) -> bool:
    """Returns False if there is state-specific information to indicate that the supervision period should not count
    towards any supervision metrics in the date range. Returns True if either there is a state-specific check that
    indicates that the supervision period should count or if there is no state-specific check to perform.
    """
    if supervision_period.state_code == "US_MO":
        overlapping_range = DateRangeDiff(
            range_1=date_range, range_2=supervision_period.duration
        ).overlapping_range

        if not overlapping_range:
            return False

        return (
            us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=overlapping_range.upper_bound_exclusive_date,
                lower_bound_inclusive_date=overlapping_range.lower_bound_inclusive_date,
                incarceration_sentences=incarceration_sentences,
                supervision_sentences=supervision_sentences,
            )
            is not None
        )

    return True


def get_state_specific_case_compliance_manager(
    supervision_period: StateSupervisionPeriod,
    case_type: StateSupervisionCaseType,
    start_of_supervision: date,
    assessments: List[StateAssessment],
    supervision_contacts: List[StateSupervisionContact],
) -> Optional[StateSupervisionCaseComplianceManager]:
    """Returns a state-specific SupervisionCaseComplianceManager object, containing information about whether the
    given supervision case is in compliance with state-specific standards. If the state of the
    supervision_period does not have state-specific compliance calculations, returns None."""
    state_code = supervision_period.state_code.upper()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionCaseCompliance(
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
        )
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionCaseCompliance(
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
        )
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionCaseCompliance(
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
        )

    return None


# TODO(#3829): Remove this helper once we've built level 1/level 2 supervision location distinction directly into our
#  schema (info currently packed into supervision_site for states that have both).
def get_supervising_officer_and_location_info_from_supervision_period(
    supervision_period: StateSupervisionPeriod,
    supervision_period_to_agent_associations: Dict[int, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Given |supervision_period| returns the relevant officer and supervision location info abiding by state-specific
    logic.
    """
    supervising_officer_external_id = None
    level_1_supervision_location = None
    level_2_supervision_location = None

    if not supervision_period.supervision_period_id:
        raise ValueError("Unexpected null supervision_period_id")

    # In some states we have squashed multiple pieces of supervision location into the supervision site field. We have
    # state-specific logic to split the supervision_site info into its appropriate fields.
    if supervision_period.state_code == "US_ID":
        if supervision_period.supervision_site:
            # In ID, supervision_site follows format "{supervision district}|{location/office within district}"
            (
                level_2_supervision_location,
                level_1_supervision_location,
            ) = supervision_period.supervision_site.split("|")
    elif supervision_period.state_code == "US_PA":
        # TODO(#6314): Remove this hack once we've re-run US_PA ingest with the fix.
        #  Replace the contents of this PA-specific block with:
        #  supervision_site = supervision_period.supervision_site
        #  if supervision_site and supervision_site.count("|") == 2:
        #     # In PA, supervision_site follows format
        #     # "{supervision district}|{supervision suboffice}|{supervision unit org code}"
        #     (
        #         level_2_supervision_location,
        #         level_1_supervision_location,
        #         _org_code,
        #     ) = supervision_site.split("|")
        period_info = supervision_period_to_agent_associations.get(
            supervision_period.supervision_period_id
        )
        if period_info:
            supervision_info = period_info["agent_external_id"]

            if supervision_info and supervision_info.count("#") == 1:
                (
                    supervision_site,
                    supervising_officer_external_id,
                ) = supervision_info.split("#")

                # If the supervising_officer_external_id is only whitespace, set to None
                if not supervising_officer_external_id.strip():
                    supervising_officer_external_id = None

                if supervision_site and supervision_site.count("|") == 2:
                    # In PA, supervision_site follows format
                    # "{supervision district}|{supervision suboffice}|{supervision unit org code}"
                    (
                        level_2_supervision_location,
                        level_1_supervision_location,
                        _org_code,
                    ) = supervision_site.split("|")
            return (
                supervising_officer_external_id,
                level_1_supervision_location or None,
                level_2_supervision_location or None,
            )

    else:
        level_1_supervision_location = supervision_period.supervision_site

    agent_info = supervision_period_to_agent_associations.get(
        supervision_period.supervision_period_id
    )

    if agent_info is not None:
        supervising_officer_external_id = agent_info["agent_external_id"]

    return (
        supervising_officer_external_id,
        level_1_supervision_location or None,
        level_2_supervision_location or None,
    )


def get_violation_type_subtype_strings_for_violation(
    violation: StateSupervisionViolation,
) -> List[str]:
    """Returns a list of strings that represent the violation subtypes present on the given |violation|. If there's no
    state-specific logic for determining the subtypes, then a list of the violation_type raw values in the violation's
    supervision_violation_types is returned."""
    if violation.state_code.upper() == "US_MO":
        return us_mo_violation_utils.us_mo_get_violation_type_subtype_strings_for_violation(
            violation
        )
    if violation.state_code.upper() == "US_PA":
        return us_pa_violation_utils.us_pa_get_violation_type_subtype_strings_for_violation(
            violation
        )

    supervision_violation_types = violation.supervision_violation_types

    if supervision_violation_types:
        return [
            violation_type_entry.violation_type.value
            for violation_type_entry in supervision_violation_types
            if violation_type_entry.violation_type
        ]

    return []


def sorted_violation_subtypes_by_severity(
    state_code: str, violation_subtypes: List[str], default_severity_order: List[str]
) -> List[str]:
    """Sorts the provided |violation_subtypes| by severity, and returns the list in order of descending severity.
    Defers to the severity ordering in the |default_severity_order| if no state-specific logic is implemented."""
    if state_code.upper() == "US_MO":
        return us_mo_violation_utils.us_mo_sorted_violation_subtypes_by_severity(
            violation_subtypes
        )
    if state_code.upper() == "US_PA":
        return us_pa_violation_utils.us_pa_sorted_violation_subtypes_by_severity(
            violation_subtypes
        )

    logging.warning(
        "No implemented violation subtype ordering for state [%s]", state_code
    )

    sorted_violation_subtypes = sorted(
        violation_subtypes,
        key=lambda subtype: safe_list_index(
            default_severity_order, subtype, sys.maxsize
        ),
    )

    return sorted_violation_subtypes


def violation_type_from_subtype(
    state_code: str, violation_subtype: str
) -> StateSupervisionViolationType:
    """Determines which StateSupervisionViolationType corresponds to the |violation_subtype| value for the given
    |state_code|. If no state-specific logic is implemented, returns the StateSupervisionViolationType corresponding to
    the |violation_subtype|."""
    if state_code.upper() == "US_MO":
        return us_mo_violation_utils.us_mo_violation_type_from_subtype(
            violation_subtype
        )
    if state_code.upper() == "US_PA":
        return us_pa_violation_utils.us_pa_violation_type_from_subtype(
            violation_subtype
        )

    for violation_type in StateSupervisionViolationType:
        if violation_subtype == violation_type.value:
            return violation_type

    raise ValueError(
        f"Unexpected violation_subtype {violation_subtype} for {state_code}."
    )


def shorthand_for_violation_subtype(state_code: str, violation_subtype: str) -> str:
    """Returns the shorthand string representing the given |violation_subtype| in the given |state_code|. If no
    state-specific logic is implemented, returns a lowercase version of the |violation_subtype| string."""
    if state_code.upper() == "US_MO":
        return us_mo_violation_utils.us_mo_shorthand_for_violation_subtype(
            violation_subtype
        )
    if state_code.upper() == "US_PA":
        return us_pa_violation_utils.us_pa_shorthand_for_violation_subtype(
            violation_subtype
        )

    logging.warning(
        "No state-specific violation subtype shorthand implementation for state [%s]",
        state_code,
    )
    return violation_subtype.lower()


# TODO(#6985): Change all "revocation" language to "commitment from supervision"
def revoked_supervision_periods_if_revocation_occurred(
    state_code: str,
    incarceration_period: StateIncarcerationPeriod,
    supervision_periods: List[StateSupervisionPeriod],
    preceding_incarceration_period: Optional[StateIncarcerationPeriod],
) -> Tuple[bool, List[StateSupervisionPeriod]]:
    """If the incarceration period was a result of a supervision revocation, finds the supervision periods that were
    revoked.

    Returns (False, []) if the incarceration period was not a result of a revocation. Returns True and the list of
    supervision periods that were revoked if the incarceration period was a result of a revocation. In some cases, it's
    possible for the admission to be a revocation even though we cannot identify the corresponding supervision periods
    that were revoked (e.g. the person was serving supervision out-of-state). In these instances, this function will
    return True and an empty list [].
    """
    revoked_periods: List[StateSupervisionPeriod] = []

    if state_code == StateCode.US_ID.value:
        (
            admission_is_revocation,
            revoked_period,
        ) = us_id_revoked_supervision_period_if_revocation_occurred(
            incarceration_period, supervision_periods, preceding_incarceration_period
        )

        if revoked_period:
            revoked_periods = [revoked_period]
    elif state_code == StateCode.US_ND.value:
        (
            admission_is_revocation,
            revoked_periods,
        ) = us_nd_pre_commitment_supervision_periods_if_commitment_from_supervision(
            incarceration_period, supervision_periods
        )
    elif state_code == StateCode.US_PA.value:
        (
            admission_is_revocation,
            revoked_periods,
        ) = us_pa_revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, supervision_periods
        )
    else:
        admission_is_revocation = is_commitment_from_supervision(
            incarceration_period.admission_reason
        )
        revoked_periods = get_relevant_supervision_periods_before_admission_date(
            incarceration_period.admission_date, supervision_periods
        )

    return admission_is_revocation, revoked_periods


# TODO(#6985): Change all "revocation" language to "commitment from supervision"
def state_specific_revocation_type_and_subtype(
    state_code: str,
    incarceration_period: StateIncarcerationPeriod,
    violation_responses: List[StateSupervisionViolationResponse],
    default_revocation_type_and_subtype_identifier: Callable[
        [StateIncarcerationPeriod],
        Tuple[Optional[StateSupervisionViolationResponseRevocationType], Optional[str]],
    ],
) -> Tuple[Optional[StateSupervisionViolationResponseRevocationType], Optional[str]]:
    """Returns the revocation_type and, if applicable, revocation_type_subtype for the given revocation admission."""
    if state_code.upper() == StateCode.US_PA.value:
        return us_pa_revocation_utils.us_pa_revocation_type_and_subtype(
            incarceration_period, violation_responses
        )

    return default_revocation_type_and_subtype_identifier(incarceration_period)


def state_specific_supervision_admission_reason_override(
    state_code: str,
    supervision_period: StateSupervisionPeriod,
    supervision_period_index: SupervisionPeriodIndex,
    incarceration_period_index: IncarcerationPeriodIndex,
) -> Optional[StateSupervisionPeriodAdmissionReason]:
    if state_code == "US_ID":
        return us_id_get_supervision_period_admission_override(
            supervision_period=supervision_period,
            supervision_period_index=supervision_period_index,
        )
    if state_code == "US_ND":
        return us_nd_infer_supervision_period_admission(
            supervision_period=supervision_period,
            supervision_period_index=supervision_period_index,
            incarceration_period_index=incarceration_period_index,
        )
    return supervision_period.admission_reason


def state_specific_violation_response_pre_processing_function(
    state_code: str,
) -> Optional[
    Callable[
        [List[StateSupervisionViolationResponse]],
        List[StateSupervisionViolationResponse],
    ]
]:
    """Returns a callable to be used to prepare StateSupervisionViolationResponses for calculations, if applicable for
    the given state. If the state doesn't have a state-specific violation response pre-processing function,
    returns None."""
    if state_code.upper() == "US_MO":
        return us_mo_violation_utils.us_mo_prepare_violation_responses_for_calculations

    return None


def state_specific_incarceration_admission_reason_override(
    incarceration_period: StateIncarcerationPeriod,
    admission_reason: StateIncarcerationPeriodAdmissionReason,
    supervision_type_at_admission: Optional[StateSupervisionPeriodSupervisionType],
    previous_incarceration_period: Optional[StateIncarcerationPeriod] = None,
) -> StateIncarcerationPeriodAdmissionReason:
    """Returns a (potentially) updated admission reason to be used in calculations, given the provided
    |incarceration_period|, |admission_reason|, |supervision_type_at_admission|, |previous_incarceration_period|
    """
    if incarceration_period.state_code == "US_ID":
        # If a person was on an investigative supervision period prior to incarceration, their admission reason
        # should be considered a new admission.
        if (
            supervision_type_at_admission
            and supervision_type_at_admission
            == StateSupervisionPeriodSupervisionType.INVESTIGATION
        ):
            return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION

        # If a person's previous IP has a different specialized_purpose_for_incarceration
        # than the current period, the correct admission reason is STATUS_CHANGE
        if (
            previous_incarceration_period
            and period_edges_are_valid_transfer(
                previous_incarceration_period,
                incarceration_period,
            )
            and previous_incarceration_period.specialized_purpose_for_incarceration
            and incarceration_period.specialized_purpose_for_incarceration
            and previous_incarceration_period.specialized_purpose_for_incarceration
            != incarceration_period.specialized_purpose_for_incarceration
        ):
            return StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE
    return admission_reason


def state_specific_incarceration_release_reason_override(
    incarceration_period: StateIncarcerationPeriod,
    release_reason: StateIncarcerationPeriodReleaseReason,
    next_incarceration_period: Optional[StateIncarcerationPeriod] = None,
) -> StateIncarcerationPeriodReleaseReason:
    """Returns a (potentially) updated release reason to be used in calculations, given the provided
    |incarceration_period|, and |next_incarceration_period|.
    """
    if incarceration_period.state_code == "US_ID":
        # If the current purpose_for_incarceration is different from the
        # next_incarceration_period's specialized_purpose_for_incarceration, then
        # the release reason should be STATUS_CHANGE
        if (
            next_incarceration_period
            and period_edges_are_valid_transfer(
                incarceration_period,
                next_incarceration_period,
            )
            and incarceration_period.specialized_purpose_for_incarceration
            and next_incarceration_period.specialized_purpose_for_incarceration
            and incarceration_period.specialized_purpose_for_incarceration
            != next_incarceration_period.specialized_purpose_for_incarceration
        ):
            return StateIncarcerationPeriodReleaseReason.STATUS_CHANGE
    return release_reason


def state_specific_specialized_purpose_for_incarceration_override(
    incarceration_period: StateIncarcerationPeriod,
    admission_reason: StateIncarcerationPeriodAdmissionReason,
) -> Optional[StateSpecializedPurposeForIncarceration]:
    """Returns a (potentially) updated specialized_purpose_for_incarceration to be used in calculations, given an
    |incarceration_period}| and |admission_reason|."""
    if incarceration_period.state_code == "US_MO":
        if (
            admission_reason
            == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            and incarceration_period.specialized_purpose_for_incarceration is None
        ):
            # In US_MO, we can infer that an admission with an `admission_reason` of `TEMPORARY_CUSTODY` is an
            # admission to a parole board hold if the period has no other set `specialized_purpose_for_incarceration`.
            return StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
    return incarceration_period.specialized_purpose_for_incarceration


def supervision_period_is_out_of_state(
    supervision_time_bucket: Union[
        NonRevocationReturnSupervisionTimeBucket, RevocationReturnSupervisionTimeBucket
    ]
) -> bool:
    """Returns whether the given supervision time bucket should be considered a supervision period that is being
    served out of state.
    """
    if supervision_time_bucket.state_code != "US_ID":
        return False

    return us_id_supervision_period_is_out_of_state(supervision_time_bucket)


# TODO(#6529):Move sorted_violation_responses_in_window() into violation_response_utils.py
def sorted_violation_responses_in_window(
    violation_responses: List[StateSupervisionViolationResponse],
    upper_bound_exclusive: date,
    lower_bound_inclusive: Optional[date],
    include_follow_up_responses: bool,
) -> List[StateSupervisionViolationResponse]:
    """First, filters the violation responses to the ones that should be used in analysis. This only includes responses
    with a set response_date that are not in a draft state and are of type VIOLATION_REPORT or CITATION.
    Returns the filtered violation responses that have a response_date between the lower_bound_inclusive and
    upper_bound_exclusive, sorted from oldest to newest."""
    filtered_responses = [
        response
        for response in violation_responses
        if response.response_date is not None
        and not response.is_draft
        and response.response_type
        in (
            StateSupervisionViolationResponseType.VIOLATION_REPORT,
            StateSupervisionViolationResponseType.CITATION,
        )
    ]

    state_filtered_responses = state_specific_filter_of_violation_responses(
        filtered_responses, include_follow_up_responses
    )

    responses_in_window = [
        response
        for response in state_filtered_responses
        if response.response_date is not None
        and response.response_date < upper_bound_exclusive
        # Only limit with a lower bound if one is set
        and (
            lower_bound_inclusive is None
            or lower_bound_inclusive <= response.response_date
        )
    ]

    # All responses will have a response_date at this point, but date.min helps to satisfy mypy
    responses_in_window.sort(key=lambda b: b.response_date or date.min)

    return responses_in_window


def state_specific_violation_responses(
    end_date: date,
    violation_responses: List[StateSupervisionViolationResponse],
    incarceration_period: Optional[StateIncarcerationPeriod],
    violation_history_window_months: int,
) -> List[StateSupervisionViolationResponse]:
    """Returns the violation response for an incarceration period when applicable for a state."""
    if incarceration_period is not None and incarceration_period.state_code == "US_ND":
        if incarceration_period.source_supervision_violation_response is not None:
            return [incarceration_period.source_supervision_violation_response]
        return []

    violation_window_lower_bound_inclusive = end_date - relativedelta(
        months=violation_history_window_months
    )
    violation_window_upper_bound_exclusive = end_date + relativedelta(days=1)

    # Do not include the follow-up responses when evaluating the violation history
    return sorted_violation_responses_in_window(
        violation_responses,
        lower_bound_inclusive=violation_window_lower_bound_inclusive,
        upper_bound_exclusive=violation_window_upper_bound_exclusive,
        include_follow_up_responses=False,
    )
