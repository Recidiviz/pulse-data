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
"""Identifies time buckets of supervision and classifies them as either instances of revocation or not. Also classifies
supervision sentences as successfully completed or not."""
import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import List, Dict, Tuple, Optional, Any, NamedTuple, Type

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.metrics import SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, \
    NonRevocationReturnSupervisionTimeBucket, \
    ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_revocation_identification import \
    us_id_revoked_supervision_period_if_revocation_occurred
from recidiviz.calculator.pipeline.utils.state_utils.us_mo import us_mo_violation_utils
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    last_day_of_month, identify_most_severe_violation_type_and_subtype, \
    identify_most_severe_response_decision, first_day_of_month, first_day_of_next_month
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    find_most_recent_assessment, find_assessment_score_change, most_recent_assessment_attributes
from recidiviz.calculator.pipeline.utils.incarceration_period_index import IncarcerationPeriodIndex
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    supervision_types_distinct_for_state, \
    default_to_supervision_period_officer_for_revocation_details_for_state, get_month_supervision_type, \
    terminating_supervision_period_supervision_type, \
    supervision_period_counts_towards_supervision_population_in_date_range_state_specific, \
    filter_violation_responses_before_revocation, \
    should_collapse_transfers_different_purpose_for_incarceration, incarceration_period_is_from_revocation, \
    filter_supervision_periods_for_revocation_identification, get_pre_revocation_supervision_type, \
    produce_supervision_time_bucket_for_period, only_state_custodial_authority_in_supervision_population, \
    get_case_compliance_on_date
from recidiviz.calculator.pipeline.utils.supervision_period_index import SupervisionPeriodIndex
from recidiviz.calculator.pipeline.utils.supervision_period_utils import prepare_supervision_periods_for_calculations, \
    get_relevant_supervision_periods_before_admission_date
from recidiviz.calculator.pipeline.utils.supervision_type_identification import \
    get_supervision_type_from_sentences
from recidiviz.calculator.pipeline.utils.time_range_utils import TimeRange, TimeRangeDiff
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import StateSpecializedPurposeForIncarceration, \
    is_revocation_admission
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecision, StateSupervisionViolationResponseType
from recidiviz.persistence.entity.entity_utils import get_single_state_code
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolationResponseDecisionEntry, StateSupervisionSentence, \
    StateAssessment, StateSupervisionViolation, \
    StateSupervisionViolationResponse, StateIncarcerationSentence

# The number of months for the window of time prior to a revocation return in which violations and violation responses
# should be considered when producing metrics related to a person's violation history leading up to the revocation
VIOLATION_HISTORY_WINDOW_MONTHS = 12


REVOCATION_TYPE_SEVERITY_ORDER = [
    StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
    StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,
    StateSupervisionViolationResponseRevocationType.REINCARCERATION
]

CASE_TYPE_SEVERITY_ORDER = [
    StateSupervisionCaseType.SEX_OFFENDER,
    StateSupervisionCaseType.DOMESTIC_VIOLENCE,
    StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
    StateSupervisionCaseType.GENERAL
]


def find_supervision_time_buckets(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> List[SupervisionTimeBucket]:
    """Finds buckets of time that a person was on supervision and determines if they resulted in revocation return.

    Transforms each StateSupervisionPeriod into months where the person spent any number of days on
    supervision. Excludes any time buckets in which the person was incarcerated for the entire time by looking through
    all of the person's StateIncarcerationPeriods. For each month that someone was on supervision, a
    SupervisionTimeBucket object is created. If the person was admitted to prison in that time for a revocation that
    matches the type of supervision the SupervisionTimeBucket describes, then that object is a
    RevocationSupervisionTimeBucket. If no applicable revocation occurs, then the object is of type
    NonRevocationReturnSupervisionTimeBucket.

    If someone is serving both probation and parole simultaneously, there will be one SupervisionTimeBucket for the
    probation type and one for the parole type. If someone has multiple overlapping supervision periods,
    there will be one SupervisionTimeBucket for each month on each supervision period.

    If a revocation return to prison occurs in a time bucket where there is no recorded supervision period, we count
    the individual as having been on supervision that time so that they can be included in the
    revocation return count and rate metrics for that bucket. In these cases, we add supplemental
    RevocationReturnSupervisionTimeBuckets to the list of SupervisionTimeBuckets.

    Args:
        - supervision_sentences: list of StateSupervisionSentences for a person
        - supervision_periods: list of StateSupervisionPeriods for a person
        - incarceration_periods: list of StateIncarcerationPeriods for a person
        - assessments: list of StateAssessments for a person
        - violations: list of StateSupervisionViolations for a person
        - violation_responses: list of StateSupervisionViolationResponses for a person
        - ssvr_agent_associations: dictionary associating StateSupervisionViolationResponse ids to information about the
            corresponding StateAgent on the response
        - supervision_period_to_agent_associations: dictionary associating StateSupervisionPeriod ids to information
            about the corresponding StateAgent
        - state_code_filter: the state_code to limit the output to. If this is 'ALL' or is omitted, then all states
        will be included in the result.

    Returns:
        A list of SupervisionTimeBuckets for the person.
    """
    if not supervision_periods and not incarceration_periods:
        return []

    if supervision_periods:
        state_code = get_single_state_code(supervision_periods)
    else:
        state_code = get_single_state_code(incarceration_periods)

    supervision_time_buckets: List[SupervisionTimeBucket] = []

    should_drop_non_state_custodial_authority_periods = \
        only_state_custodial_authority_in_supervision_population(state_code)

    supervision_periods = prepare_supervision_periods_for_calculations(
        supervision_periods,
        drop_non_state_custodial_authority_periods=should_drop_non_state_custodial_authority_periods)

    should_collapse_transfers_with_different_pfi = \
        should_collapse_transfers_different_purpose_for_incarceration(state_code)

    # We don't want to collapse temporary custody periods with revocations because we want to use the actual date
    # of the revocation admission for the revocation buckets
    incarceration_periods = prepare_incarceration_periods_for_calculations(
        incarceration_periods,
        collapse_temporary_custody_periods_with_revocation=False,
        collapse_transfers_with_different_pfi=should_collapse_transfers_with_different_pfi)

    supervision_period_index = SupervisionPeriodIndex(supervision_periods=supervision_periods)
    incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=incarceration_periods)

    projected_supervision_completion_buckets = classify_supervision_success(supervision_sentences,
                                                                            incarceration_period_index,
                                                                            supervision_period_to_agent_associations)

    supervision_time_buckets.extend(projected_supervision_completion_buckets)

    for supervision_period in supervision_period_index.supervision_periods:
        if produce_supervision_time_bucket_for_period(supervision_period):
            supervision_time_buckets = supervision_time_buckets + find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_period_to_agent_associations)

            supervision_termination_bucket = find_supervision_termination_bucket(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                assessments,
                supervision_period_to_agent_associations,
                incarceration_period_index
            )

            if supervision_termination_bucket:
                supervision_time_buckets.append(supervision_termination_bucket)

    supervision_time_buckets = supervision_time_buckets + find_revocation_return_buckets(
        supervision_sentences, incarceration_sentences, supervision_periods,
        assessments, violation_responses, ssvr_agent_associations,
        supervision_period_to_agent_associations, incarceration_period_index)

    if supervision_types_distinct_for_state(state_code):
        supervision_time_buckets = _convert_buckets_to_dual(supervision_time_buckets)
    else:
        supervision_time_buckets = _expand_dual_supervision_buckets(supervision_time_buckets)

    return supervision_time_buckets


def find_time_buckets_for_supervision_period(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod,
        supervision_period_index: SupervisionPeriodIndex,
        incarceration_period_index: IncarcerationPeriodIndex,
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]) -> List[SupervisionTimeBucket]:
    """Finds months that this person was on supervision for the given StateSupervisionPeriod, where the person was not
    incarcerated for the full month and did not have a revocation admission that month.

    Args:
        - supervision_sentences: List of StateSupervisionSentences for a person
        - incarceration_sentences: List of StateIncarcerationSentence for a person
        - supervision_period: The supervision period the person was on
        - supervision_period_index: Class containing information about this person's supervision periods
        - incarceration_period_index: Class containing information about this person's incarceration periods
        - assessments: List of StateAssessment for a person
        - violation_responses: List of StateSupervisionViolationResponse for a person
        - supervision_period_to_agent_associations: dictionary associating StateSupervisionPeriod ids to information
            about the corresponding StateAgent on the period
    Returns
        - A set of unique SupervisionTimeBuckets for the person for the given StateSupervisionPeriod.
    """
    supervision_month_buckets: List[SupervisionTimeBucket] = []

    start_date = supervision_period.start_date
    termination_date = supervision_period.termination_date

    if start_date is None:
        return supervision_month_buckets

    start_of_month = first_day_of_month(start_date)

    # The last month this person will count towards supervision population is the month of the last full day on
    # supervision.
    month_upper_bound = \
        first_day_of_month(termination_date - timedelta(days=1)) \
        if termination_date else first_day_of_month(date.today())

    while start_of_month <= month_upper_bound:
        if month_is_non_revocation_supervision_bucket(
                start_of_month,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                incarceration_period_index):

            supervision_type = get_month_supervision_type(
                start_of_month, supervision_sentences, incarceration_sentences, supervision_period)
            end_of_month = last_day_of_month(start_of_month)

            assessment_score = None
            assessment_level = None
            assessment_type = None

            most_recent_assessment = find_most_recent_assessment(end_of_month, assessments)

            if most_recent_assessment:
                assessment_score = most_recent_assessment.assessment_score
                assessment_level = most_recent_assessment.assessment_level
                assessment_type = most_recent_assessment.assessment_type

            supervising_officer_external_id, supervising_district_external_id = \
                _get_supervising_officer_and_district(supervision_period, supervision_period_to_agent_associations)

            case_type = _identify_most_severe_case_type(supervision_period)

            end_of_violation_window = \
                end_of_month if (termination_date is None or end_of_month < termination_date) else termination_date
            violation_history = get_violation_and_response_history(end_of_violation_window, violation_responses)

            is_on_supervision_last_day_of_month = _get_is_on_supervision_last_day_of_month(
                end_of_month,
                incarceration_period_index,
                supervision_sentences,
                incarceration_sentences,
                supervision_period
            )

            case_compliance: Optional[SupervisionCaseCompliance] = None

            # For now, we are only calculating case compliance at the end of each month
            if is_on_supervision_last_day_of_month:
                if not supervision_period.supervision_period_id:
                    raise ValueError("Unexpected supervision period without a supervision_period_id.")

                start_of_supervision = supervision_period_index.supervision_start_dates_by_period_id.get(
                    supervision_period.supervision_period_id)

                if not start_of_supervision:
                    raise ValueError("SupervisionPeriodIndex.supervision_start_dates_by_period_id incomplete.")

                case_compliance = get_case_compliance_on_date(
                    supervision_period, start_of_supervision, end_of_month, most_recent_assessment)

            supervision_month_buckets.append(
                NonRevocationReturnSupervisionTimeBucket(
                    state_code=supervision_period.state_code,
                    year=start_of_month.year,
                    month=start_of_month.month,
                    supervision_type=supervision_type,
                    case_type=case_type,
                    assessment_score=assessment_score,
                    assessment_level=assessment_level,
                    assessment_type=assessment_type,
                    most_severe_violation_type=violation_history.most_severe_violation_type,
                    most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                    response_count=violation_history.response_count,
                    supervising_officer_external_id=supervising_officer_external_id,
                    supervising_district_external_id=supervising_district_external_id,
                    supervision_level=supervision_period.supervision_level,
                    supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                    is_on_supervision_last_day_of_month=is_on_supervision_last_day_of_month,
                    case_compliance=case_compliance
                )
            )

        start_of_month = start_of_month + relativedelta(months=1)

    return supervision_month_buckets


def has_revocation_admission_in_month(
        date_in_month: date,
        incarceration_period_index: IncarcerationPeriodIndex) -> bool:

    incarceration_periods_by_admission_month = incarceration_period_index.incarceration_periods_by_admission_month
    if date_in_month.year not in incarceration_periods_by_admission_month or \
            date_in_month.month not in incarceration_periods_by_admission_month[date_in_month.year]:
        return False

    # An admission to prison happened during this month
    incarceration_periods = incarceration_periods_by_admission_month[date_in_month.year][date_in_month.month]
    for incarceration_period in incarceration_periods:
        ip_index = incarceration_period_index.incarceration_periods.index(incarceration_period)
        preceding_incarceration_period = None

        if ip_index > 0:
            preceding_incarceration_period = incarceration_period_index.incarceration_periods[ip_index - 1]

        if incarceration_period_is_from_revocation(incarceration_period, preceding_incarceration_period):
            return True
    return False


def supervision_period_counts_towards_supervision_population_in_date_range(
        date_range: TimeRange,
        incarceration_period_index: IncarcerationPeriodIndex,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod) -> bool:
    """Returns True if the existence of the |supervision_period| means a person can be counted towards the supervision
    population in the provided date range.
    """

    is_fully_incarcerated_for_range = incarceration_period_index.is_fully_incarcerated_for_range(date_range)

    if is_fully_incarcerated_for_range:
        return False

    supervision_overlapping_range = \
        TimeRangeDiff(TimeRange.for_supervision_period(supervision_period), date_range).overlapping_range

    if not supervision_overlapping_range:
        return False

    return supervision_period_counts_towards_supervision_population_in_date_range_state_specific(
        date_range=date_range,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
        supervision_period=supervision_period
    )


def month_is_non_revocation_supervision_bucket(
        start_of_month: date,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod,
        incarceration_period_index: IncarcerationPeriodIndex):
    """Determines whether the given month was a month on supervision without a revocation and without being
    incarcerated for the full time spent on supervision that month.
    """

    if has_revocation_admission_in_month(start_of_month, incarceration_period_index):
        return False

    month_range = TimeRange.for_month(start_of_month.year, start_of_month.month)
    sp_range = TimeRange.for_supervision_period(supervision_period)
    overlapping_range = TimeRangeDiff(range_1=month_range, range_2=sp_range).overlapping_range

    if not overlapping_range:
        return False

    supervision_period_counts_towards_supervision_population_in_month = \
        supervision_period_counts_towards_supervision_population_in_date_range(
            date_range=overlapping_range,
            supervision_sentences=supervision_sentences,
            incarceration_sentences=incarceration_sentences,
            supervision_period=supervision_period,
            incarceration_period_index=incarceration_period_index)

    return supervision_period_counts_towards_supervision_population_in_month


def find_supervision_termination_bucket(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod,
        supervision_period_index: SupervisionPeriodIndex,
        assessments: List[StateAssessment],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        incarceration_period_index: IncarcerationPeriodIndex
) -> Optional[SupervisionTimeBucket]:
    """Identifies an instance of supervision termination. If the given supervision_period has a valid start_date and
    termination_date, then returns a SupervisionTerminationBucket with the details of the termination.

    Calculates the change in assessment score from the beginning of supervision to the termination of supervision.
    This is done by identifying the first reassessment (or, the second score) and the last score during a
    supervision period, and taking the difference between the last score and the first reassessment score.

    If a person has multiple supervision periods that end in a given month, the the earliest start date and the latest
    termination date of the periods is used to estimate the start and end dates of the supervision. These dates
    are then used to determine what the second and last assessment scores are.

    If this supervision does not have a termination_date, then None is returned.
    """
    if supervision_period.start_date is not None and supervision_period.termination_date is not None:

        supervision_period_counts_towards_supervision_population_at_any_point = \
            supervision_period_counts_towards_supervision_population_in_date_range(
                date_range=TimeRange.for_supervision_period(supervision_period),
                supervision_sentences=supervision_sentences,
                incarceration_sentences=incarceration_sentences,
                supervision_period=supervision_period,
                incarceration_period_index=incarceration_period_index)

        if not supervision_period_counts_towards_supervision_population_at_any_point:
            # If no portion of the supervision period counts towards the supervision population at any point, do not
            # emit a termination bucket.
            return None

        start_date = supervision_period.start_date
        termination_date = supervision_period.termination_date

        termination_year = termination_date.year
        termination_month = termination_date.month

        periods_terminated_in_year = supervision_period_index.supervision_periods_by_termination_month.get(
            termination_year)

        if periods_terminated_in_year:
            periods_terminated_in_month = periods_terminated_in_year.get(termination_month)

            if periods_terminated_in_month:
                for period in periods_terminated_in_month:
                    if period.start_date and period.start_date < start_date:
                        start_date = period.start_date

                    if period.termination_date and period.termination_date > termination_date:
                        termination_date = period.termination_date

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            find_assessment_score_change(
                start_date, termination_date, assessments)

        supervising_officer_external_id, supervising_district_external_id = \
            _get_supervising_officer_and_district(supervision_period, supervision_period_to_agent_associations)

        case_type = _identify_most_severe_case_type(supervision_period)

        supervision_type = terminating_supervision_period_supervision_type(
            supervision_period, supervision_sentences, incarceration_sentences)

        return SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            termination_date=termination_date,
            year=termination_date.year,
            month=termination_date.month,
            supervision_type=supervision_type,
            case_type=case_type,
            assessment_score=end_assessment_score,
            assessment_level=end_assessment_level,
            assessment_type=end_assessment_type,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=assessment_score_change,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id
        )

    return None


RevocationDetails = NamedTuple('RevocationDetails', [
        ('revocation_type', Optional[StateSupervisionViolationResponseRevocationType]),
        ('source_violation_type', Optional[StateSupervisionViolationType]),
        ('supervising_officer_external_id', Optional[str]),
        ('supervising_district_external_id', Optional[str])])


def _get_revocation_details(incarceration_period: StateIncarcerationPeriod,
                            supervision_period: Optional[StateSupervisionPeriod],
                            ssvr_agent_associations: Dict[int, Dict[Any, Any]],
                            supervision_period_to_agent_associations:
                            Optional[Dict[int, Dict[Any, Any]]]) -> RevocationDetails:
    """Identifies the attributes of the revocation return from the supervision period that was revoked, if available,
     or the source_supervision_violation_response on the incarceration_period, if it is available.
    """
    revocation_type = None
    source_violation_type = None
    supervising_officer_external_id = None
    supervising_district_external_id = None

    source_violation_response = incarceration_period.source_supervision_violation_response

    if source_violation_response:
        response_decisions = source_violation_response.supervision_violation_response_decisions
        if response_decisions:
            revocation_type = _identify_most_severe_revocation_type(response_decisions)

        # TODO(2840): Remove this once revocation type is being set on the proper fields on the response decisions
        if revocation_type is None:
            revocation_type = source_violation_response.revocation_type

        source_violation = source_violation_response.supervision_violation
        if source_violation:
            source_violation_type, _ = identify_most_severe_violation_type_and_subtype([source_violation])

        supervision_violation_response_id = source_violation_response.supervision_violation_response_id

        if supervision_violation_response_id:
            agent_info = ssvr_agent_associations.get(supervision_violation_response_id)

            if agent_info is not None:
                supervising_officer_external_id = agent_info.get('agent_external_id')
                supervising_district_external_id = agent_info.get('district_external_id')

    if not supervising_officer_external_id and \
            default_to_supervision_period_officer_for_revocation_details_for_state(incarceration_period.state_code):
        if supervision_period and supervision_period_to_agent_associations:
            supervising_officer_external_id, supervising_district_external_id = \
                _get_supervising_officer_and_district(supervision_period, supervision_period_to_agent_associations)

    if revocation_type is None:
        # TODO(3341): Consider removing revocation_type and always looking at the specialized_purpose_for_incarceration
        #  on the revoked period
        if incarceration_period.specialized_purpose_for_incarceration == \
                StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON:
            revocation_type = StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON
        elif incarceration_period.specialized_purpose_for_incarceration == \
                StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION:
            revocation_type = StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
        else:
            # If the person is not admitted for treatment or shock incarceration, assume this is a reincarceration
            revocation_type = StateSupervisionViolationResponseRevocationType.REINCARCERATION

    revocation_details_result = RevocationDetails(
        revocation_type, source_violation_type, supervising_officer_external_id, supervising_district_external_id)

    return revocation_details_result


ViolationHistory = NamedTuple('ViolationHistory', [
        ('most_severe_violation_type', Optional[StateSupervisionViolationType]),
        ('most_severe_violation_type_subtype', Optional[str]),
        ('most_severe_response_decision', Optional[StateSupervisionViolationResponseDecision]),
        ('response_count', Optional[int]),
        ('violation_history_description', Optional[str]),
        ('violation_type_frequency_counter', Optional[List[List[str]]])])


def get_violation_and_response_history(
        revocation_date: date,
        violation_responses: List[StateSupervisionViolationResponse]
) -> ViolationHistory:
    """Identifies and returns the most severe violation type, the most severe decision on the responses, and the total
    number of responses that were recorded during a window of time preceding a revocation.
    """
    responses_in_window = _get_responses_in_window_before_revocation(revocation_date, violation_responses)

    violations_in_window: List[StateSupervisionViolation] = []
    response_decisions: List[StateSupervisionViolationResponseDecision] = []
    updated_responses: List[StateSupervisionViolationResponse] = []

    for response in responses_in_window:
        # TODO(2995): Formalize state-specific calc logic
        if response.state_code == 'US_MO':
            updated_responses.append(us_mo_violation_utils.normalize_violations_on_responses(response))
        else:
            updated_responses.append(response)

    for response in updated_responses:
        if response.supervision_violation:
            violations_in_window.append(response.supervision_violation)

        if response.supervision_violation_response_decisions:
            decision_entries = response.supervision_violation_response_decisions

            for decision_entry in decision_entries:
                if decision_entry.decision:
                    response_decisions.append(decision_entry.decision)

    # Find the most severe violation type info of all of the entries in the window
    most_severe_violation_type, most_severe_violation_type_subtype = \
        identify_most_severe_violation_type_and_subtype(violations_in_window)

    # Find the most severe decision in all of the responses in the window
    most_severe_response_decision = identify_most_severe_response_decision(response_decisions)

    violation_type_entries = []
    for violation in violations_in_window:
        violation_type_entries.extend(violation.supervision_violation_types)

    violation_history_description = _get_violation_history_description(violations_in_window)

    violation_type_frequency_counter = _get_violation_type_frequency_counter(violations_in_window)

    # Count the number of responses in the window
    response_count = len(responses_in_window)

    violation_history_result = ViolationHistory(
        most_severe_violation_type,
        most_severe_violation_type_subtype,
        most_severe_response_decision,
        response_count,
        violation_history_description,
        violation_type_frequency_counter)

    return violation_history_result


def _get_responses_in_window_before_revocation(revocation_date: date,
                                               violation_responses: List[StateSupervisionViolationResponse]) \
        -> List[StateSupervisionViolationResponse]:
    """Looks at the series of violation responses that preceded a revocation. Finds the last violation response that was
    written before the revocation_date. Then, returns the violation responses that were written within
    VIOLATION_HISTORY_WINDOW_MONTHS months of the response_date on that last response.
    """
    responses_before_revocation = [
        response for response in violation_responses
        if response.response_date is not None
        and not response.is_draft
        and response.response_type in (StateSupervisionViolationResponseType.VIOLATION_REPORT,
                                       StateSupervisionViolationResponseType.CITATION)
        and response.response_date <= revocation_date
    ]

    responses_before_revocation = filter_violation_responses_before_revocation(responses_before_revocation)

    if not responses_before_revocation:
        logging.warning("No recorded responses before the revocation date.")
        return []

    responses_before_revocation.sort(key=lambda b: b.response_date)

    last_response_before_revocation = responses_before_revocation[-1]

    if not last_response_before_revocation.response_date:
        # This should never happen, but is here to silence mypy warnings about empty response_dates.
        raise ValueError("Not effectively filtering out responses without valid response_dates.")

    history_cutoff_date = (last_response_before_revocation.response_date
                           - relativedelta(months=VIOLATION_HISTORY_WINDOW_MONTHS))

    responses_in_window = [
        response for response in responses_before_revocation
        if response.response_date is not None
        and history_cutoff_date <= response.response_date <= last_response_before_revocation.response_date
    ]

    return responses_in_window


def _get_violation_history_description(violations: List[StateSupervisionViolation]) -> Optional[str]:
    """Returns a string description of the violation history given the violation type entries. Tallies the number of
    each violation type, and then builds a string that lists the number of each of the represented types in the order
    listed in the violation_type_shorthand dictionary and separated by a semicolon.

    For example, if someone has 3 felonies and 2 technicals, this will return '3fel;2tech'.
    """
    if not violations:
        return None

    state_code = violations[0].state_code
    ranked_violation_type_and_subtype_counts: Dict[str, int] = {}

    if state_code.upper() == 'US_MO':
        ranked_violation_type_and_subtype_counts = \
            us_mo_violation_utils.get_ranked_violation_type_and_subtype_counts(violations)

    descriptions = [f"{count}{label}" for label, count in
                    ranked_violation_type_and_subtype_counts.items() if count > 0]

    if descriptions:
        return ';'.join(descriptions)

    return None


def _get_violation_type_frequency_counter(violations: List[StateSupervisionViolation]) -> Optional[List[List[str]]]:
    """For every violation in violations, builds a list of strings, where each string is a violation type or a
    condition violated that is recorded on the given violation. Returns a list of all lists of strings, where the length
    of the list is the number of violations."""
    violation_type_frequency_counter: List[List[str]] = []

    for violation in violations:
        violation_type_list: List[str] = []

        includes_technical_violation = False

        for violation_type_entry in violation.supervision_violation_types:
            if violation_type_entry.violation_type and \
                    violation_type_entry.violation_type != StateSupervisionViolationType.TECHNICAL:
                violation_type_list.append(violation_type_entry.violation_type.value)
            else:
                includes_technical_violation = True

        for condition_entry in violation.supervision_violated_conditions:
            condition = condition_entry.condition
            if condition:
                # Condition values are free text so we standardize all to be upper case
                violation_type_list.append(condition.upper())

        # If there are no stored violation types, but there was a TECHNICAL violation type present, then mark this as a
        # technical violation without conditions
        if not violation_type_list and includes_technical_violation:
            violation_type_list.append('TECHNICAL_NO_CONDITIONS')

        violation_type_frequency_counter.append(violation_type_list)

    return violation_type_frequency_counter if violation_type_frequency_counter else None


def _get_is_on_supervision_last_day_of_month(
        any_date_in_month: date,
        incarceration_period_index: IncarcerationPeriodIndex,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod
) -> bool:
    if date.today() < last_day_of_month(any_date_in_month):
        return False

    end_of_month = last_day_of_month(any_date_in_month)
    first_of_next_month = first_day_of_next_month(any_date_in_month)

    return supervision_period_counts_towards_supervision_population_in_date_range(
        date_range=TimeRange(end_of_month, first_of_next_month),
        supervision_sentences=supervision_sentences,
        incarceration_sentences=incarceration_sentences,
        supervision_period=supervision_period,
        incarceration_period_index=incarceration_period_index)


def find_revocation_return_buckets(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_periods: List[StateSupervisionPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        incarceration_period_index: IncarcerationPeriodIndex
) -> List[SupervisionTimeBucket]:
    """Looks at all incarceration periods to see if they were revocation returns. For each revocation admission, adds
    one RevocationReturnSupervisionTimeBuckets for each overlapping supervision period. If there are no overlapping
    supervision periods, looks for a recently terminated period. If there are no overlapping or recently terminated
    supervision periods, adds a RevocationReturnSupervisionTimeBuckets with as much information about the time on
    supervision as possible.
    """
    revocation_return_buckets: List[SupervisionTimeBucket] = []

    if not incarceration_period_index.incarceration_periods:
        return revocation_return_buckets

    filtered_supervision_periods = filter_supervision_periods_for_revocation_identification(supervision_periods)

    for index, incarceration_period in enumerate(incarceration_period_index.incarceration_periods):
        if not incarceration_period.admission_date:
            raise ValueError(f"Admission date for null for {incarceration_period}")

        previous_incarceration_period = (incarceration_period_index.incarceration_periods[index - 1]
                                         if index > 0 else None)

        admission_is_revocation, revoked_supervision_periods = _revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, filtered_supervision_periods, previous_incarceration_period)

        if not admission_is_revocation:
            continue

        admission_date = incarceration_period.admission_date
        admission_year = admission_date.year
        admission_month = admission_date.month

        assessment_score, assessment_level, assessment_type = most_recent_assessment_attributes(
            admission_date, assessments)

        if revoked_supervision_periods:
            # Add a RevocationReturnSupervisionTimeBucket for each supervision period that was revoked
            for supervision_period in revoked_supervision_periods:
                revocation_details = _get_revocation_details(
                    incarceration_period, supervision_period,
                    ssvr_agent_associations, supervision_period_to_agent_associations)

                pre_revocation_supervision_type = get_pre_revocation_supervision_type(
                    incarceration_sentences, supervision_sentences, incarceration_period, supervision_period)

                case_type = _identify_most_severe_case_type(supervision_period)
                supervision_level = supervision_period.supervision_level
                supervision_level_raw_text = supervision_period.supervision_level_raw_text

                # Get details about the violation and response history leading up to the revocation
                violation_history = get_violation_and_response_history(admission_date, violation_responses)

                if pre_revocation_supervision_type is not None:
                    revocation_month_bucket = RevocationReturnSupervisionTimeBucket(
                        state_code=incarceration_period.state_code,
                        year=admission_year,
                        month=admission_month,
                        revocation_admission_date=admission_date,
                        supervision_type=pre_revocation_supervision_type,
                        case_type=case_type,
                        assessment_score=assessment_score,
                        assessment_level=assessment_level,
                        assessment_type=assessment_type,
                        revocation_type=revocation_details.revocation_type,
                        source_violation_type=revocation_details.source_violation_type,
                        most_severe_violation_type=violation_history.most_severe_violation_type,
                        most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                        most_severe_response_decision=violation_history.most_severe_response_decision,
                        response_count=violation_history.response_count,
                        violation_history_description=violation_history.violation_history_description,
                        violation_type_frequency_counter=violation_history.violation_type_frequency_counter,
                        supervising_officer_external_id=revocation_details.supervising_officer_external_id,
                        supervising_district_external_id=revocation_details.supervising_district_external_id,
                        supervision_level=supervision_level,
                        supervision_level_raw_text=supervision_level_raw_text,
                        # Note: This is incorrect in the case where you are revoked, then released by the end of the
                        #  month to a new supervision period that overlaps with EOM. We expect this case to be rare or
                        #  non-existent since we don't count temporary / board hold periods as revocations.
                        is_on_supervision_last_day_of_month=False
                    )

                    revocation_return_buckets.append(revocation_month_bucket)
        else:
            # There are no overlapping or proximal supervision periods. Add one
            # RevocationReturnSupervisionTimeBucket with as many details as possible about this revocation
            revocation_details = _get_revocation_details(
                incarceration_period, None, ssvr_agent_associations, None)

            pre_revocation_supervision_type = get_pre_revocation_supervision_type(
                incarceration_sentences, supervision_sentences, incarceration_period, None)

            # TODO(2853): Don't default to GENERAL once we figure out how to handle unset fields
            case_type = StateSupervisionCaseType.GENERAL

            # Get details about the violation and response history leading up to the revocation
            violation_history = get_violation_and_response_history(admission_date, violation_responses)

            if pre_revocation_supervision_type is not None:
                revocation_month_bucket = RevocationReturnSupervisionTimeBucket(
                    state_code=incarceration_period.state_code,
                    year=admission_year,
                    month=admission_month,
                    revocation_admission_date=admission_date,
                    supervision_type=pre_revocation_supervision_type,
                    case_type=case_type,
                    assessment_score=assessment_score,
                    assessment_level=assessment_level,
                    assessment_type=assessment_type,
                    revocation_type=revocation_details.revocation_type,
                    source_violation_type=revocation_details.source_violation_type,
                    most_severe_violation_type=violation_history.most_severe_violation_type,
                    most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                    most_severe_response_decision=violation_history.most_severe_response_decision,
                    response_count=violation_history.response_count,
                    violation_history_description=violation_history.violation_history_description,
                    violation_type_frequency_counter=violation_history.violation_type_frequency_counter,
                    supervising_officer_external_id=revocation_details.supervising_officer_external_id,
                    supervising_district_external_id=revocation_details.supervising_district_external_id,
                    # Note: This is incorrect in the case where you are revoked, then released by the end of the
                    #  month to a new supervision period that overlaps with EOM. We expect this case to be rare or
                    #  non-existent since we don't count temporary / board hold periods as revocations.
                    is_on_supervision_last_day_of_month=False
                )

                revocation_return_buckets.append(revocation_month_bucket)

    return revocation_return_buckets


def classify_supervision_success(supervision_sentences: List[StateSupervisionSentence],
                                 incarceration_period_index: IncarcerationPeriodIndex,
                                 supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
                                 ) -> List[ProjectedSupervisionCompletionBucket]:
    """This classifies whether supervision projected to end in a given month was completed successfully.

    For supervision sentences with a projected_completion_date, where that date is before or on today's date,
    and the supervision sentence has a set completion_date, looks at all supervision periods that have terminated and
    finds the one with the latest termination date. From that supervision period, classifies the termination as either
    successful or not successful.
    """
    projected_completion_buckets: List[ProjectedSupervisionCompletionBucket] = []

    for supervision_sentence in supervision_sentences:
        sentence_start_date = supervision_sentence.start_date
        sentence_completion_date = supervision_sentence.completion_date
        projected_completion_date = supervision_sentence.projected_completion_date

        # These fields must be set to be included in any supervision success metrics
        if not sentence_start_date or not projected_completion_date:
            continue

        # Only include sentences that were supposed to end by now, have ended, and have a completion status on them
        if projected_completion_date <= date.today() and sentence_completion_date:
            latest_supervision_period = None

            for supervision_period in supervision_sentence.supervision_periods:
                termination_date = supervision_period.termination_date

                if termination_date:
                    if not latest_supervision_period \
                            or latest_supervision_period.termination_date < termination_date:
                        latest_supervision_period = supervision_period

            if latest_supervision_period:
                completion_bucket = _get_projected_completion_bucket(
                    supervision_sentence,
                    latest_supervision_period,
                    incarceration_period_index,
                    supervision_period_to_agent_associations
                )

                if completion_bucket:
                    projected_completion_buckets.append(completion_bucket)

    return projected_completion_buckets


def _get_projected_completion_bucket(
        supervision_sentence,
        supervision_period: StateSupervisionPeriod,
        incarceration_period_index: IncarcerationPeriodIndex,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> Optional[ProjectedSupervisionCompletionBucket]:
    """Returns a ProjectedSupervisionCompletionBucket for the given supervision sentence and its last terminated period,
    if the sentence should be included in the success metric counts. If the sentence should not be included in success
    metrics, then returns None."""
    if not _include_termination_in_success_metric(supervision_period.termination_reason):
        return None

    start_date = supervision_sentence.start_date
    completion_date = supervision_sentence.completion_date
    projected_completion_date = supervision_sentence.projected_completion_date

    if completion_date < start_date:
        logging.warning("Supervision sentence completion date is before the start date: %s", supervision_sentence)
        return None

    supervision_type = get_supervision_type_from_sentences(incarceration_sentences=[],
                                                           supervision_sentences=[supervision_sentence])

    sentence_days_served = (supervision_sentence.completion_date - supervision_sentence.start_date).days

    # TODO(2596): Assert that the sentence status is COMPLETED or COMMUTED to qualify as successful
    supervision_success = supervision_period.termination_reason in (StateSupervisionPeriodTerminationReason.DISCHARGE,
                                                                    StateSupervisionPeriodTerminationReason.EXPIRATION)

    incarcerated_during_sentence = incarceration_period_index.incarceration_admissions_between_dates(start_date,
                                                                                                     completion_date)

    supervising_officer_external_id, supervising_district_external_id = \
        _get_supervising_officer_and_district(supervision_period, supervision_period_to_agent_associations)

    case_type = _identify_most_severe_case_type(supervision_period)

    # TODO(2975): Note that this metric measures success by projected completion month. Update or expand this
    #  metric to capture the success of early termination as well
    return ProjectedSupervisionCompletionBucket(
        state_code=supervision_period.state_code,
        year=projected_completion_date.year,
        month=projected_completion_date.month,
        supervision_type=supervision_type,
        case_type=case_type,
        successful_completion=supervision_success,
        incarcerated_during_sentence=incarcerated_during_sentence,
        sentence_days_served=sentence_days_served,
        supervising_officer_external_id=supervising_officer_external_id,
        supervising_district_external_id=supervising_district_external_id
    )


def _include_termination_in_success_metric(termination_reason: Optional[StateSupervisionPeriodTerminationReason]):
    """Determines whether the given termination of supervision should be included in a supervision success metric."""
    # If this is the last supervision period termination status, there is some kind of data error and we cannot
    # determine whether this sentence ended successfully - exclude these entirely
    if not termination_reason or termination_reason in (
            StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN,
            StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
    ):
        return False

    # If this is the last supervision period termination status, then the supervision sentence ended in some sort of
    # truncated, not-necessarily successful manner unrelated to a failure on supervision - exclude these entirely
    if termination_reason in (
            StateSupervisionPeriodTerminationReason.DEATH,
            StateSupervisionPeriodTerminationReason.TRANSFER_OUT_OF_STATE,
            StateSupervisionPeriodTerminationReason.SUSPENSION
    ):
        return False

    # If the last period is an investigative period, then the person was never actually sentenced formally. In this case
    # we should not include the period in our "success" metrics.
    if termination_reason == StateSupervisionPeriodTerminationReason.INVESTIGATION:
        return False

    if termination_reason in (
            # Successful terminations
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            StateSupervisionPeriodTerminationReason.EXPIRATION,
            # Unsuccessful terminations
            StateSupervisionPeriodTerminationReason.ABSCONSION,
            StateSupervisionPeriodTerminationReason.REVOCATION,
            StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
    ):
        return True

    raise ValueError(f"Unexpected StateSupervisionPeriodTerminationReason: {termination_reason}")


def _get_supervising_officer_and_district(
        supervision_period: StateSupervisionPeriod,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]) \
        -> Tuple[Optional[str], Optional[str]]:

    supervising_officer_external_id = None
    supervising_district_external_id = supervision_period.supervision_site

    if supervision_period.supervision_period_id:
        agent_info = supervision_period_to_agent_associations.get(supervision_period.supervision_period_id)

        if agent_info is not None:
            supervising_officer_external_id = agent_info.get('agent_external_id')

            if supervising_district_external_id is None:
                supervising_district_external_id = agent_info.get('district_external_id')

    return supervising_officer_external_id, supervising_district_external_id


def _identify_most_severe_revocation_type(
        response_decision_entries:
        List[StateSupervisionViolationResponseDecisionEntry]) -> \
        Optional[StateSupervisionViolationResponseRevocationType]:
    """Identifies the most severe revocation type on the violation response
    according to the static revocation type ranking."""
    # Note: RETURN_TO_SUPERVISION is not included as a revocation type for a
    # revocation return to prison
    revocation_types = [resp.revocation_type for resp in response_decision_entries]

    return next((revocation_type for revocation_type in REVOCATION_TYPE_SEVERITY_ORDER
                 if revocation_type in revocation_types), None)


def _identify_most_severe_case_type(supervision_period: StateSupervisionPeriod) -> StateSupervisionCaseType:
    """Identifies the most severe supervision case type that the supervision period is classified as. If there are no
    case types on the period that are listed in the severity ranking, then StateSupervisionCaseType.GENERAL is
    returned."""
    case_type_entries = supervision_period.case_type_entries

    if case_type_entries:
        case_types = [entry.case_type for entry in case_type_entries]
    else:
        # TODO(2853): Don't default to GENERAL once we figure out how to handle unset fields
        case_types = [StateSupervisionCaseType.GENERAL]

    return next((case_type for case_type in CASE_TYPE_SEVERITY_ORDER
                 if case_type in case_types), StateSupervisionCaseType.GENERAL)


def _expand_dual_supervision_buckets(supervision_time_buckets: List[SupervisionTimeBucket]) -> \
        List[SupervisionTimeBucket]:
    """For any SupervisionTimeBuckets that are of DUAL supervision type, makes a copy of the bucket that has a PAROLE
    supervision type and a copy of the bucket that has a PROBATION supervision type. Returns all buckets, including the
    duplicated buckets for each of the DUAL supervision buckets, because we want these buckets to contribute to PAROLE,
    PROBATION, and DUAL breakdowns of any metric."""
    additional_supervision_months: List[SupervisionTimeBucket] = []

    for supervision_time_bucket in supervision_time_buckets:
        if supervision_time_bucket.supervision_type == StateSupervisionPeriodSupervisionType.DUAL:
            parole_copy = attr.evolve(
                supervision_time_bucket, supervision_type=StateSupervisionPeriodSupervisionType.PAROLE)

            additional_supervision_months.append(parole_copy)

            probation_copy = attr.evolve(
                supervision_time_bucket, supervision_type=StateSupervisionPeriodSupervisionType.PROBATION)

            additional_supervision_months.append(probation_copy)

    supervision_time_buckets.extend(additional_supervision_months)

    return supervision_time_buckets


# Each SupervisionMetricType with a list of the SupervisionTimeBuckets that contribute to that metric
BUCKET_TYPES_FOR_METRIC: Dict[SupervisionMetricType, List[Type[SupervisionTimeBucket]]] = {
    SupervisionMetricType.ASSESSMENT_CHANGE: [SupervisionTerminationBucket],
    SupervisionMetricType.COMPLIANCE: [NonRevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.POPULATION: [
        NonRevocationReturnSupervisionTimeBucket, RevocationReturnSupervisionTimeBucket
    ],
    SupervisionMetricType.REVOCATION: [RevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.REVOCATION_ANALYSIS: [RevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.REVOCATION_VIOLATION_TYPE_ANALYSIS: [RevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.SUCCESS: [ProjectedSupervisionCompletionBucket],
    SupervisionMetricType.SUCCESSFUL_SENTENCE_DAYS_SERVED: [ProjectedSupervisionCompletionBucket],
}


def _convert_buckets_to_dual(supervision_time_buckets: List[SupervisionTimeBucket]) -> \
        List[SupervisionTimeBucket]:
    """For some states, we want to track DUAL supervision as distinct from both PAROLE and PROBATION. For these states,
    if someone has two buckets in the same month that will contribute to the same type of metric, and these buckets are
    of different supervision types (one is PAROLE and one is PROBATION, or one is DUAL and the other is something other
    than DUAL), then we want that person to only contribute to metrics with a supervision type of DUAL. All buckets of
    that type in that month are then replaced with ones that have DUAL as the set supervision_type.

    Returns an updated list of SupervisionTimeBuckets.
    """
    buckets_by_month: Dict[int, Dict[int, List[SupervisionTimeBucket]]] = defaultdict(lambda: defaultdict(list))

    for bucket in supervision_time_buckets:
        year = bucket.year
        month = bucket.month

        buckets_by_month[year][month].append(bucket)

    month_bucket_groups = [
        month_bucket_group for _, months in buckets_by_month.items()
        for _, month_bucket_group in months.items()
    ]

    updated_supervision_time_buckets: List[SupervisionTimeBucket] = []

    for month_bucket_group in month_bucket_groups:
        for _, bucket_types in BUCKET_TYPES_FOR_METRIC.items():
            buckets_for_this_metric = [
                bucket for bucket in month_bucket_group
                for bucket_type in bucket_types
                if isinstance(bucket, bucket_type)
            ]

            # If there is more than one bucket for this metric in this month
            if buckets_for_this_metric and len(buckets_for_this_metric) > 1:
                parole_buckets = _get_buckets_with_supervision_type(buckets_for_this_metric,
                                                                    StateSupervisionPeriodSupervisionType.PAROLE)
                probation_buckets = \
                    _get_buckets_with_supervision_type(buckets_for_this_metric,
                                                       StateSupervisionPeriodSupervisionType.PROBATION)
                dual_buckets = _get_buckets_with_supervision_type(buckets_for_this_metric,
                                                                  StateSupervisionPeriodSupervisionType.DUAL)

                # If they were on both parole and probation in this month, change every bucket for this metric
                # to have a supervision type of DUAL
                if (parole_buckets and probation_buckets) or dual_buckets:
                    for bucket in buckets_for_this_metric:
                        updated_bucket = attr.evolve(
                            bucket, supervision_type=StateSupervisionPeriodSupervisionType.DUAL)
                        month_bucket_group.remove(bucket)
                        month_bucket_group.append(updated_bucket)

        updated_supervision_time_buckets.extend(month_bucket_group)

    return updated_supervision_time_buckets


def _get_buckets_with_supervision_type(buckets: List[SupervisionTimeBucket],
                                       supervision_type: StateSupervisionPeriodSupervisionType) -> \
        List[SupervisionTimeBucket]:
    """Returns each SupervisionTimeBucket in buckets if the supervision_type on the bucket matches the given
    supervision_type."""
    return [
        bucket for bucket in buckets
        if bucket.supervision_type == supervision_type
    ]


def _revoked_supervision_periods_if_revocation_occurred(
        incarceration_period: StateIncarcerationPeriod,
        supervision_periods: List[StateSupervisionPeriod],
        preceding_incarceration_period: Optional[StateIncarcerationPeriod]) -> \
        Tuple[bool, List[StateSupervisionPeriod]]:
    """If the incarceration period was a result of a supervision revocation, finds the supervision periods that were
    revoked.

    Returns False, [] if the incarceration period was not a result of a revocation. Returns True and the list of
    supervision periods that were revoked if the incarceration period was a result of a revocation. In some cases, it's
    possible for the admission to be a revocation even though we cannot identify the corresponding supervision periods
    that were revoked (e.g. the person was serving supervision out-of-state). In these instances, this function will
    return True and an empty list [].
    """
    revoked_periods: List[StateSupervisionPeriod] = []

    if incarceration_period.state_code == 'US_ID':
        admission_is_revocation, revoked_period = \
            us_id_revoked_supervision_period_if_revocation_occurred(
                incarceration_period, supervision_periods, preceding_incarceration_period
            )

        if revoked_period:
            revoked_periods = [revoked_period]
    else:
        admission_is_revocation = is_revocation_admission(incarceration_period.admission_reason)
        revoked_periods = get_relevant_supervision_periods_before_admission_date(incarceration_period.admission_date,
                                                                                 supervision_periods)

    return admission_is_revocation, revoked_periods
