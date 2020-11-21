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
from datetime import date
from typing import List, Dict, Tuple, Optional, Any, NamedTuple, Type, Set

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.metrics import SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, \
    NonRevocationReturnSupervisionTimeBucket, \
    ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.calculator.pipeline.utils.execution_utils import list_of_dicts_to_dict_with_keys
from recidiviz.calculator.pipeline.utils.calculator_utils import identify_most_severe_response_decision
from recidiviz.calculator.pipeline.utils.incarceration_period_index import IncarcerationPeriodIndex
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    supervision_types_mutually_exclusive_for_state, \
    default_to_supervision_period_officer_for_revocation_details_for_state, get_month_supervision_type, \
    terminating_supervision_period_supervision_type, \
    supervision_period_counts_towards_supervision_population_in_date_range_state_specific, \
    filter_violation_responses_before_revocation, \
    should_collapse_transfers_different_purpose_for_incarceration, incarceration_period_is_from_revocation, \
    filter_supervision_periods_for_revocation_identification, get_pre_revocation_supervision_type, \
    should_produce_supervision_time_bucket_for_period, only_state_custodial_authority_in_supervision_population, \
    get_case_compliance_on_date, include_decisions_on_follow_up_responses, \
    second_assessment_on_supervision_is_more_reliable, get_supervision_district_from_supervision_period, \
    revoked_supervision_periods_if_revocation_occurred, \
    state_specific_violation_response_pre_processing_function
from recidiviz.calculator.pipeline.utils.supervision_period_index import SupervisionPeriodIndex
from recidiviz.calculator.pipeline.utils.supervision_period_utils import prepare_supervision_periods_for_calculations
from recidiviz.calculator.pipeline.utils.supervision_type_identification import \
    get_supervision_type_from_sentences
from recidiviz.calculator.pipeline.utils.violation_utils import identify_most_severe_violation_type_and_subtype, \
    shorthand_description_for_ranked_violation_counts, get_violation_type_frequency_counter, \
    prepare_violation_responses_for_calculations
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel, StateAssessmentType, \
    StateAssessmentClass
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import StateSpecializedPurposeForIncarceration
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecision, StateSupervisionViolationResponseType
from recidiviz.common.date import DateRange, DateRangeDiff, last_day_of_month
from recidiviz.persistence.entity.entity_utils import get_single_state_code
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolationResponseDecisionEntry, StateSupervisionSentence, \
    StateAssessment, StateSupervisionViolation, \
    StateSupervisionViolationResponse, StateIncarcerationSentence, StateSupervisionContact

# The number of months for the window of time prior to a revocation return in which violations and violation responses
# should be considered when producing metrics related to a person's violation history leading up to the revocation
VIOLATION_HISTORY_WINDOW_MONTHS = 12


REVOCATION_TYPE_SEVERITY_ORDER = [
    StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
    StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,
    StateSupervisionViolationResponseRevocationType.REINCARCERATION
]

CASE_TYPE_SEVERITY_ORDER = [
    StateSupervisionCaseType.SEX_OFFENSE,
    StateSupervisionCaseType.DOMESTIC_VIOLENCE,
    StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS,
    # Diversionary courts
    StateSupervisionCaseType.DRUG_COURT,
    StateSupervisionCaseType.MENTAL_HEALTH_COURT,
    StateSupervisionCaseType.FAMILY_COURT,
    StateSupervisionCaseType.VETERANS_COURT,
    # End Diversionary courts
    StateSupervisionCaseType.ALCOHOL_DRUG,
    StateSupervisionCaseType.GENERAL
]


def find_supervision_time_buckets(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        supervision_contacts: List[StateSupervisionContact],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_judicial_district_association: List[Dict[str, Any]],
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
        - supervision_period_judicial_district_association: a list of dictionaries with information connecting
            StateSupervisionPeriod ids to the judicial district responsible for the period of supervision

    Returns:
        A list of SupervisionTimeBuckets for the person.
    """
    if not supervision_periods and not incarceration_periods:
        return []

    if supervision_periods:
        state_code = get_single_state_code(supervision_periods)
    else:
        state_code = get_single_state_code(incarceration_periods)

    supervision_period_to_judicial_district_associations = list_of_dicts_to_dict_with_keys(
        supervision_period_judicial_district_association, StateSupervisionPeriod.get_class_id_name())

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
        state_code,
        incarceration_periods,
        collapse_transfers=True,
        collapse_temporary_custody_periods_with_revocation=False,
        collapse_transfers_with_different_pfi=should_collapse_transfers_with_different_pfi,
        overwrite_facility_information_in_transfers=True)

    supervision_period_index = SupervisionPeriodIndex(supervision_periods=supervision_periods)
    incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=incarceration_periods)

    violation_responses = prepare_violation_responses_for_calculations(
        violation_responses=violation_responses,
        pre_processing_function=state_specific_violation_response_pre_processing_function(state_code=state_code)
    )

    projected_supervision_completion_buckets = classify_supervision_success(
        supervision_sentences,
        incarceration_period_index,
        supervision_period_to_agent_associations,
        supervision_period_to_judicial_district_associations)

    supervision_time_buckets.extend(projected_supervision_completion_buckets)

    for supervision_period in supervision_period_index.supervision_periods:
        if should_produce_supervision_time_bucket_for_period(supervision_period,
                                                             incarceration_sentences,
                                                             supervision_sentences):
            judicial_district_code = _get_judicial_district_code(
                supervision_period, supervision_period_to_judicial_district_associations
            )

            supervision_time_buckets = supervision_time_buckets + find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                supervision_period_to_agent_associations,
                judicial_district_code
            )

            supervision_termination_bucket = find_supervision_termination_bucket(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                assessments,
                violation_responses,
                supervision_period_to_agent_associations,
                judicial_district_code
            )

            if supervision_termination_bucket:
                supervision_time_buckets.append(supervision_termination_bucket)

    supervision_time_buckets = supervision_time_buckets + find_revocation_return_buckets(
        supervision_sentences,
        incarceration_sentences,
        supervision_periods,
        assessments,
        violation_responses,
        ssvr_agent_associations,
        supervision_period_to_agent_associations,
        supervision_period_to_judicial_district_associations,
        incarceration_period_index)

    if supervision_types_mutually_exclusive_for_state(state_code):
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
        supervision_contacts: List[StateSupervisionContact],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        judicial_district_code: Optional[str] = None
) -> List[SupervisionTimeBucket]:
    """Finds days that this person was on supervision for the given StateSupervisionPeriod, where the person was not
    incarcerated and did not have a revocation admission that day.

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
        - judicial_district_code: The judicial district responsible for the period of supervision
    Returns
        - A set of unique SupervisionTimeBuckets for the person for the given StateSupervisionPeriod.
    """
    supervision_day_buckets: List[SupervisionTimeBucket] = []

    start_date = supervision_period.start_date
    termination_date = supervision_period.termination_date

    if start_date is None:
        return supervision_day_buckets

    bucket_date = start_date

    end_date = termination_date if termination_date else date.today() + relativedelta(days=1)

    while bucket_date < end_date:
        if on_supervision_on_date(
                bucket_date,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                incarceration_period_index):

            supervision_type = get_month_supervision_type(
                bucket_date, supervision_sentences, incarceration_sentences, supervision_period)

            assessment_score = None
            assessment_level = None
            assessment_type = None

            most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                bucket_date,
                assessments,
                assessment_class=StateAssessmentClass.RISK,
                state_code=supervision_period.state_code
            )

            if most_recent_assessment:
                assessment_score = most_recent_assessment.assessment_score
                assessment_level = most_recent_assessment.assessment_level
                assessment_type = most_recent_assessment.assessment_type

            supervising_officer_external_id, supervising_district_external_id = \
                _get_supervising_officer_and_district(supervision_period, supervision_period_to_agent_associations)

            case_type = _identify_most_severe_case_type(supervision_period)

            violation_history = get_violation_and_response_history(supervision_period.state_code,
                                                                   bucket_date,
                                                                   violation_responses)

            is_on_supervision_last_day_of_month = (bucket_date == last_day_of_month(bucket_date))

            case_compliance: Optional[SupervisionCaseCompliance] = None

            # For now, we are only calculating case compliance at the end of each month
            if is_on_supervision_last_day_of_month:
                if not supervision_period.supervision_period_id:
                    raise ValueError("Unexpected supervision period without a supervision_period_id.")

                start_of_supervision = supervision_period_index.supervision_start_dates_by_period_id.get(
                    supervision_period.supervision_period_id)

                if not start_of_supervision:
                    raise ValueError("SupervisionPeriodIndex.supervision_start_dates_by_period_id incomplete.")

                case_compliance = get_case_compliance_on_date(supervision_period,
                                                              case_type,
                                                              start_of_supervision,
                                                              bucket_date,
                                                              assessments,
                                                              supervision_contacts)

            supervision_day_buckets.append(
                NonRevocationReturnSupervisionTimeBucket(
                    state_code=supervision_period.state_code,
                    year=bucket_date.year,
                    month=bucket_date.month,
                    bucket_date=bucket_date,
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
                    case_compliance=case_compliance,
                    judicial_district_code=judicial_district_code
                )
            )

        bucket_date = bucket_date + relativedelta(days=1)

    return supervision_day_buckets


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
        ip_index = incarceration_period_index.incarceration_periods.index(incarceration_period)
        preceding_incarceration_period = None

        if ip_index > 0:
            preceding_incarceration_period = incarceration_period_index.incarceration_periods[ip_index - 1]

        if incarceration_period_is_from_revocation(incarceration_period, preceding_incarceration_period):
            return True
    return False


def supervision_period_counts_towards_supervision_population_in_date_range(
        date_range: DateRange,
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
        DateRangeDiff(supervision_period.duration, date_range).overlapping_range

    if not supervision_overlapping_range:
        return False

    return supervision_period_counts_towards_supervision_population_in_date_range_state_specific(
        date_range=date_range,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
        supervision_period=supervision_period
    )


def on_supervision_on_date(
        evaluation_date: date,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod,
        incarceration_period_index: IncarcerationPeriodIndex):
    """Determines whether the person was on supervision on a given date. We do not count someone as being on supervision
     for a given date if they were incarcerated or revoked that day."""
    if has_revocation_admission_on_date(evaluation_date, incarceration_period_index):
        return False

    # This should never happen
    if not supervision_period.duration.contains_day(evaluation_date):
        raise ValueError("evaluation_date must fall between the start and end of the supervision_period")

    day_range = DateRange.for_day(evaluation_date)

    supervision_period_counts_towards_supervision_population_on_date = \
        supervision_period_counts_towards_supervision_population_in_date_range(
            date_range=day_range,
            supervision_sentences=supervision_sentences,
            incarceration_sentences=incarceration_sentences,
            supervision_period=supervision_period,
            incarceration_period_index=incarceration_period_index)

    return supervision_period_counts_towards_supervision_population_on_date


def find_supervision_termination_bucket(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod,
        supervision_period_index: SupervisionPeriodIndex,
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        judicial_district_code: Optional[str] = None
) -> Optional[SupervisionTimeBucket]:
    """Identifies an instance of supervision termination. If the given supervision_period has a valid start_date and
    termination_date, then returns a SupervisionTerminationBucket with the details of the termination.

    Calculates the change in assessment score from the beginning of supervision to the termination of supervision.
    This is done by identifying the first reassessment (or, the second score) and the last score during a
    supervision period, and taking the difference between the last score and the first reassessment score.

    If a person has multiple supervision periods that end in a given month, the the earliest start date and the latest
    termination date of the periods is used to estimate the start and end dates of the supervision. These dates
    are then used to determine what the second and last assessment scores are. However, the termination_date on the
    SupervisionTerminationBucket will always be the termination_date on the supervision_period.

    If this supervision does not have a termination_date, then None is returned.
    """
    if supervision_period.start_date is not None and supervision_period.termination_date is not None:
        assessment_start_date = supervision_period.start_date
        termination_date = supervision_period.termination_date
        assessment_termination_date = supervision_period.termination_date

        termination_year = termination_date.year
        termination_month = termination_date.month

        periods_terminated_in_year = supervision_period_index.supervision_periods_by_termination_month.get(
            termination_year)

        if periods_terminated_in_year:
            periods_terminated_in_month = periods_terminated_in_year.get(termination_month)

            if periods_terminated_in_month:
                for period in periods_terminated_in_month:
                    if period.start_date and period.start_date < assessment_start_date:
                        assessment_start_date = period.start_date

                    if period.termination_date and period.termination_date > termination_date:
                        assessment_termination_date = period.termination_date

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            find_assessment_score_change(
                supervision_period.state_code,
                assessment_start_date,
                assessment_termination_date,
                assessments)

        violation_history = get_violation_and_response_history(supervision_period.state_code,
                                                               termination_date,
                                                               violation_responses)

        supervising_officer_external_id, supervising_district_external_id = \
            _get_supervising_officer_and_district(supervision_period, supervision_period_to_agent_associations)

        case_type = _identify_most_severe_case_type(supervision_period)

        supervision_type = terminating_supervision_period_supervision_type(
            supervision_period, supervision_sentences, incarceration_sentences)

        return SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            bucket_date=termination_date,
            year=termination_date.year,
            month=termination_date.month,
            supervision_type=supervision_type,
            case_type=case_type,
            supervision_level=supervision_period.supervision_level,
            supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            assessment_score=end_assessment_score,
            assessment_level=end_assessment_level,
            assessment_type=end_assessment_type,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=assessment_score_change,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id,
            judicial_district_code=judicial_district_code,
            response_count=violation_history.response_count,
            most_severe_violation_type=violation_history.most_severe_violation_type,
            most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype
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

        # TODO(#2840): Remove this once revocation type is being set on the proper fields on the response decisions
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
        # TODO(#3341): Consider removing revocation_type and always looking at the specialized_purpose_for_incarceration
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
        state_code: str,
        end_date: date,
        violation_responses: List[StateSupervisionViolationResponse]
) -> ViolationHistory:
    """Identifies and returns the most severe violation type, the most severe decision on the responses, and the total
    number of responses that were recorded during a window of time preceding the |end_date|.
    """
    if not violation_responses:
        return ViolationHistory(
            most_severe_violation_type=None,
            most_severe_violation_type_subtype=None,
            most_severe_response_decision=None,
            response_count=0,
            violation_history_description=None,
            violation_type_frequency_counter=None)

    responses_in_window = _get_responses_in_window_before_revocation(end_date, violation_responses,
                                                                     include_follow_up_responses=False)

    violations_in_window: List[StateSupervisionViolation] = []
    violation_ids_in_window: Set[int] = set()
    response_decisions: List[StateSupervisionViolationResponseDecision] = []

    for response in responses_in_window:
        violation = response.supervision_violation

        if (violation and violation.supervision_violation_id
                and violation.supervision_violation_id not in violation_ids_in_window):
            violations_in_window.append(violation)
            violation_ids_in_window.add(violation.supervision_violation_id)

    # Find the most severe violation type info of all of the entries in the window
    most_severe_violation_type, most_severe_violation_type_subtype = \
        identify_most_severe_violation_type_and_subtype(violations_in_window)

    responses_in_window_for_decision_evaluation = responses_in_window

    if include_decisions_on_follow_up_responses(state_code):
        responses_in_window_for_decision_evaluation = _get_responses_in_window_before_revocation(
            end_date, violation_responses, include_follow_up_responses=True)

    most_severe_response_decision = None
    if responses_in_window_for_decision_evaluation:
        # Find the most recent response
        most_recent_response = max(responses_in_window_for_decision_evaluation,
                                   key=lambda b: b.response_date or date.min)

        if most_recent_response.supervision_violation_response_decisions:
            decision_entries = most_recent_response.supervision_violation_response_decisions

            for decision_entry in decision_entries:
                if decision_entry.decision:
                    response_decisions.append(decision_entry.decision)

        # Find the most severe decision on the most recent response
        most_severe_response_decision = identify_most_severe_response_decision(response_decisions)

    violation_type_entries = []
    for violation in violations_in_window:
        violation_type_entries.extend(violation.supervision_violation_types)

    violation_history_description = get_violation_history_description(violations_in_window)

    violation_type_frequency_counter = get_violation_type_frequency_counter(violations_in_window)

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
                                               violation_responses: List[StateSupervisionViolationResponse],
                                               include_follow_up_responses: bool) \
        -> List[StateSupervisionViolationResponse]:
    """Looks at the series of violation responses that preceded a revocation. Finds the last violation response that was
    written before the end_date. Then, returns the violation responses that were written within
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

    responses_before_revocation = filter_violation_responses_before_revocation(responses_before_revocation,
                                                                               include_follow_up_responses)

    if not responses_before_revocation:
        logging.warning("No recorded responses before the revocation date.")
        return []

    responses_before_revocation.sort(key=lambda b: b.response_date or date.min)

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


def find_revocation_return_buckets(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_periods: List[StateSupervisionPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_judicial_district_associations: Dict[int, Dict[Any, Any]],
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

        admission_is_revocation, revoked_supervision_periods = revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, filtered_supervision_periods, previous_incarceration_period)

        if not admission_is_revocation:
            continue

        admission_date = incarceration_period.admission_date
        admission_year = admission_date.year
        admission_month = admission_date.month

        assessment_score, assessment_level, assessment_type = \
            assessment_utils.most_recent_applicable_assessment_attributes_for_class(
                admission_date,
                assessments,
                assessment_class=StateAssessmentClass.RISK,
                state_code=incarceration_period.state_code
            )

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
                violation_history = get_violation_and_response_history(incarceration_period.state_code,
                                                                       admission_date,
                                                                       violation_responses)

                judicial_district_code = _get_judicial_district_code(
                    supervision_period, supervision_period_to_judicial_district_associations)

                if pre_revocation_supervision_type is not None:
                    revocation_month_bucket = RevocationReturnSupervisionTimeBucket(
                        state_code=incarceration_period.state_code,
                        year=admission_year,
                        month=admission_month,
                        bucket_date=admission_date,
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
                        is_on_supervision_last_day_of_month=False,
                        judicial_district_code=judicial_district_code
                    )

                    revocation_return_buckets.append(revocation_month_bucket)
        else:
            # There are no overlapping or proximal supervision periods. Add one
            # RevocationReturnSupervisionTimeBucket with as many details as possible about this revocation
            revocation_details = _get_revocation_details(
                incarceration_period, None, ssvr_agent_associations, None)

            pre_revocation_supervision_type = get_pre_revocation_supervision_type(
                incarceration_sentences, supervision_sentences, incarceration_period, None)

            case_type = StateSupervisionCaseType.GENERAL

            # Get details about the violation and response history leading up to the revocation
            violation_history = get_violation_and_response_history(incarceration_period.state_code,
                                                                   admission_date,
                                                                   violation_responses)

            if pre_revocation_supervision_type is not None:
                revocation_month_bucket = RevocationReturnSupervisionTimeBucket(
                    state_code=incarceration_period.state_code,
                    year=admission_year,
                    month=admission_month,
                    bucket_date=admission_date,
                    supervision_type=pre_revocation_supervision_type,
                    case_type=case_type,
                    supervision_level=None,
                    supervision_level_raw_text=None,
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
                    is_on_supervision_last_day_of_month=False,
                    judicial_district_code=None
                )

                revocation_return_buckets.append(revocation_month_bucket)

    return revocation_return_buckets


def classify_supervision_success(supervision_sentences: List[StateSupervisionSentence],
                                 incarceration_period_index: IncarcerationPeriodIndex,
                                 supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
                                 supervision_period_to_judicial_district_associations: Dict[int, Dict[Any, Any]]
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
                    supervision_period_to_agent_associations,
                    supervision_period_to_judicial_district_associations
                )

                if completion_bucket:
                    projected_completion_buckets.append(completion_bucket)

    return projected_completion_buckets


def _get_projected_completion_bucket(
        supervision_sentence,
        supervision_period: StateSupervisionPeriod,
        incarceration_period_index: IncarcerationPeriodIndex,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_judicial_district_associations: Dict[int, Dict[Any, Any]]
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

    # TODO(#2596): Assert that the sentence status is COMPLETED or COMMUTED to qualify as successful
    supervision_success = supervision_period.termination_reason in (StateSupervisionPeriodTerminationReason.DISCHARGE,
                                                                    StateSupervisionPeriodTerminationReason.EXPIRATION)

    incarcerated_during_sentence = incarceration_period_index.incarceration_admissions_between_dates(start_date,
                                                                                                     completion_date)

    supervising_officer_external_id, supervising_district_external_id = \
        _get_supervising_officer_and_district(supervision_period, supervision_period_to_agent_associations)

    case_type = _identify_most_severe_case_type(supervision_period)

    judicial_district_code = _get_judicial_district_code(supervision_period,
                                                         supervision_period_to_judicial_district_associations)

    last_day_of_projected_month = last_day_of_month(projected_completion_date)

    # TODO(#2975): Note that this metric measures success by projected completion month. Update or expand this
    #  metric to capture the success of early termination as well
    return ProjectedSupervisionCompletionBucket(
        state_code=supervision_period.state_code,
        year=projected_completion_date.year,
        month=projected_completion_date.month,
        bucket_date=last_day_of_projected_month,
        supervision_type=supervision_type,
        supervision_level=supervision_period.supervision_level,
        supervision_level_raw_text=supervision_period.supervision_level_raw_text,
        case_type=case_type,
        successful_completion=supervision_success,
        incarcerated_during_sentence=incarcerated_during_sentence,
        sentence_days_served=sentence_days_served,
        supervising_officer_external_id=supervising_officer_external_id,
        supervising_district_external_id=supervising_district_external_id,
        judicial_district_code=judicial_district_code
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
            StateSupervisionPeriodTerminationReason.COMMUTED,
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            StateSupervisionPeriodTerminationReason.DISMISSED,
            StateSupervisionPeriodTerminationReason.EXPIRATION,
            StateSupervisionPeriodTerminationReason.PARDONED,
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
    supervising_district_external_id = get_supervision_district_from_supervision_period(supervision_period)

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
    SupervisionMetricType.SUPERVISION_TERMINATION: [SupervisionTerminationBucket],
    SupervisionMetricType.SUPERVISION_COMPLIANCE: [NonRevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.SUPERVISION_POPULATION: [
        NonRevocationReturnSupervisionTimeBucket, RevocationReturnSupervisionTimeBucket
    ],
    SupervisionMetricType.SUPERVISION_REVOCATION: [RevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: [RevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS: [RevocationReturnSupervisionTimeBucket],
    SupervisionMetricType.SUPERVISION_SUCCESS: [ProjectedSupervisionCompletionBucket],
    SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED: [ProjectedSupervisionCompletionBucket],
}


def _convert_buckets_to_dual(supervision_time_buckets: List[SupervisionTimeBucket]) -> \
        List[SupervisionTimeBucket]:
    """For some states, we want to track DUAL supervision as distinct from both PAROLE and PROBATION. For these states,
    if someone has two buckets on the same day that will contribute to the same type of metric, and these buckets are
    of different supervision types (one is PAROLE and one is PROBATION, or one is DUAL and the other is something other
    than DUAL), then we want that person to only contribute to metrics with a supervision type of DUAL. All buckets of
    that type on that day are then replaced with ones that have DUAL as the set supervision_type.

    Returns an updated list of SupervisionTimeBuckets.
    """
    buckets_by_date: Dict[date, List[SupervisionTimeBucket]] = defaultdict(list)

    for bucket in supervision_time_buckets:
        bucket_date = bucket.bucket_date
        buckets_by_date[bucket_date].append(bucket)

    day_bucket_groups = list(buckets_by_date.values())

    updated_supervision_time_buckets: List[SupervisionTimeBucket] = []

    for day_bucket_group in day_bucket_groups:
        if len(day_bucket_group) < 2:
            # We only need to convert buckets if there are two that fall in the same time period
            updated_supervision_time_buckets.extend(day_bucket_group)
            continue

        for _, bucket_types in BUCKET_TYPES_FOR_METRIC.items():
            buckets_for_this_metric = [
                bucket for bucket in day_bucket_group
                for bucket_type in bucket_types
                if isinstance(bucket, bucket_type)
            ]

            # If there is more than one bucket for this metric on this day
            if buckets_for_this_metric and len(buckets_for_this_metric) > 1:
                parole_buckets = _get_buckets_with_supervision_type(buckets_for_this_metric,
                                                                    StateSupervisionPeriodSupervisionType.PAROLE)
                probation_buckets = \
                    _get_buckets_with_supervision_type(buckets_for_this_metric,
                                                       StateSupervisionPeriodSupervisionType.PROBATION)
                dual_buckets = _get_buckets_with_supervision_type(buckets_for_this_metric,
                                                                  StateSupervisionPeriodSupervisionType.DUAL)

                # If they were on both parole and probation on this day, change every bucket for this metric
                # to have a supervision type of DUAL
                if (parole_buckets and probation_buckets) or dual_buckets:
                    for bucket in buckets_for_this_metric:
                        updated_bucket = attr.evolve(
                            bucket, supervision_type=StateSupervisionPeriodSupervisionType.DUAL)
                        day_bucket_group.remove(bucket)
                        day_bucket_group.append(updated_bucket)

        updated_supervision_time_buckets.extend(day_bucket_group)

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


def _get_judicial_district_code(
        supervision_period: StateSupervisionPeriod,
        supervision_period_to_judicial_district: Dict[int, Dict[Any, Any]]) -> Optional[str]:
    """Retrieves the judicial_district_code corresponding to the supervision_period, if one exists."""
    supervision_period_id = supervision_period.supervision_period_id

    if supervision_period_id is None:
        raise ValueError("Unexpected unset supervision_period_id.")

    ip_info = supervision_period_to_judicial_district.get(supervision_period_id)

    if ip_info is not None:
        return ip_info.get('judicial_district_code')

    return None


def find_assessment_score_change(state_code: str,
                                 start_date: date,
                                 termination_date: date,
                                 assessments: List[StateAssessment]) -> \
        Tuple[Optional[int], Optional[int], Optional[StateAssessmentLevel], Optional[StateAssessmentType]]:
    """Finds the difference in scores between the last assessment that happened between the start_date and
    termination_date (inclusive) and the the first "reliable" assessment that was conducted after the start of
    supervision. The first "reliable" assessment is either the first or second assessment, depending on state-specific
    logic. Returns the assessment score change, the ending assessment score, the ending assessment level, and the
    assessment type. If there aren't enough assessments to compare, or the first reliable assessment and the last
    assessment are not of the same type, returns (None, None, None, None)."""
    if assessments:
        assessments_in_period = [
            assessment for assessment in assessments
            if assessment.assessment_date is not None
            and start_date <= assessment.assessment_date <= termination_date
        ]

        index_of_first_reliable_assessment = 1 if second_assessment_on_supervision_is_more_reliable(state_code) else 0
        min_assessments = 2 + index_of_first_reliable_assessment

        # If this person had less than the min number of assessments then we cannot compare the first reliable
        # assessment to the most recent assessment.
        if assessments_in_period and len(assessments_in_period) >= min_assessments:
            # Mypy complains that assessment_date might be None, even though that has already been filtered above.
            assessments_in_period.sort(key=lambda b: b.assessment_date) # type: ignore[arg-type,return-value]

            first_reliable_assessment = assessments_in_period[index_of_first_reliable_assessment]
            last_assessment = assessments_in_period[-1]

            # Assessments must be of the same type
            if last_assessment.assessment_type == first_reliable_assessment.assessment_type:
                first_reliable_assessment_date = first_reliable_assessment.assessment_date
                last_assessment_date = last_assessment.assessment_date

                # Ensure these assessments were actually issued on different days
                if (first_reliable_assessment_date and last_assessment_date
                        and last_assessment_date > first_reliable_assessment_date):
                    first_reliable_assessment_score = first_reliable_assessment.assessment_score
                    last_assessment_score = last_assessment.assessment_score

                    if first_reliable_assessment_score is not None and last_assessment_score is not None:
                        assessment_score_change = (last_assessment_score - first_reliable_assessment_score)

                        return (assessment_score_change,
                                last_assessment.assessment_score,
                                last_assessment.assessment_level,
                                last_assessment.assessment_type)

    return None, None, None, None


def get_violation_history_description(violations: List[StateSupervisionViolation]) -> Optional[str]:
    """Returns a string description of the violation history given the violation type entries. Tallies the number of
    each violation type, and then builds a string that lists the number of each of the represented types in the order
    listed in the violation_type_shorthand dictionary and separated by a semicolon.

    For example, if someone has 3 felonies and 2 technicals, this will return '3fel;2tech'.
    """
    if not violations:
        return None

    subtype_counts: Dict[str, int] = defaultdict(int)

    # Count all violation types and subtypes
    for violation in violations:
        most_severe_violation_type_and_subtype = identify_most_severe_violation_type_and_subtype([violation])
        if not most_severe_violation_type_and_subtype:
            continue
        _, most_severe_subtype = most_severe_violation_type_and_subtype

        if most_severe_subtype:
            subtype_counts[most_severe_subtype] += 1

    state_code = get_single_state_code(violations)

    return shorthand_description_for_ranked_violation_counts(state_code, subtype_counts)
