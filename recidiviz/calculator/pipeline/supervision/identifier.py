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
"""Identifies time buckets of supervision and classifies them as either
instances of revocation or not. Also classifies supervision sentences as
successfully completed or not."""
import logging
from collections import defaultdict
from datetime import date
from typing import List, Dict, Set, Tuple, Optional, Any, NamedTuple

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, \
    NonRevocationReturnSupervisionTimeBucket, \
    ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils import us_mo_utils
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    last_day_of_month, identify_most_severe_violation_type_and_subtype, \
    identify_most_severe_response_decision
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    find_most_recent_assessment, find_assessment_score_change
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodTerminationReason
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecision
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolationResponseDecisionEntry, StateSupervisionSentence, \
    StateAssessment, StateSupervisionViolation, \
    StateSupervisionViolationResponse

# The number of months for the window of time prior to a revocation return in which violations and violation responses
# should be considered when producing metrics related to a person's violation history leading up to the revocation
VIOLATION_HISTORY_WINDOW_MONTHS = 6

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
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        state_code: Optional[str] = 'ALL') \
        -> List[SupervisionTimeBucket]:
    """Finds buckets of time that a person was on supervision and determines
    if they resulted in revocation return.

    Transforms each StateSupervisionPeriod into years and months where the
    person spent any number of days on supervision. Excludes any time buckets
    in which the person was incarcerated for the entire time by looking through
    all of the person's StateIncarcerationPeriods. For each month and year that
    someone was on supervision, a SupervisionTimeBucket object is created. If
    the person was admitted to prison in that time for a revocation that matches
    the type of supervision the SupervisionTimeBucket describes, then that
    object is a RevocationSupervisionTimeBucket. If no applicable revocation
    occurs, then the object is of type NonRevocationSupervisionTimeBucket.

    If someone serving both probation and parole simultaneously,
    there will be one SupervisionTimeBucket for the probation type and one for
    the parole type. If someone has multiple overlapping supervision periods,
    there will be one SupervisionTimeBucket for each month and year on each
    supervision period.

    If a revocation return to prison occurs in a time bucket where there is no
    recorded supervision period, we count the individual as having been on
    supervision that time so that they can be included in the
    revocation return count and rate metrics for that bucket. In these cases,
    we add supplemental RevocationReturnSupervisionTimeBuckets to the list of
    SupervisionTimeBuckets.

    Args:
        - supervision_sentences: list of StateSupervisionSentences for a person
        - supervision_periods: list of StateSupervisionPeriods for a person
        - incarceration_periods: list of StateIncarcerationPeriods for a person
        - assessments: list of StateAssessments for a person
        - violations: list of StateSupervisionViolations for a person
        - violation_responses: list of StateSupervisionViolationResponses for
            a person
        - ssvr_agent_associations: dictionary associating
            StateSupervisionViolationResponse ids to information about the
            corresponding StateAgent on the response
        - supervision_period_to_agent_associations: dictionary associating
            StateSupervisionPeriod ids to information about the corresponding
            StateAgent
        - state_code: the state_code to limit the output to. If this is 'ALL' or
            is omitted, then all states will be included in the result.

    Returns:
        A list of SupervisionTimeBuckets for the person.
    """
    supervision_time_buckets: List[SupervisionTimeBucket] = []

    incarceration_periods = \
        prepare_incarceration_periods_for_calculations(incarceration_periods)

    incarceration_periods.sort(key=lambda b: b.admission_date)

    indexed_incarceration_periods = \
        index_incarceration_periods_by_admission_month(incarceration_periods)

    indexed_supervision_periods = \
        _index_supervision_periods_by_termination_month(supervision_periods)

    months_of_incarceration = _identify_months_of_incarceration(
        incarceration_periods)

    projected_supervision_completion_buckets = \
        classify_supervision_success(supervision_sentences,
                                     supervision_period_to_agent_associations)

    supervision_time_buckets.extend(projected_supervision_completion_buckets)

    for supervision_period in supervision_periods:
        # Don't process placeholder supervision periods
        if not is_placeholder(supervision_period):
            supervision_time_buckets = \
                supervision_time_buckets + \
                find_time_buckets_for_supervision_period(
                    supervision_period,
                    indexed_incarceration_periods,
                    months_of_incarceration,
                    assessments,
                    violation_responses,
                    ssvr_agent_associations,
                    supervision_period_to_agent_associations)

            supervision_termination_bucket = \
                find_supervision_termination_bucket(
                    supervision_period,
                    indexed_supervision_periods,
                    assessments,
                    supervision_period_to_agent_associations
                )

            if supervision_termination_bucket:
                supervision_time_buckets.append(supervision_termination_bucket)

    # TODO(2680): Update revocation logic to not rely on adding months for
    #  missing returns
    supervision_time_buckets = add_missing_revocation_returns(
        incarceration_periods, supervision_time_buckets, assessments, violation_responses, ssvr_agent_associations)

    if state_code != 'ALL':
        supervision_time_buckets = [
            bucket for bucket in supervision_time_buckets
            if bucket.state_code == state_code
        ]

    return supervision_time_buckets


def find_time_buckets_for_supervision_period(
        supervision_period: StateSupervisionPeriod,
        indexed_incarceration_periods:
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]],
        months_of_incarceration: Set[Tuple[int, int]],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations:
        Dict[int, Dict[Any, Any]]) -> \
        List[SupervisionTimeBucket]:
    """Finds time that this person was on supervision for the given
    StateSupervisionPeriod, classified as either an instance of
    revocation or not.

    Args:
        - supervision_period: The supervision period the person was on
        - indexed_incarceration_periods: A dictionary mapping years and months
            of admissions to prison to the StateIncarcerationPeriods that
            started in that month.
        - months_of_incarceration: A set of tuples in the format (year, month)
            for each month of which this person has been incarcerated for the
            full month.
        - ssvr_agent_associations: dictionary associating
            StateSupervisionViolationResponse ids to information about the
            corresponding StateAgent on the response

    Returns
        - A set of unique SupervisionTimeBuckets for the person for the given
            StateSupervisionPeriod.
    """
    supervision_month_buckets = \
        find_month_buckets_for_supervision_period(
            supervision_period,
            indexed_incarceration_periods,
            months_of_incarceration,
            assessments,
            violation_responses,
            ssvr_agent_associations,
            supervision_period_to_agent_associations)

    supervision_year_buckets = convert_month_buckets_to_year_buckets(
        supervision_period, supervision_month_buckets, assessments,
        supervision_period_to_agent_associations)

    return supervision_month_buckets + supervision_year_buckets


def find_month_buckets_for_supervision_period(
        supervision_period: StateSupervisionPeriod,
        indexed_incarceration_periods:
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]],
        months_of_incarceration: Set[Tuple[int, int]],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations:
        Dict[int, Dict[Any, Any]]) -> \
        List[SupervisionTimeBucket]:
    """Finds months that this person was on supervision for the given
    StateSupervisionPeriod, where the person was not incarcerated for the full
    month. Classifies each time bucket on supervision as either an instance of
    revocation or not.

    Args:
        - supervision_period: The supervision period the person was on
        - indexed_incarceration_periods: A dictionary mapping years and months
            of admissions to prison to the StateIncarcerationPeriods that
            started in that month.
        - months_of_incarceration: A set of tuples in the format (year, month)
            for each month of which this person has been incarcerated for the
            full month.
        - ssvr_agent_associations: dictionary associating
            StateSupervisionViolationResponse ids to information about the
            corresponding StateAgent on the response
    Returns
        - A set of unique SupervisionTimeBuckets for the person for the given
            StateSupervisionPeriod.
    """
    supervision_month_buckets: List[SupervisionTimeBucket] = []

    start_date = supervision_period.start_date
    termination_date = supervision_period.termination_date

    if termination_date is None:
        termination_date = date.today()

    if start_date is None:
        return supervision_month_buckets

    time_bucket = date(start_date.year, start_date.month, 1)

    while time_bucket <= termination_date:
        if time_bucket.year in indexed_incarceration_periods.keys() \
                and time_bucket.month in indexed_incarceration_periods[time_bucket.year].keys():
            # An admission to prison happened during this month

            time_bucket_tuple = (time_bucket.year, time_bucket.month)

            incarceration_periods = indexed_incarceration_periods[time_bucket.year][time_bucket.month]

            time_bucket_incremented = False

            for index, incarceration_period in enumerate(incarceration_periods):
                # Get the supervision time bucket for this month
                supervision_month_bucket = _get_supervision_time_bucket(
                    supervision_period,
                    incarceration_period,
                    assessments,
                    violation_responses,
                    time_bucket_tuple, months_of_incarceration,
                    ssvr_agent_associations,
                    supervision_period_to_agent_associations)

                if isinstance(supervision_month_bucket, RevocationReturnSupervisionTimeBucket) \
                        or index == len(incarceration_periods) - 1:

                    if supervision_month_bucket is not None:
                        supervision_month_buckets.append(supervision_month_bucket)

                    release_date = incarceration_period.release_date
                    if release_date is None or termination_date < release_date:
                        # Person is either still in custody or this
                        # supervision period ended before the incarceration
                        # period was over. Stop counting supervision months
                        # for this supervision period.
                        return supervision_month_buckets

                    if release_date.year > time_bucket.year or release_date.month > time_bucket.month:
                        # If they were released in a later month than the one we
                        # are currently looking at, start on the month of the
                        # release and continue to count supervision months.
                        time_bucket = date(release_date.year, release_date.month, 1)
                        time_bucket_incremented = True

                    break

            if time_bucket_incremented:
                # The time bucket has been incremented to the year of a release
                # from incarceration. Continue with this next time bucket year.
                continue

        if (time_bucket.year, time_bucket.month) not in months_of_incarceration:

            start_of_month = date(time_bucket.year, time_bucket.month, 1)
            end_of_month = last_day_of_month(start_of_month)

            assessment_score, assessment_level, assessment_type = \
                find_most_recent_assessment(end_of_month, assessments)

            supervising_officer_external_id, supervising_district_external_id = \
                _get_supervising_officer_and_district(
                    supervision_period.supervision_period_id, supervision_period_to_agent_associations)

            case_type = _identify_most_severe_case_type(supervision_period)

            supervision_month_buckets.append(
                NonRevocationReturnSupervisionTimeBucket.for_month(
                    state_code=supervision_period.state_code,
                    year=time_bucket.year,
                    month=time_bucket.month,
                    supervision_type=supervision_period.supervision_type,
                    case_type=case_type,
                    assessment_score=assessment_score,
                    assessment_level=assessment_level,
                    assessment_type=assessment_type,
                    supervising_officer_external_id=supervising_officer_external_id,
                    supervising_district_external_id=supervising_district_external_id))

        time_bucket = time_bucket + relativedelta(months=1)

    return supervision_month_buckets


def convert_month_buckets_to_year_buckets(
        supervision_period: StateSupervisionPeriod,
        supervision_month_buckets: List[SupervisionTimeBucket],
        assessments: List[StateAssessment],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> List[SupervisionTimeBucket]:
    """Converts a list of SupervisionTimeBuckets that contains months on
    supervision into a list of SupervisionTimeBuckets for years on supervision.

    If there are only NonRevocationReturnSupervisionTimeBuckets for a given
    year, adds a NonRevocationReturnSupervisionTimeBucket for that year.

    For every RevocationReturnSupervisionTimeBucket, adds a new
    RevocationReturnSupervisionTimeBucket for the given year. Only includes
    NonRevocationReturnSupervisionTimeBucket that follow revocations if there
    are no subsequent revocations during the given year.
    """
    years_on_supervision_buckets: Dict[int, List[SupervisionTimeBucket]] \
        = defaultdict()

    # Convert month buckets to year buckets
    for supervision_month_bucket in supervision_month_buckets:
        year = supervision_month_bucket.year

        end_of_year = date(year, 12, 31)

        assessment_score, assessment_level, assessment_type = \
            find_most_recent_assessment(end_of_year, assessments)

        if year not in years_on_supervision_buckets.keys():
            if isinstance(supervision_month_bucket,
                          RevocationReturnSupervisionTimeBucket):
                years_on_supervision_buckets[year] = [
                    RevocationReturnSupervisionTimeBucket.
                    for_year_from_month_assessment_override(
                        supervision_month_bucket,
                        assessment_score,
                        assessment_level,
                        assessment_type)]
            elif isinstance(supervision_month_bucket,
                            NonRevocationReturnSupervisionTimeBucket):
                # Record that they were on supervision for this year but don't
                # add any buckets yet.
                years_on_supervision_buckets[year] = []
        else:
            if isinstance(supervision_month_bucket,
                          RevocationReturnSupervisionTimeBucket):
                if years_on_supervision_buckets[year]:
                    last_bucket = years_on_supervision_buckets[year][-1]
                    if isinstance(last_bucket,
                                  NonRevocationReturnSupervisionTimeBucket):
                        # This person was revoked, released from incarceration,
                        # and then re-revoked. Remove this non-revocation
                        # bucket.
                        years_on_supervision_buckets[year].pop(-1)
                # Add this revocation bucket
                years_on_supervision_buckets[year].append(
                    RevocationReturnSupervisionTimeBucket.
                    for_year_from_month_assessment_override(
                        supervision_month_bucket,
                        assessment_score,
                        assessment_level,
                        assessment_type))
            elif isinstance(supervision_month_bucket,
                            NonRevocationReturnSupervisionTimeBucket):
                if years_on_supervision_buckets[year]:
                    last_bucket = years_on_supervision_buckets[year][-1]
                    if isinstance(last_bucket,
                                  RevocationReturnSupervisionTimeBucket):
                        # If there is a non-revocation following a revocation,
                        # that means this person was released back onto
                        # supervision after being revoked. Add this new
                        # non-revocation bucket.
                        years_on_supervision_buckets[year].append(
                            NonRevocationReturnSupervisionTimeBucket.
                            for_year_from_month_assessment_override(
                                supervision_month_bucket,
                                assessment_score,
                                assessment_level,
                                assessment_type))

    supervision_year_buckets: List[SupervisionTimeBucket] = []

    for year in years_on_supervision_buckets.keys():
        if not years_on_supervision_buckets[year]:
            # This person was on supervision for this year and there were no
            # revocations. Add a non-revocation bucket for this year.

            end_of_year = date(year, 12, 31)

            assessment_score, assessment_level, assessment_type = \
                find_most_recent_assessment(end_of_year, assessments)

            supervising_officer_external_id, \
                supervising_district_external_id = \
                _get_supervising_officer_and_district(
                    supervision_period.supervision_period_id,
                    supervision_period_to_agent_associations)

            case_type = _identify_most_severe_case_type(supervision_period)

            supervision_year_buckets.append(
                NonRevocationReturnSupervisionTimeBucket.for_year(
                    state_code=supervision_period.state_code,
                    year=year,
                    supervision_type=supervision_period.supervision_type,
                    case_type=case_type,
                    assessment_score=assessment_score,
                    assessment_level=assessment_level,
                    assessment_type=assessment_type,
                    supervising_officer_external_id=
                    supervising_officer_external_id,
                    supervising_district_external_id=
                    supervising_district_external_id
                )
            )
        else:
            # Add all buckets for this year
            for year_bucket in years_on_supervision_buckets[year]:
                supervision_year_buckets.append(year_bucket)

    return supervision_year_buckets


def find_supervision_termination_bucket(
        supervision_period: StateSupervisionPeriod,
        indexed_supervision_periods:
        Dict[int, Dict[int, List[StateSupervisionPeriod]]],
        assessments: List[StateAssessment],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> Optional[SupervisionTimeBucket]:
    """Identifies an instance of supervision termination. If the given
    supervision_period has a valid start_date and termination_date, then
    returns a SupervisionTerminationBucket with the details of the termination.

    Calculates the change in assessment score from the beginning of supervision
    to the termination of supervision. This is done by identifying the first
    reassessment (or, the second score) and the last score during a
    supervision period, and taking the difference between the last score and the
    first reassessment score.

    If a person has multiple supervision periods that end in a given month, the
    the earliest start date and the latest termination date of the periods is
    used to estimate the start and end dates of the supervision. These dates
    are then used to determine what the second and last assessment scores are.

    If this supervision does not have a termination_date, then None is returned.
    """
    if supervision_period.start_date is not None and \
            supervision_period.termination_date is not None:
        start_date = supervision_period.start_date
        termination_date = supervision_period.termination_date

        termination_year = termination_date.year
        termination_month = termination_date.month

        periods_terminated_in_year = \
            indexed_supervision_periods.get(termination_year)

        if periods_terminated_in_year:
            periods_terminated_in_month = \
                periods_terminated_in_year.get(termination_month)

            if periods_terminated_in_month:
                for period in periods_terminated_in_month:
                    if period.start_date and period.start_date < start_date:
                        start_date = period.start_date

                    if period.termination_date and \
                            period.termination_date > termination_date:
                        termination_date = period.termination_date

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            find_assessment_score_change(
                start_date, termination_date, assessments)

        supervising_officer_external_id, \
            supervising_district_external_id = \
            _get_supervising_officer_and_district(
                supervision_period.supervision_period_id,
                supervision_period_to_agent_associations)

        case_type = _identify_most_severe_case_type(supervision_period)

        return SupervisionTerminationBucket.for_month(
            state_code=supervision_period.state_code,
            year=termination_date.year,
            month=termination_date.month,
            supervision_type=supervision_period.supervision_type,
            case_type=case_type,
            assessment_score=end_assessment_score,
            assessment_level=end_assessment_level,
            assessment_type=end_assessment_type,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=assessment_score_change,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=
            supervising_district_external_id
        )

    return None


def _revocation_occurred(admission_reason: AdmissionReason, supervision_type: Optional[StateSupervisionType],
                         admission_date: date, supervision_start_date: date) -> bool:
    """If the supervision started before or on the admission date, and the
    admission reason represents a revocation admission, then returns True. Else,
    returns False."""
    # TODO(2788): Decide whether we should enforce that the supervision type of
    #  the period that is ending matches the admission reason
    if supervision_start_date > admission_date:
        return False

    if ((admission_reason == AdmissionReason.PROBATION_REVOCATION and supervision_type == StateSupervisionType.PAROLE)
            or (admission_reason == AdmissionReason.PAROLE_REVOCATION
                and supervision_type == StateSupervisionType.PROBATION)):
        logging.info("Revocation type %s does not match the supervision type %s", admission_reason, supervision_type)

    return admission_reason in (AdmissionReason.PAROLE_REVOCATION,
                                AdmissionReason.PROBATION_REVOCATION)


def _get_supervision_time_bucket(
        supervision_period: StateSupervisionPeriod,
        incarceration_period: StateIncarcerationPeriod,
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        time_bucket: Tuple[int, int],
        months_of_incarceration: Set[Tuple[int, int]],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations:
        Dict[int, Dict[Any, Any]]) \
        -> Optional[SupervisionTimeBucket]:
    """Returns a SupervisionTimeBucket if one should be recorded given the supervision period and the months of
    incarceration.

    If a revocation occurred during the time bucket, returns a RevocationSupervisionTimeBucket. If a revocation did not
    occur, and the person was not incarcerated for the whole time, then returns a NonRevocationSupervisionTimeBucket.
    If the person was incarcerated for the whole time, or the required fields are not set on the periods, then returns
    None.
    """
    bucket_year, bucket_month = time_bucket

    start_of_month = date(bucket_year, bucket_month, 1)
    end_of_month = last_day_of_month(start_of_month)

    admission_reason = incarceration_period.admission_reason
    admission_date = incarceration_period.admission_date

    supervision_start_date = supervision_period.start_date

    if not admission_date or not admission_reason or not supervision_start_date:
        return None

    assessment_score, assessment_level, assessment_type = find_most_recent_assessment(end_of_month, assessments)

    case_type = _identify_most_severe_case_type(supervision_period)

    if _revocation_occurred(
            admission_reason, supervision_period.supervision_type, admission_date, supervision_start_date):
        # A revocation occurred
        revocation_type, violation_type, supervising_officer_external_id, supervising_district_external_id = \
            _get_revocation_details(
                incarceration_period,
                ssvr_agent_associations,
                supervision_period,
                supervision_period_to_agent_associations)

        # Get details about the violation and response history leading up to the revocation
        violation_history = get_violation_and_response_history(admission_date, violation_responses)

        supervision_type = supervision_period.supervision_type

        if not supervision_type:
            # Infer the supervision type from the admission reason
            if incarceration_period.admission_reason == AdmissionReason.PROBATION_REVOCATION:
                supervision_type = StateSupervisionType.PROBATION
            elif incarceration_period.admission_reason == AdmissionReason.PAROLE_REVOCATION:
                supervision_type = StateSupervisionType.PAROLE

        return RevocationReturnSupervisionTimeBucket.for_month(
            state_code=supervision_period.state_code,
            year=bucket_year,
            month=bucket_month,
            supervision_type=supervision_type,
            case_type=case_type,
            assessment_score=assessment_score,
            assessment_level=assessment_level,
            assessment_type=assessment_type,
            revocation_type=revocation_type,
            source_violation_type=violation_type,
            most_severe_violation_type=violation_history.most_severe_violation_type,
            most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
            most_severe_response_decision=violation_history.most_severe_response_decision,
            response_count=violation_history.response_count,
            violation_history_description=violation_history.violation_history_description,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id)

    if (bucket_year, bucket_month) not in months_of_incarceration:
        # They weren't incarcerated for this month and there
        # was no revocation
        return NonRevocationReturnSupervisionTimeBucket.for_month(
            state_code=supervision_period.state_code,
            year=bucket_year,
            month=bucket_month,
            supervision_type=supervision_period.supervision_type,
            case_type=case_type,
            assessment_score=assessment_score,
            assessment_level=assessment_level,
            assessment_type=assessment_type)

    return None


def _get_revocation_details(
        incarceration_period: StateIncarcerationPeriod,
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period: Optional[StateSupervisionPeriod],
        supervision_period_to_agent_associations:
        Optional[Dict[int, Dict[Any, Any]]],
) -> Tuple[Optional[StateSupervisionViolationResponseRevocationType],
           Optional[StateSupervisionViolationType],
           Optional[str], Optional[str]]:
    """Identifies the attributes of the revocation return from the source_supervision_violation_response on the
    incarceration_period, if it exists, or information on the preceding supervision period, if it is present.
    """

    source_violation_response = incarceration_period.source_supervision_violation_response
    revocation_type = None
    violation_type = None
    supervising_officer_external_id = None
    supervising_district_external_id = None
    if source_violation_response:
        response_decisions = source_violation_response.supervision_violation_response_decisions
        if response_decisions:
            revocation_type = _identify_most_severe_revocation_type(response_decisions)

        # TODO(2840): Remove this once revocation type is being set on the proper fields on the response decisions
        if revocation_type is None:
            revocation_type = source_violation_response.revocation_type

            if revocation_type is None:
                # If the revocation type is not set, assume this is a reincarceration
                revocation_type = StateSupervisionViolationResponseRevocationType.REINCARCERATION

        source_violation = source_violation_response.supervision_violation
        if source_violation:
            violation_type, _ = identify_most_severe_violation_type_and_subtype([source_violation])

        supervision_violation_response_id = source_violation_response.supervision_violation_response_id

        if supervision_violation_response_id:
            agent_info = ssvr_agent_associations.get(supervision_violation_response_id)

            if agent_info is not None:
                supervising_officer_external_id = agent_info.get('agent_external_id')
                supervising_district_external_id = agent_info.get('district_external_id')
    elif supervision_period and supervision_period_to_agent_associations:
        # Get the supervising officer and district data from the preceding
        # supervision period
        if supervising_officer_external_id is None and \
                supervising_district_external_id is None and \
                supervision_period.supervision_period_id:

            supervising_officer_external_id, supervising_district_external_id = \
                _get_supervising_officer_and_district(
                    supervision_period.supervision_period_id, supervision_period_to_agent_associations)

    return revocation_type, violation_type, supervising_officer_external_id, supervising_district_external_id


ViolationHistory = NamedTuple('ViolationHistory', [
        ('most_severe_violation_type', Optional[StateSupervisionViolationType]),
        ('most_severe_violation_type_subtype', Optional[str]),
        ('most_severe_response_decision', Optional[StateSupervisionViolationResponseDecision]),
        ('response_count', Optional[int]),
        ('violation_history_description', Optional[str])])


def get_violation_and_response_history(
        revocation_date: date,
        violation_responses: List[StateSupervisionViolationResponse]
) -> ViolationHistory:
    """Looks at the series of violation responses that preceded a revocation. Selects the violation responses that
    occurred within VIOLATION_HISTORY_WINDOW_MONTHS months of the revocation return date, and looks at those responses
    and the corresponding violations. Identifies and returns the most severe violation type that was recorded during
    the period, the most severe decision on the responses, and the total number of responses during the period.
    """

    history_cutoff_date = revocation_date - relativedelta(months=VIOLATION_HISTORY_WINDOW_MONTHS)

    responses_in_window = [
        response for response in violation_responses
        if response.response_date is not None
        and not response.is_draft
        and history_cutoff_date <= response.response_date <= revocation_date
    ]

    violations_in_window: List[StateSupervisionViolation] = []

    response_decisions: List[StateSupervisionViolationResponseDecision] = []
    for response in responses_in_window:
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
    most_severe_decision = identify_most_severe_response_decision(response_decisions)

    violation_type_entries = []
    for violation in violations_in_window:
        violation_type_entries.extend(violation.supervision_violation_types)

    violation_history_description = _get_violation_history_description(violations_in_window)

    # Count the number of responses in the window
    response_count = len(responses_in_window)
    violation_history_result = ViolationHistory(
        most_severe_violation_type,
        most_severe_violation_type_subtype,
        most_severe_decision,
        response_count,
        violation_history_description)

    return violation_history_result


def _get_violation_history_description(violations: List[StateSupervisionViolation]) -> Optional[str]:
    """Returns a string description of the violation history given the violation
    type entries. Tallies the number of each violation type, and then builds a
    string that lists the number of each of the represented types in the order
    listed in the violation_type_shorthand dictionary and separated by a
    semicolon.

    For example, if someone has 3 felonies and 2 technicals, this will return
    '3fel;2tech'.
    """
    if not violations:
        return None

    state_code = violations[0].state_code
    ranked_violation_type_and_subtype_counts: Dict[str, int] = {}

    if state_code.upper() == 'US_MO':
        ranked_violation_type_and_subtype_counts = \
            us_mo_utils.get_ranked_violation_type_and_subtype_counts(violations)

    descriptions = [f"{count}{label}" for label, count in
                    ranked_violation_type_and_subtype_counts.items() if count > 0]

    if descriptions:
        return ';'.join(descriptions)

    return None


def index_incarceration_periods_by_admission_month(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]]:
    """Organizes the list of StateIncarcerationPeriods by the year and month
     of the admission_date on the period."""

    indexed_incarceration_periods: \
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]] = defaultdict()

    for incarceration_period in incarceration_periods:
        if incarceration_period.admission_date:
            year = incarceration_period.admission_date.year
            month = incarceration_period.admission_date.month

            if year not in indexed_incarceration_periods.keys():
                indexed_incarceration_periods[year] = {
                    month: [incarceration_period]
                }
            elif month not in indexed_incarceration_periods[year].keys():
                indexed_incarceration_periods[year][month] = \
                    [incarceration_period]
            else:
                indexed_incarceration_periods[year][month].append(
                    incarceration_period)

    return indexed_incarceration_periods


def add_missing_revocation_returns(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_time_buckets: List[SupervisionTimeBucket],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]]) -> \
        List[SupervisionTimeBucket]:
    """Looks at all incarceration periods to see if they were revocation returns. If the list of supervision time
    buckets does not have a recorded revocation return corresponding to the incarceration admission, then
    RevocationReturnSupervisionTimeBuckets are added to the supervision_time_buckets.
    """
    for incarceration_period in incarceration_periods:
        # This check is here to silence mypy warnings. We have already validated that this incarceration period has an
        # admission date.
        if incarceration_period.admission_date:
            (revocation_type, violation_type, supervising_officer_external_id, supervising_district_external_id) = \
                _get_revocation_details(incarceration_period, ssvr_agent_associations, None, None)

            if revocation_type is None:
                # If the revocation type is not set, assume this is a reincarceration
                revocation_type = StateSupervisionViolationResponseRevocationType.REINCARCERATION

            year = incarceration_period.admission_date.year
            month = incarceration_period.admission_date.month

            supervision_type = None

            if incarceration_period.admission_reason == AdmissionReason.PROBATION_REVOCATION:
                supervision_type = StateSupervisionType.PROBATION
            elif incarceration_period.admission_reason == AdmissionReason.PAROLE_REVOCATION:
                supervision_type = StateSupervisionType.PAROLE

            start_of_month = date(year, month, 1)
            end_of_month = last_day_of_month(start_of_month)

            assessment_score, assessment_level, assessment_type = find_most_recent_assessment(end_of_month, assessments)

            admission_date = incarceration_period.admission_date

            # Get details about the violation and response history leading up to
            # the revocation
            violation_history = get_violation_and_response_history(admission_date, violation_responses)

            case_type = StateSupervisionCaseType.GENERAL

            if supervision_type is not None:
                supervision_month_bucket = RevocationReturnSupervisionTimeBucket.for_month(
                    state_code=incarceration_period.state_code,
                    year=year,
                    month=month,
                    supervision_type=supervision_type,
                    case_type=case_type,
                    assessment_score=assessment_score,
                    assessment_level=assessment_level,
                    assessment_type=assessment_type,
                    revocation_type=revocation_type,
                    source_violation_type=violation_type,
                    most_severe_violation_type=violation_history.most_severe_violation_type,
                    most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                    most_severe_response_decision=violation_history.most_severe_response_decision,
                    response_count=violation_history.response_count,
                    violation_history_description=violation_history.violation_history_description,
                    supervising_officer_external_id=supervising_officer_external_id,
                    supervising_district_external_id=supervising_district_external_id)

                revocation_year_accounted_for = False
                for existing_supervision_bucket in supervision_time_buckets:
                    if isinstance(existing_supervision_bucket, RevocationReturnSupervisionTimeBucket) \
                            and existing_supervision_bucket.year == supervision_month_bucket.year \
                            and existing_supervision_bucket.month == supervision_month_bucket.month:
                        revocation_year_accounted_for = True
                        continue

                if not revocation_year_accounted_for:
                    supervision_time_buckets.append(supervision_month_bucket)

                supervision_year_bucket = \
                    RevocationReturnSupervisionTimeBucket.for_year(
                        state_code=incarceration_period.state_code,
                        year=year,
                        supervision_type=supervision_type,
                        case_type=case_type,
                        assessment_score=assessment_score,
                        assessment_level=assessment_level,
                        assessment_type=assessment_type,
                        revocation_type=revocation_type,
                        source_violation_type=violation_type,
                        most_severe_violation_type=violation_history.most_severe_violation_type,
                        most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                        most_severe_response_decision=violation_history.most_severe_response_decision,
                        response_count=violation_history.response_count,
                        supervising_officer_external_id=
                        supervising_officer_external_id,
                        supervising_district_external_id=
                        supervising_district_external_id)

                revocation_year_accounted_for = False
                for existing_supervision_bucket in supervision_time_buckets:
                    if isinstance(existing_supervision_bucket, RevocationReturnSupervisionTimeBucket) \
                            and existing_supervision_bucket.year == supervision_year_bucket.year \
                            and existing_supervision_bucket.month == supervision_year_bucket.month:
                        revocation_year_accounted_for = True
                        continue

                if not revocation_year_accounted_for:
                    supervision_time_buckets.append(supervision_year_bucket)

                    end_of_year = date(year, 12, 31)

                    assessment_score, assessment_level, assessment_type = \
                        find_most_recent_assessment(end_of_year, assessments)

                    non_revocation_supervision_year_bucket = NonRevocationReturnSupervisionTimeBucket.for_year(
                        state_code=incarceration_period.state_code,
                        year=year,
                        supervision_type=supervision_type,
                        case_type=case_type,
                        assessment_score=assessment_score,
                        assessment_level=assessment_level,
                        assessment_type=assessment_type)

                    #  If we had previously classified this year as a non-revocation year, remove that non-revocation
                    #  time bucket
                    if non_revocation_supervision_year_bucket in supervision_time_buckets:
                        supervision_time_buckets.remove(non_revocation_supervision_year_bucket)

    return supervision_time_buckets


def classify_supervision_success(
        supervision_sentences: List[StateSupervisionSentence],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]) \
        -> List[ProjectedSupervisionCompletionBucket]:
    """
    This classifies whether supervision projected to end in a given month was
    completed successfully.

    For supervision sentences with a projected_completion_date, where that date
    is before or on today's date, looks at all supervision periods that have
    terminated and finds the one with the latest termination date. From that
    supervision period, classifies the termination as either successful or not
    successful.
    """
    projected_completion_buckets: \
        List[ProjectedSupervisionCompletionBucket] = []

    for supervision_sentence in supervision_sentences:
        projected_completion_date = \
            supervision_sentence.projected_completion_date

        if projected_completion_date and \
                projected_completion_date <= date.today():
            year = projected_completion_date.year
            month = projected_completion_date.month
            latest_termination_date = None
            latest_supervision_period = None

            for supervision_period in supervision_sentence.supervision_periods:
                termination_date = supervision_period.termination_date
                if termination_date:
                    if latest_termination_date is None or \
                            latest_termination_date < termination_date:
                        latest_termination_date = termination_date
                        latest_supervision_period = \
                            supervision_period

            if latest_supervision_period:
                completion_bucket = \
                    _get_projected_completion_bucket_from_supervision_period(
                        year, month,
                        latest_supervision_period,
                        supervision_period_to_agent_associations
                    )
                if completion_bucket.supervision_type is not None:
                    projected_completion_buckets.append(completion_bucket)

    return projected_completion_buckets


def _get_projected_completion_bucket_from_supervision_period(
        year: int, month: int,
        supervision_period: StateSupervisionPeriod,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> ProjectedSupervisionCompletionBucket:

    supervision_success = supervision_period.termination_reason in \
                          [StateSupervisionPeriodTerminationReason.DISCHARGE,
                           StateSupervisionPeriodTerminationReason.EXPIRATION]

    supervising_officer_external_id, supervising_district_external_id = \
        _get_supervising_officer_and_district(
            supervision_period.supervision_period_id,
            supervision_period_to_agent_associations)

    case_type = _identify_most_severe_case_type(supervision_period)

    return ProjectedSupervisionCompletionBucket.for_month(
        state_code=supervision_period.state_code,
        year=year,
        month=month,
        supervision_type=supervision_period.supervision_type,
        case_type=case_type,
        successful_completion=supervision_success,
        supervising_officer_external_id=supervising_officer_external_id,
        supervising_district_external_id=supervising_district_external_id
    )


def _get_supervising_officer_and_district(
        supervision_period_id: Optional[int],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]) \
        -> Tuple[Optional[str], Optional[str]]:

    supervising_officer_external_id = None
    supervising_district_external_id = None

    if supervision_period_id:
        agent_info = \
            supervision_period_to_agent_associations.get(supervision_period_id)

        if agent_info is not None:
            supervising_officer_external_id = agent_info.get(
                'agent_external_id')
            supervising_district_external_id = agent_info.get(
                'district_external_id')

    return supervising_officer_external_id, supervising_district_external_id


def _index_supervision_periods_by_termination_month(
        supervision_periods: List[StateSupervisionPeriod]) -> \
        Dict[int, Dict[int, List[StateSupervisionPeriod]]]:
    """Organizes the list of StateSupervisionPeriods by the year and month
     of the termination_date on the period."""

    indexed_supervision_periods: \
        Dict[int, Dict[int, List[StateSupervisionPeriod]]] = defaultdict()

    for supervision_period in supervision_periods:
        if supervision_period.termination_date:
            year = supervision_period.termination_date.year
            month = supervision_period.termination_date.month

            if year not in indexed_supervision_periods.keys():
                indexed_supervision_periods[year] = {
                    month: [supervision_period]
                }
            elif month not in indexed_supervision_periods[year].keys():
                indexed_supervision_periods[year][month] = \
                    [supervision_period]
            else:
                indexed_supervision_periods[year][month].append(
                    supervision_period)

    return indexed_supervision_periods


def _identify_months_of_incarceration(
        incarceration_periods: List[StateIncarcerationPeriod]) -> \
        Set[Tuple[int, int]]:
    """For each StateIncarcerationPeriod, identifies months where the
    person was incarcerated for every day during that month. Returns a set of
    months in the format (year, month) for which the person spent the entire
    month in a prison."""

    months_incarcerated: Set[Tuple[int, int]] = set()

    for incarceration_period in incarceration_periods:
        admission_date = incarceration_period.admission_date
        release_date = incarceration_period.release_date

        if admission_date is None:
            return months_incarcerated

        if release_date is None:
            release_date = date.today()

        if admission_date.day == 1:
            month_date = admission_date
        else:
            month_date = date(admission_date.year, admission_date.month, 1) + \
                relativedelta(months=1)

        while month_date + relativedelta(months=1) <= \
                release_date + relativedelta(days=1):
            months_incarcerated.add((month_date.year, month_date.month))
            month_date = month_date + relativedelta(months=1)

    return months_incarcerated


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
