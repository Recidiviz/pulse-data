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
from typing import List, Dict, Set, Tuple, Optional, Any

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    SupervisionTimeBucket, RevocationReturnSupervisionTimeBucket, \
    NonRevocationReturnSupervisionTimeBucket, \
    ProjectedSupervisionCompletionBucket
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    last_day_of_month, find_most_recent_assessment
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
    import StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionViolationTypeEntry, \
    StateSupervisionViolationResponseDecisionEntry, StateSupervisionSentence, \
    StateAssessment


def find_supervision_time_buckets(
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_periods: List[StateIncarcerationPeriod],
        assessments: List[StateAssessment],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]) \
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
        - incarceration_periods: list of StateIncarcerationPeriods for a person
        - ssvr_agent_associations: dictionary associating
            StateSupervisionViolationResponse ids to information about the
            corresponding StateAgent on the response
        - supervision_period_to_agent_associations: dictionary associating
            StateSupervisionPeriod ids to information about the corresponding
            StateAgent

    Returns:
        A list of SupervisionTimeBuckets for the person.
    """
    supervision_time_buckets: List[SupervisionTimeBucket] = []

    incarceration_periods = \
        prepare_incarceration_periods_for_calculations(incarceration_periods)

    incarceration_periods.sort(key=lambda b: b.admission_date)

    indexed_incarceration_periods = \
        index_incarceration_periods_by_admission_month(incarceration_periods)

    months_of_incarceration = identify_months_of_incarceration(
        incarceration_periods)

    supervision_period_ids: Set[int] = set()
    supervision_periods: List[StateSupervisionPeriod] = []

    projected_supervision_completion_buckets = \
        classify_supervision_success(supervision_sentences,
                                     supervision_period_to_agent_associations)

    supervision_time_buckets.extend(projected_supervision_completion_buckets)

    for supervision_sentence in supervision_sentences:
        for supervision_period in supervision_sentence.supervision_periods:
            if supervision_period.supervision_period_id not in \
                    supervision_period_ids:
                # Do not add duplicate supervision periods that are attached
                # to multiple sentences
                supervision_periods.append(supervision_period)
            if supervision_period.supervision_period_id:
                supervision_period_ids.add(
                    supervision_period.supervision_period_id)

    for supervision_period in supervision_periods:
        # Don't process placeholder supervision periods
        if not is_placeholder(supervision_period):
            supervision_time_buckets = supervision_time_buckets + \
                                       find_time_buckets_for_supervision_period(
                                           supervision_period,
                                           indexed_incarceration_periods,
                                           months_of_incarceration,
                                           assessments,
                                           ssvr_agent_associations)

    # TODO(2680): Update revocation logic to not rely on adding months for
    #  missing returns
    supervision_time_buckets = add_missing_revocation_returns(
        incarceration_periods, supervision_time_buckets, assessments,
        ssvr_agent_associations)

    return supervision_time_buckets


def find_time_buckets_for_supervision_period(
        supervision_period: StateSupervisionPeriod,
        indexed_incarceration_periods:
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]],
        months_of_incarceration: Set[Tuple[int, int]],
        assessments: List[StateAssessment],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]]) -> \
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
            ssvr_agent_associations)

    supervision_year_buckets = convert_month_buckets_to_year_buckets(
        supervision_period, supervision_month_buckets, assessments)

    return supervision_month_buckets + supervision_year_buckets


def find_month_buckets_for_supervision_period(
        supervision_period: StateSupervisionPeriod,
        indexed_incarceration_periods:
        Dict[int, Dict[int, List[StateIncarcerationPeriod]]],
        months_of_incarceration: Set[Tuple[int, int]],
        assessments: List[StateAssessment],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]]) -> \
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
        if time_bucket.year in indexed_incarceration_periods.keys() and \
                time_bucket.month in \
                indexed_incarceration_periods[time_bucket.year].keys():
            # An admission to prison happened during this month

            time_bucket_tuple = (time_bucket.year, time_bucket.month)

            incarceration_periods = \
                indexed_incarceration_periods[time_bucket.year][
                    time_bucket.month]

            time_bucket_incremented = False

            for index, incarceration_period in enumerate(incarceration_periods):
                # Get the supervision time bucket for this month
                supervision_month_bucket = _get_supervision_time_bucket(
                    supervision_period, incarceration_period,
                    assessments,
                    time_bucket_tuple, months_of_incarceration,
                    ssvr_agent_associations)

                if isinstance(supervision_month_bucket,
                              RevocationReturnSupervisionTimeBucket) or \
                        index == len(incarceration_periods) - 1:

                    if supervision_month_bucket is not None:
                        supervision_month_buckets.append(
                            supervision_month_bucket)

                    release_date = incarceration_period.release_date
                    if release_date is None or \
                            termination_date < release_date:
                        # Person is either still in custody or this
                        # supervision period ended before the incarceration
                        # period was over. Stop counting supervision months
                        # for this supervision period.
                        return supervision_month_buckets

                    if release_date.year > time_bucket.year or \
                            release_date.month > time_bucket.month:
                        # If they were released in a later month than the one we
                        # are currently looking at, start on the month of the
                        # release and continue to count supervision months.
                        time_bucket = date(
                            release_date.year, release_date.month, 1)
                        time_bucket_incremented = True

                    break

            if time_bucket_incremented:
                # The time bucket has been incremented to the year of a release
                # from incarceration. Continue with this next time bucket year.
                continue

        if (time_bucket.year, time_bucket.month) not in \
                months_of_incarceration:

            start_of_month = date(time_bucket.year, time_bucket.month, 1)
            end_of_month = last_day_of_month(start_of_month)

            assessment_score, assessment_type = \
                find_most_recent_assessment(end_of_month, assessments)

            supervision_month_buckets.append(
                NonRevocationReturnSupervisionTimeBucket.for_month(
                    state_code=supervision_period.state_code,
                    year=time_bucket.year,
                    month=time_bucket.month,
                    supervision_type=supervision_period.supervision_type,
                    assessment_score=assessment_score,
                    assessment_type=assessment_type))

        time_bucket = time_bucket + relativedelta(months=1)

    return supervision_month_buckets


def convert_month_buckets_to_year_buckets(
        supervision_period: StateSupervisionPeriod,
        supervision_month_buckets: List[SupervisionTimeBucket],
        assessments: List[StateAssessment]
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

        assessment_score, assessment_type = \
            find_most_recent_assessment(end_of_year, assessments)

        if year not in years_on_supervision_buckets.keys():
            if isinstance(supervision_month_bucket,
                          RevocationReturnSupervisionTimeBucket):
                years_on_supervision_buckets[year] = [
                    RevocationReturnSupervisionTimeBucket.
                    for_year_from_month_assessment_override(
                        supervision_month_bucket,
                        assessment_score,
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
                                assessment_type))

    supervision_year_buckets: List[SupervisionTimeBucket] = []

    for year in years_on_supervision_buckets.keys():
        if not years_on_supervision_buckets[year]:
            # This person was on supervision for this year and there were no
            # revocations. Add a non-revocation bucket for this year.

            end_of_year = date(year, 12, 31)

            assessment_score, assessment_type = \
                find_most_recent_assessment(end_of_year, assessments)

            supervision_year_buckets.append(
                NonRevocationReturnSupervisionTimeBucket.for_year(
                    state_code=supervision_period.state_code,
                    year=year,
                    supervision_type=supervision_period.supervision_type,
                    assessment_score=assessment_score,
                    assessment_type=assessment_type
                )
            )
        else:
            # Add all buckets for this year
            for year_bucket in years_on_supervision_buckets[year]:
                supervision_year_buckets.append(year_bucket)

    return supervision_year_buckets


def _revocation_occurred(admission_reason: AdmissionReason,
                         admission_date: date,
                         supervision_type: StateSupervisionType,
                         supervision_start_date: date) -> \
        bool:
    """For a given pair of a StateSupervisionPeriod and a
    StateIncarcerationPeriod, returns true if the supervision type matches the
    type of supervision revocation in the admission reason."""
    if (admission_reason == AdmissionReason.PAROLE_REVOCATION and
            supervision_type == StateSupervisionType.PROBATION) or \
            (admission_reason == AdmissionReason.PROBATION_REVOCATION and
             supervision_type == StateSupervisionType.PAROLE):
        logging.info("Revocation type %s does not match the supervision type"
                     "%s.", admission_reason,
                     supervision_type)

    if supervision_start_date > admission_date:
        return False

    return admission_reason == AdmissionReason.PAROLE_REVOCATION and \
        supervision_type == StateSupervisionType.PAROLE or \
        admission_reason == AdmissionReason.PROBATION_REVOCATION and \
        supervision_type == StateSupervisionType.PROBATION


def _get_supervision_time_bucket(supervision_period: StateSupervisionPeriod,
                                 incarceration_period: StateIncarcerationPeriod,
                                 assessments: List[StateAssessment],
                                 time_bucket: Tuple[int, int],
                                 months_of_incarceration:
                                 Set[Tuple[int, int]],
                                 ssvr_agent_associations:
                                 Dict[int, Dict[Any, Any]]) \
        -> Optional[SupervisionTimeBucket]:
    """Returns a SupervisionTimeBucket if one should be recorded given the
    supervision period and the months of incarceration.

    If a revocation occurred during the time bucket, returns a
    RevocationSupervisionTimeBucket. If a revocation did not occur, and the
    person was not incarcerated for the whole time, then returns a
    NonRevocationSupervisionTimeBucket. If the person was incarcerated for the
    whole time, or the required fields are not set on the periods,
    then returns None.
    """
    bucket_year, bucket_month = time_bucket

    start_of_month = date(bucket_year, bucket_month, 1)
    end_of_month = last_day_of_month(start_of_month)

    admission_reason = incarceration_period.admission_reason
    admission_date = incarceration_period.admission_date

    supervision_type = supervision_period.supervision_type
    supervision_start_date = supervision_period.start_date

    if not admission_date or not admission_reason or not supervision_type or \
            not supervision_start_date:
        return None

    assessment_score, assessment_type = \
        find_most_recent_assessment(end_of_month, assessments)

    if _revocation_occurred(admission_reason, admission_date, supervision_type,
                            supervision_start_date):
        # A revocation occurred
        (revocation_type, violation_type, supervising_officer_external_id,
         supervising_district_external_id) = \
            _get_revocation_details_from_incarceration_period(
                incarceration_period, ssvr_agent_associations
            )

        return RevocationReturnSupervisionTimeBucket.for_month(
            state_code=supervision_period.state_code,
            year=bucket_year,
            month=bucket_month,
            supervision_type=supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type,
            revocation_type=revocation_type,
            source_violation_type=violation_type,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=supervising_district_external_id)

    if (bucket_year, bucket_month) not in \
            months_of_incarceration:
        # They weren't incarcerated for this month and there
        # was no revocation
        return NonRevocationReturnSupervisionTimeBucket.for_month(
            state_code=supervision_period.state_code,
            year=bucket_year,
            month=bucket_month,
            supervision_type=supervision_type,
            assessment_score=assessment_score,
            assessment_type=assessment_type)

    return None


def _get_revocation_details_from_incarceration_period(
        incarceration_period: StateIncarcerationPeriod,
        ssvr_agent_associations: Dict[int, Dict[Any, Any]]
) -> Tuple[Optional[StateSupervisionViolationResponseRevocationType],
           Optional[StateSupervisionViolationType],
           Optional[str], Optional[str]]:
    """Identifies the attributes of the revocation return from the
    source_supervision_violation_response on the incarceration_period."""

    source_violation_response = \
        incarceration_period.source_supervision_violation_response
    revocation_type = None
    violation_type = None
    supervising_officer_external_id = None
    supervising_district_external_id = None
    if source_violation_response:
        response_decisions = \
            source_violation_response.supervision_violation_response_decisions
        if response_decisions:
            revocation_type = _identify_most_serious_revocation_type(
                response_decisions
            )

        source_violation = \
            source_violation_response.supervision_violation
        if source_violation:
            violation_type = _identify_most_serious_violation_type(
                source_violation.supervision_violation_types)

        supervision_violation_response_id = \
            source_violation_response.supervision_violation_response_id

        if supervision_violation_response_id:
            agent_info = \
                ssvr_agent_associations.get(
                    supervision_violation_response_id)

            if agent_info is not None:
                supervising_officer_external_id = agent_info.get(
                    'agent_external_id')
                supervising_district_external_id = agent_info.get(
                    'district_external_id')

    return (revocation_type, violation_type, supervising_officer_external_id,
            supervising_district_external_id)


def _identify_most_serious_violation_type(
        violation_type_entries:
        List[StateSupervisionViolationTypeEntry]) -> \
        Optional[StateSupervisionViolationType]:
    """Identifies the most serious violation type on the violation according
    to the static violation type ranking."""
    violation_type_severity_order = [
        StateSupervisionViolationType.FELONY,
        StateSupervisionViolationType.MISDEMEANOR,
        StateSupervisionViolationType.ABSCONDED,
        StateSupervisionViolationType.MUNICIPAL,
        StateSupervisionViolationType.ESCAPED,
        StateSupervisionViolationType.TECHNICAL
    ]

    violation_types = [vte.violation_type for vte in violation_type_entries]
    for violation_type in violation_type_severity_order:
        if violation_type in violation_types:
            return violation_type
    return None


def _identify_most_serious_revocation_type(
        response_decisions:
        List[StateSupervisionViolationResponseDecisionEntry]) -> \
        Optional[StateSupervisionViolationResponseRevocationType]:
    """Identifies the most serious revocation type on the violation response
    according to the static revocation type ranking."""
    # Note: RETURN_TO_SUPERVISION is not included as a revocation type for a
    # revocation return to prison
    revocation_type_severity_order = [
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
        StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,
        StateSupervisionViolationResponseRevocationType.REINCARCERATION
    ]

    revocation_types = [resp.revocation_type for resp in response_decisions]
    for revocation_type in revocation_type_severity_order:
        if revocation_type in revocation_types:
            return revocation_type
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


def identify_months_of_incarceration(
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


def add_missing_revocation_returns(
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_time_buckets: List[SupervisionTimeBucket],
        assessments: List[StateAssessment],
        ssvr_agent_associations: Dict[int, Dict[Any, Any]]) -> \
        List[SupervisionTimeBucket]:
    """Looks at all incarceration periods to see if they were revocation
    returns. If the list of supervision time buckets does not have a recorded
    revocation return corresponding to the incarceration admission, then
    RevocationReturnSupervisionTimeBuckets are added to the
    supervision_time_buckets.
    """

    for incarceration_period in incarceration_periods:
        # This check is here to silence mypy warnings. We have already validated
        # that this incarceration period has an admission date.
        if incarceration_period.admission_date:
            (revocation_type, violation_type, supervising_officer_external_id,
             supervising_district_external_id) = \
                _get_revocation_details_from_incarceration_period(
                    incarceration_period, ssvr_agent_associations
                )

            year = incarceration_period.admission_date.year
            month = incarceration_period.admission_date.month

            supervision_type = None

            if incarceration_period.admission_reason == \
                    AdmissionReason.PROBATION_REVOCATION:
                supervision_type = StateSupervisionType.PROBATION
            elif incarceration_period.admission_reason == \
                    AdmissionReason.PAROLE_REVOCATION:
                supervision_type = StateSupervisionType.PAROLE

            start_of_month = date(year, month, 1)
            end_of_month = last_day_of_month(start_of_month)

            assessment_score, assessment_type = \
                find_most_recent_assessment(end_of_month, assessments)

            if supervision_type is not None:
                supervision_month_bucket = \
                    RevocationReturnSupervisionTimeBucket.for_month(
                        state_code=incarceration_period.state_code,
                        year=year,
                        month=month,
                        supervision_type=supervision_type,
                        assessment_score=assessment_score,
                        assessment_type=assessment_type,
                        revocation_type=revocation_type,
                        source_violation_type=violation_type,
                        supervising_officer_external_id=
                        supervising_officer_external_id,
                        supervising_district_external_id=
                        supervising_district_external_id)

                if supervision_month_bucket not in supervision_time_buckets:
                    supervision_time_buckets.append(supervision_month_bucket)

                supervision_year_bucket = \
                    RevocationReturnSupervisionTimeBucket.for_year(
                        state_code=incarceration_period.state_code,
                        year=year,
                        supervision_type=supervision_type,
                        assessment_score=assessment_score,
                        assessment_type=assessment_type,
                        revocation_type=revocation_type,
                        source_violation_type=violation_type,
                        supervising_officer_external_id=
                        supervising_officer_external_id,
                        supervising_district_external_id=
                        supervising_district_external_id)

                if supervision_year_bucket not in supervision_time_buckets:
                    supervision_time_buckets.append(supervision_year_bucket)

                    end_of_year = date(year, 12, 31)

                    assessment_score, assessment_type = \
                        find_most_recent_assessment(end_of_year, assessments)

                    non_revocation_supervision_year_bucket = \
                        NonRevocationReturnSupervisionTimeBucket.for_year(
                            state_code=incarceration_period.state_code,
                            year=year,
                            supervision_type=supervision_type,
                            assessment_score=assessment_score,
                            assessment_type=assessment_type,
                        )

                    #  If we had previously classified this year as a
                    #  non-revocation year, remove that non-revocation time
                    #  bucket
                    if non_revocation_supervision_year_bucket in \
                            supervision_time_buckets:
                        supervision_time_buckets.remove(
                            non_revocation_supervision_year_bucket)

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

    If there are multiple supervision sentences with projected completions
    in the same month, but of varying supervision types, then one completion
    bucket is recorded per supervision type per month.  If there are multiple
    supervision sentences of the same supervision type with projected
    completions in the same month, this only classifies that month as a
    successful completion for that supervision type if all of the terminations
    of that supervision type for that month were successful.
    """

    supervision_success_by_month: \
        Dict[Tuple[int, int], Dict[StateSupervisionType,
                                   ProjectedSupervisionCompletionBucket]] = {}

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
                    if (year, month) not in \
                            supervision_success_by_month.keys():
                        supervision_success_by_month[(year, month)] = {
                            completion_bucket.supervision_type:
                                completion_bucket
                        }
                    elif completion_bucket.supervision_type not in \
                            supervision_success_by_month[
                                    (year, month)].keys() \
                            or not completion_bucket.successful_completion:
                        # If this supervision type is already represented
                        # for this month, only replace the current value if
                        # this supervision completion bucket represents an
                        # unsuccessful completion.
                        supervision_success_by_month[(year, month)][
                            completion_bucket.supervision_type] = \
                            completion_bucket

    projected_completion_buckets = [
        bucket
        for month_groups in supervision_success_by_month.values()
        for bucket in month_groups.values()
    ]

    return projected_completion_buckets


def _get_projected_completion_bucket_from_supervision_period(
        year: int, month: int,
        supervision_period: StateSupervisionPeriod,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> ProjectedSupervisionCompletionBucket:

    supervision_success = supervision_period.termination_reason in \
                          [StateSupervisionPeriodTerminationReason.DISCHARGE,
                           StateSupervisionPeriodTerminationReason.EXPIRATION]

    supervising_officer_external_id = None
    supervising_district_external_id = None

    if supervision_period.supervision_period_id:
        agent_info = \
            supervision_period_to_agent_associations.get(
                supervision_period.supervision_period_id)

        if agent_info is not None:
            supervising_officer_external_id = agent_info.get(
                'agent_external_id')
            supervising_district_external_id = agent_info.get(
                'district_external_id')

    return ProjectedSupervisionCompletionBucket.for_month(
        state_code=supervision_period.state_code,
        year=year,
        month=month,
        supervision_type=supervision_period.supervision_type,
        successful_completion=supervision_success,
        supervising_officer_external_id=supervising_officer_external_id,
        supervising_district_external_id=supervising_district_external_id
    )
