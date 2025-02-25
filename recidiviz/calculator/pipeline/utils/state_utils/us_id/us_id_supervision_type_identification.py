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
"""US_ID-specific implementations of functions related to supervision type identification."""
import itertools
from collections import defaultdict
from datetime import date
from typing import Optional, List, Dict, Set

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.supervision_type_identification import _get_most_relevant_supervision_type
from recidiviz.common.common_utils import date_spans_overlap_exclusive
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence, StateSupervisionSentence, \
    StateIncarcerationPeriod, StateSupervisionPeriod

# We expect transition dates from supervision to incarceration to be fairly close together for the data in US_ID. We
# limit the search for a pre or post-incarceration supervision type to 30 days prior to admission or 30 days following
# release.
INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT = 30


def us_id_get_pre_incarceration_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the pre-incarceration supervision type for US_ID people by calculating the most recent type of
    supervision a given person was on within INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT days of the
    incarceration admission. If no supervision type is found, returns None.
    """
    admission_date = incarceration_period.admission_date

    if not admission_date:
        raise ValueError(
            f'No admission date for incarceration period {incarceration_period.incarceration_period_id}')

    supervision_periods = _get_supervision_periods_from_sentences(incarceration_sentences, supervision_sentences)

    return us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date=admission_date,
        lower_bound_inclusive_date=
        admission_date - relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT),
        supervision_periods=supervision_periods
    )


def us_id_get_post_incarceration_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the post-incarceration supervision type for US_ID people by calculating the type of supervision the
    person was on directly after their release from incarceration.
    """
    if not incarceration_period.release_date:
        raise ValueError(
            f'No release date for incarceration period {incarceration_period.incarceration_period_id}')

    if incarceration_period.release_reason != StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE:
        return None

    supervision_periods = _get_supervision_periods_from_sentences(incarceration_sentences, supervision_sentences)

    return us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date=
        incarceration_period.release_date + relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT),
        lower_bound_inclusive_date=incarceration_period.release_date,
        supervision_periods=supervision_periods
    )


def us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date: date,
        lower_bound_inclusive_date: Optional[date],
        supervision_periods: List[StateSupervisionPeriod],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Finds the most recent nonnull supervision period supervision type on the supervision periods, preceding or
    overlapping the provided date. An optional lower bound may be provided to limit the lookback window.

    Returns the most recent StateSupervisionPeriodSupervisionType. If there is no valid supervision
    type found (e.g. the person has only been incarcerated for the time window), returns None.
    """
    supervision_types_by_end_date: Dict[date, Set[StateSupervisionPeriodSupervisionType]] = defaultdict(set)

    lower_bound_exclusive_date = (lower_bound_inclusive_date - relativedelta(days=1)
                                  if lower_bound_inclusive_date else date.min)

    for supervision_period in supervision_periods:
        start_date = supervision_period.start_date

        if not start_date:
            continue

        termination_date = (supervision_period.termination_date
                            if supervision_period.termination_date else date.today())

        supervision_period_supervision_type = supervision_period.supervision_period_supervision_type

        if not supervision_period_supervision_type:
            continue

        if not date_spans_overlap_exclusive(
                start_1=lower_bound_exclusive_date,
                end_1=upper_bound_exclusive_date,
                start_2=start_date,
                end_2=termination_date):
            continue

        supervision_types_by_end_date[termination_date].add(supervision_period_supervision_type)

    if not supervision_types_by_end_date:
        return None

    max_end_date = max(supervision_types_by_end_date.keys())

    return _get_most_relevant_supervision_type(supervision_types_by_end_date[max_end_date])


def _get_supervision_periods_from_sentences(incarceration_sentences: List[StateIncarcerationSentence],
                                            supervision_sentences: List[StateSupervisionSentence]) -> \
        List[StateSupervisionPeriod]:
    """Returns all unique supervision periods associated with any of the given sentences."""
    sentences = itertools.chain(supervision_sentences, incarceration_sentences)
    supervision_period_ids: Set[int] = set()
    supervision_periods: List[StateSupervisionPeriod] = []

    for sentence in sentences:
        if not isinstance(sentence, (StateIncarcerationSentence, StateSupervisionSentence)):
            raise ValueError(f"Sentence has unexpected type {type(sentence)}")

        for supervision_period in sentence.supervision_periods:
            supervision_period_id = supervision_period.supervision_period_id

            if supervision_period_id is not None and supervision_period_id not in supervision_period_ids:
                supervision_periods.append(supervision_period)
                supervision_period_ids.add(supervision_period_id)

    return supervision_periods
