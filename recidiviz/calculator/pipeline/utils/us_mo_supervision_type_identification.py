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
"""US_MO-specific implementations of functions related to supervision type identification."""
import datetime
import itertools
from collections import defaultdict
from typing import List, Set, Optional, Dict

from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month, first_day_of_month
from recidiviz.calculator.pipeline.utils.supervision_type_identification import \
    _sentence_supervision_types_to_supervision_period_supervision_type
from recidiviz.calculator.pipeline.utils.us_mo_sentence_classification import UsMoSentenceMixin
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionSentence, StateIncarcerationSentence, \
    StateSupervisionPeriod, StateIncarcerationPeriod


def us_mo_get_pre_incarceration_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the pre-incarceration supervision type for US_MO people by calculating the most recent type of
    supervision a given person was on.
    """
    if not incarceration_period.admission_date:
        raise ValueError(
            f'No admission date for incarceration period {incarceration_period.incarceration_period_id}')

    return us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date=incarceration_period.admission_date,
        lower_bound_inclusive_date=None,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences
    )


def us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date: datetime.date,
        lower_bound_inclusive_date: Optional[datetime.date],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],

) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Finds the most recent nonnull supervision period supervision type associated the person with these sentences,
    preceding the provided date. An optional lower bound may be provided to limit the lookback window.

    Returns a tuple (last valid date of that supervision type span, supervision type). If there is no valid supervision
    type found (e.g. the person has only been incarcerated for the time window).
    """
    supervision_types_by_end_date: Dict[datetime.date, Set[Optional[StateSupervisionType]]] = defaultdict(set)
    sentences = itertools.chain(supervision_sentences, incarceration_sentences)
    for sentence in sentences:
        if not isinstance(sentence, UsMoSentenceMixin):
            raise ValueError(f'Sentence has unexpected type {type(sentence)}')
        res = sentence.get_most_recent_supervision_type_before_upper_bound_day(
            upper_bound_exclusive_date=upper_bound_exclusive_date,
            lower_bound_inclusive_date=lower_bound_inclusive_date)
        if res:
            last_supervision_date, supervision_type = res
            supervision_types_by_end_date[last_supervision_date].add(supervision_type)

    if not supervision_types_by_end_date:
        return None

    max_end_date = max(supervision_types_by_end_date.keys())

    return _sentence_supervision_types_to_supervision_period_supervision_type(
        supervision_types_by_end_date[max_end_date])


def us_mo_get_month_supervision_type(
        any_date_in_month: datetime.date,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: StateSupervisionPeriod
) -> StateSupervisionPeriodSupervisionType:
    """Calculates the supervision period supervision type that should be attributed to a US_MO supervision period
    on a given month.

    The date used to calculate the supervision period supervision type is either the last day of the month, or
    the last day of supervision, whichever comes first.
    """
    start_of_month = first_day_of_month(any_date_in_month)
    end_of_month = last_day_of_month(any_date_in_month)
    first_of_next_month = end_of_month + datetime.timedelta(days=1)

    if supervision_period.termination_date is None:
        upper_bound_exclusive_date = first_of_next_month
    else:
        upper_bound_exclusive_date = min(first_of_next_month, supervision_period.termination_date)

    lower_bound_inclusive = max(start_of_month, supervision_period.start_date)

    supervision_type = \
        us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
            upper_bound_exclusive_date=upper_bound_exclusive_date,
            lower_bound_inclusive_date=lower_bound_inclusive,
            supervision_sentences=supervision_sentences,
            incarceration_sentences=incarceration_sentences)

    if not supervision_type:
        return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

    return supervision_type


def us_mo_get_supervision_period_supervision_type_on_date(
        supervision_type_determination_date: datetime.date,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence]) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the US_MO supervision period supervision type for any period overlapping with the provided
    |supervision_type_determination_date|.
    """

    upper_bound_exclusive_date = supervision_type_determination_date + datetime.timedelta(days=1)

    return us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
        upper_bound_exclusive_date=upper_bound_exclusive_date,
        lower_bound_inclusive_date=supervision_type_determination_date,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences
    )
