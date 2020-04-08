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
import logging
from typing import List, Set, Optional

from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month
from recidiviz.calculator.pipeline.utils.supervision_type_identification import \
    _sentence_supervision_types_to_supervision_period_supervision_type
from recidiviz.calculator.pipeline.utils.us_mo_sentence_classification import UsMoSupervisionSentence, \
    UsMoIncarcerationSentence
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateSupervisionSentence, StateIncarcerationSentence, \
    StateSupervisionPeriod, StateIncarcerationPeriod


def us_mo_get_pre_incarceration_supervision_type(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Calculates the pre-incarceration supervision type for US_MO people by calculating the supervision period
    supervision type on the day before reincarceration.
    """
    if not incarceration_period.admission_date:
        raise ValueError(
            f'No admission date for incarceration period {incarceration_period.incarceration_period_id}')

    last_day_before_incarceration = incarceration_period.admission_date - datetime.timedelta(days=1)
    return us_mo_get_supervision_period_supervision_type_on_date(last_day_before_incarceration,
                                                                 supervision_sentences,
                                                                 incarceration_sentences)


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
    end_of_month = last_day_of_month(any_date_in_month)
    if supervision_period.termination_date is None:
        supervision_type_determination_date = end_of_month
    else:
        supervision_type_determination_date = min(end_of_month,
                                                  supervision_period.termination_date - datetime.timedelta(days=1))
    return us_mo_get_supervision_period_supervision_type_on_date(supervision_type_determination_date,
                                                                 supervision_sentences,
                                                                 incarceration_sentences)


def us_mo_get_supervision_period_supervision_type_on_date(
        supervision_type_determination_date: datetime.date,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence]) -> StateSupervisionPeriodSupervisionType:
    """Calculates the US_MO supervision period supervision type for any period overlapping with the provided
    |supervision_type_determination_date|.
    """

    supervision_types: Set[Optional[StateSupervisionType]] = set()
    for ss in supervision_sentences:
        if not isinstance(ss, UsMoSupervisionSentence):
            raise ValueError(f'Supervision sentence has unexpected type {type(ss)}')
        supervision_types.add(ss.get_sentence_supervision_type_on_day(supervision_type_determination_date))

    for in_s in incarceration_sentences:
        if not isinstance(in_s, UsMoIncarcerationSentence):
            raise ValueError(f'Incarceration sentence has unexpected type {type(in_s)}')
        supervision_types.add(in_s.get_sentence_supervision_type_on_day(supervision_type_determination_date))

    if supervision_types == {None}:
        logging.warning('Should have at least one valid sentence on [%s] date, found none]',
                        supervision_type_determination_date)

    return _sentence_supervision_types_to_supervision_period_supervision_type(supervision_types)
