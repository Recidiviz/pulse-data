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
"""Identifies instances of admission and release from incarceration."""
from datetime import date
from typing import List, Optional, Any, Dict, Set, Union, Tuple

from dateutil.relativedelta import relativedelta
from pydot import frozendict

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent, \
    IncarcerationReleaseEvent, IncarcerationStayEvent
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    last_day_of_month
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.calculator.pipeline.utils.supervision_period_utils import \
    _get_relevant_supervision_periods_before_admission_date
from recidiviz.calculator.pipeline.utils.supervision_type_identification import get_pre_incarceration_supervision_type
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSentenceGroup, StateCharge, \
    StateIncarcerationSentence, StateSupervisionSentence, StateSupervisionPeriod, PeriodType


def find_incarceration_events(
        sentence_groups: List[StateSentenceGroup],
        county_of_residence: Optional[str]) -> List[IncarcerationEvent]:
    """Finds instances of admission or release from incarceration.

    Transforms StateIncarcerationPeriods into IncarcerationAdmissionEvents,
    IncarcerationStayEvents, and IncarcerationReleaseEvents, representing
    admissions, stays in, and releases from incarceration in a state prison.

    Args:
        - incarceration_periods: All of the person's StateIncarcerationPeriods

    Returns:
        A list of IncarcerationEvents for the person.
    """
    incarceration_events: List[IncarcerationEvent] = []
    incarceration_sentences = []
    supervision_sentences = []
    for sentence_group in sentence_groups:
        incarceration_sentences.extend(sentence_group.incarceration_sentences)
        supervision_sentences.extend(sentence_group.supervision_sentences)
    incarceration_periods, supervision_periods = get_unique_periods_from_sentence_groups_and_add_backedges(
        sentence_groups)

    if not incarceration_periods:
        return incarceration_events

    incarceration_events.extend(find_all_end_of_month_state_prison_stay_events(
        incarceration_sentences,
        supervision_sentences,
        incarceration_periods,
        supervision_periods,
        county_of_residence))

    incarceration_events.extend(find_all_admission_release_events(
        incarceration_sentences,
        supervision_sentences,
        incarceration_periods,
        supervision_periods,
        county_of_residence))

    return incarceration_events


def find_all_admission_release_events(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        original_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: List[StateSupervisionPeriod],
        county_of_residence: Optional[str],
) -> List[Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]]:
    """Given the |original_incarceration_periods| generates and returns all IncarcerationAdmissionEvents and
    IncarcerationReleaseEvents.
    """
    incarceration_events: List[Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]] = []

    incarceration_periods = prepare_incarceration_periods_for_calculations(
        original_incarceration_periods, collapse_temporary_custody_periods_with_revocation=True)

    de_duplicated_incarceration_admissions = de_duplicated_admissions(incarceration_periods)

    for incarceration_period in de_duplicated_incarceration_admissions:
        admission_event = admission_event_for_period(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            supervision_periods,
            county_of_residence)

        if admission_event:
            incarceration_events.append(admission_event)

    de_duplicated_incarceration_releases = de_duplicated_releases(incarceration_periods)

    for incarceration_period in de_duplicated_incarceration_releases:
        release_event = release_event_for_period(incarceration_period, county_of_residence)

        if release_event:
            incarceration_events.append(release_event)

    return incarceration_events


def find_all_end_of_month_state_prison_stay_events(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        original_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_periods: List[StateSupervisionPeriod],
        county_of_residence: Optional[str],
) -> List[IncarcerationStayEvent]:
    """Given the |original_incarceration_periods| generates and returns all IncarcerationStayEvents based on the
    final day relevant months.
    """
    incarceration_stay_events: List[IncarcerationStayEvent] = []

    incarceration_periods = prepare_incarceration_periods_for_calculations(
        original_incarceration_periods, collapse_transfers=False)

    for incarceration_period in incarceration_periods:
        period_stay_events = find_end_of_month_state_prison_stays(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            supervision_periods,
            county_of_residence)

        if period_stay_events:
            incarceration_stay_events.extend(period_stay_events)

    return incarceration_stay_events


def find_end_of_month_state_prison_stays(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        supervision_periods: List[StateSupervisionPeriod],
        county_of_residence: Optional[str]) -> List[IncarcerationStayEvent]:
    """Finds months for which this person was incarcerated in a state prison on the last day of the month.
    """
    incarceration_stay_events: List[IncarcerationStayEvent] = []

    if incarceration_period.incarceration_type != StateIncarcerationType.STATE_PRISON:
        return incarceration_stay_events

    admission_date = incarceration_period.admission_date
    release_date = incarceration_period.release_date

    if release_date is None:
        release_date = date.today()

    if admission_date is None:
        return incarceration_stay_events

    relevant_pre_incarceration_supervision_periods = \
        _get_relevant_supervision_periods_before_admission_date(admission_date, supervision_periods)
    supervision_type_at_admission = get_pre_incarceration_supervision_type(
        incarceration_sentences, supervision_sentences, incarceration_period,
        relevant_pre_incarceration_supervision_periods)

    sentence_group = _get_sentence_group_for_incarceration_period(incarceration_period)

    end_of_month = last_day_of_month(admission_date)

    while end_of_month <= release_date:
        most_serious_charge = find_most_serious_prior_charge_in_sentence_group(sentence_group, end_of_month)
        most_serious_offense_ncic_code = most_serious_charge.ncic_code if most_serious_charge else None
        most_serious_offense_statute = most_serious_charge.statute if most_serious_charge else None

        incarceration_stay_events.append(
            IncarcerationStayEvent(
                state_code=incarceration_period.state_code,
                event_date=end_of_month,
                facility=incarceration_period.facility,
                county_of_residence=county_of_residence,
                most_serious_offense_ncic_code=most_serious_offense_ncic_code,
                most_serious_offense_statute=most_serious_offense_statute,
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                supervision_type_at_admission=supervision_type_at_admission,
            )
        )

        end_of_month = last_day_of_month(end_of_month + relativedelta(days=1))

    return incarceration_stay_events


def _get_sentence_group_for_incarceration_period(
        incarceration_period: StateIncarcerationPeriod
) -> StateSentenceGroup:
    sentence_groups = [
        incarceration_sentence.sentence_group for incarceration_sentence in incarceration_period.incarceration_sentences
        if incarceration_sentence.sentence_group
    ]

    sentence_groups.extend([
        supervision_sentence.sentence_group for supervision_sentence in incarceration_period.supervision_sentences
        if supervision_sentence.sentence_group
    ])

    sentence_group_ids = {sentence_group.sentence_group_id for sentence_group in sentence_groups}

    if len(sentence_group_ids) != 1:
        raise ValueError(f"Found unexpected number of sentence groups attached to incarceration_period "
                         f"[{incarceration_period.incarceration_period_id}]. Ids: [{str(sentence_group_ids)}].")

    return sentence_groups[0]


def find_most_serious_prior_charge_in_sentence_group(sentence_group: StateSentenceGroup,
                                                     sentence_start_upper_bound: date) -> Optional[StateCharge]:
    """Finds the most serious offense associated with a sentence that started prior to the cutoff date and is within the
    same sentence group as the incarceration period.

    StateCharges have an optional `ncic_code` field that contains the identifying NCIC code for the offense, as well as
    a `statute` field that describes the offense. NCIC codes decrease in severity as the code increases. E.g. '1010' is
    a more serious offense than '5599'. Therefore, this returns the statute associated with the lowest ranked NCIC code
    attached to the charges in the sentence groups that this incarceration period is attached to. Although most NCIC
    codes are numbers, some may contain characters such as the letter 'A', so the codes are sorted alphabetically.
    """

    relevant_charges: List[StateCharge] = []

    def is_relevant_charge(sentence_start_date: Optional[date], charge: StateCharge):
        return charge.ncic_code and sentence_start_date and sentence_start_date < sentence_start_upper_bound

    for incarceration_sentence in sentence_group.incarceration_sentences:
        relevant_charges_in_sentence = [charge for charge in incarceration_sentence.charges
                                        if is_relevant_charge(incarceration_sentence.start_date, charge)]
        relevant_charges.extend(relevant_charges_in_sentence)

    for supervision_sentence in sentence_group.supervision_sentences:
        relevant_charges_in_sentence = [charge for charge in supervision_sentence.charges
                                        if is_relevant_charge(supervision_sentence.start_date, charge)]
        relevant_charges.extend(relevant_charges_in_sentence)

    if relevant_charges:
        relevant_charges.sort(key=lambda b: b.ncic_code)
        return relevant_charges[0]

    return None


def de_duplicated_admissions(incarceration_periods: List[StateIncarcerationPeriod]) -> List[StateIncarcerationPeriod]:
    """Returns a list of incarceration periods that are de-duplicated
    for any incarceration periods that share state_code,
    admission_date, admission_reason, and facility."""

    unique_admission_dicts: Set[Dict[str, Any]] = set()

    unique_incarceration_admissions: List[StateIncarcerationPeriod] = []

    for incarceration_period in incarceration_periods:
        admission_dict = frozendict({
            'state_code': incarceration_period.state_code,
            'admission_date': incarceration_period.admission_date,
            'admission_reason': incarceration_period.admission_reason,
            'facility': incarceration_period.facility,
        })

        if admission_dict not in unique_admission_dicts:
            unique_incarceration_admissions.append(incarceration_period)

        unique_admission_dicts.add(admission_dict)

    return unique_incarceration_admissions


def de_duplicated_releases(incarceration_periods: List[StateIncarcerationPeriod]) -> List[StateIncarcerationPeriod]:
    """Returns a list of incarceration periods that are de-duplicated
    for any incarceration periods that share state_code,
    release_date, release_reason, and facility."""

    unique_release_dicts: Set[Dict[str, Any]] = set()

    unique_incarceration_releases: List[StateIncarcerationPeriod] = []

    for incarceration_period in incarceration_periods:
        release_dict = frozendict({
            'state_code': incarceration_period.state_code,
            'release_date': incarceration_period.release_date,
            'release_reason': incarceration_period.release_reason,
            'facility': incarceration_period.facility
        })

        if release_dict not in unique_release_dicts:
            unique_incarceration_releases.append(incarceration_period)

        unique_release_dicts.add(release_dict)

    return unique_incarceration_releases


def admission_event_for_period(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        supervision_periods: List[StateSupervisionPeriod],
        county_of_residence: Optional[str]) -> Optional[IncarcerationAdmissionEvent]:
    """Returns an IncarcerationAdmissionEvent if this incarceration period represents an admission to incarceration."""

    admission_date = incarceration_period.admission_date
    admission_reason = incarceration_period.admission_reason
    relevant_pre_incarceration_supervision_periods = \
        _get_relevant_supervision_periods_before_admission_date(admission_date, supervision_periods)
    supervision_type_at_admission = get_pre_incarceration_supervision_type(
        incarceration_sentences,
        supervision_sentences,
        incarceration_period,
        relevant_pre_incarceration_supervision_periods)
    incarceration_type = incarceration_period.incarceration_type
    specialized_purpose_for_incarceration = incarceration_period.specialized_purpose_for_incarceration

    if admission_date and admission_reason and incarceration_type == StateIncarcerationType.STATE_PRISON:
        return IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=admission_date,
            facility=incarceration_period.facility,
            admission_reason=admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            supervision_type_at_admission=supervision_type_at_admission,
            specialized_purpose_for_incarceration=specialized_purpose_for_incarceration,
            county_of_residence=county_of_residence,
        )

    return None


def release_event_for_period(
        incarceration_period: StateIncarcerationPeriod,
        county_of_residence: Optional[str]) -> Optional[IncarcerationReleaseEvent]:
    """Returns an IncarcerationReleaseEvent if this incarceration period represents an release from incarceration."""

    release_date = incarceration_period.release_date
    release_reason = incarceration_period.release_reason
    incarceration_type = incarceration_period.incarceration_type

    if release_date and release_reason and \
            incarceration_type == StateIncarcerationType.STATE_PRISON:
        return IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=release_date,
            facility=incarceration_period.facility,
            release_reason=release_reason,
            county_of_residence=county_of_residence,
        )

    return None


def get_unique_periods_from_sentence_groups_and_add_backedges(
        sentence_groups: List[StateSentenceGroup]
) -> Tuple[List[StateIncarcerationPeriod], List[StateSupervisionPeriod]]:
    """Returns a list of unique StateIncarcerationPeriods and StateSupervisionSentences hanging off of the
    StateSentenceGroups. Additionally adds sentence backedges to all StateIncarcerationPeriods/StateSupervisionPeriods.
    """

    incarceration_periods_and_sentences: \
        List[Tuple[Union[StateSupervisionSentence, StateIncarcerationSentence], StateIncarcerationPeriod]] = []
    supervision_periods_and_sentences: \
        List[Tuple[Union[StateSupervisionSentence, StateIncarcerationSentence], StateSupervisionPeriod]] = []

    for sentence_group in sentence_groups:
        for incarceration_sentence in sentence_group.incarceration_sentences:
            for incarceration_period in incarceration_sentence.incarceration_periods:
                incarceration_periods_and_sentences.append((incarceration_sentence, incarceration_period))
            for supervision_period in incarceration_sentence.supervision_periods:
                supervision_periods_and_sentences.append((incarceration_sentence, supervision_period))
        for supervision_sentence in sentence_group.supervision_sentences:
            for incarceration_period in supervision_sentence.incarceration_periods:
                incarceration_periods_and_sentences.append((supervision_sentence, incarceration_period))
            for supervision_period in supervision_sentence.supervision_periods:
                supervision_periods_and_sentences.append((supervision_sentence, supervision_period))

    unique_incarceration_periods = _set_backedges_and_return_unique_periods(incarceration_periods_and_sentences)
    unique_supervision_periods = _set_backedges_and_return_unique_periods(supervision_periods_and_sentences)

    return unique_incarceration_periods, unique_supervision_periods


def _set_backedges_and_return_unique_periods(
        sentences_and_periods: List[Tuple[Union[StateSupervisionSentence, StateIncarcerationSentence], PeriodType]]
) -> List[PeriodType]:
    period_ids: Set[int] = set()

    unique_periods = []
    for sentence, period in sentences_and_periods:
        # TODO(2888): Remove this once these bi-directional relationships are hydrated properly
        # Setting this manually because this direction of hydration doesn't happen in the hydration steps
        if isinstance(sentence, StateSupervisionSentence):
            period.supervision_sentences.append(sentence)
        if isinstance(sentence, StateIncarcerationSentence):
            period.incarceration_sentences.append(sentence)

        period_id = period.get_id()
        if not period_id:
            raise ValueError('No period id found for incarceration period.')
        if period_id and period_id not in period_ids:
            period_ids.add(period_id)
            unique_periods.append(period)

    return unique_periods
