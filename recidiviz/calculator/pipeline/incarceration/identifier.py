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
from typing import List, Optional, Any, Dict, Set, Union

from dateutil.relativedelta import relativedelta
from pydot import frozendict

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent, \
    IncarcerationReleaseEvent, IncarcerationStayEvent
from recidiviz.calculator.pipeline.utils.calculator_utils import \
    last_day_of_month
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.persistence.entity.entity_utils import get_single_state_code
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSentenceGroup, StateCharge


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
    incarceration_periods = get_incarceration_periods_from_sentence_groups(sentence_groups)

    if not incarceration_periods:
        return incarceration_events

    state_code = get_single_state_code(incarceration_periods)

    incarceration_events.extend(
        find_all_end_of_month_state_prison_stay_events(state_code, incarceration_periods, county_of_residence))

    incarceration_events.extend(find_all_admission_release_events(incarceration_periods, county_of_residence))

    return incarceration_events


def find_all_admission_release_events(
        original_incarceration_periods: List[StateIncarcerationPeriod],
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
        admission_event = admission_event_for_period(incarceration_period, county_of_residence)

        if admission_event:
            incarceration_events.append(admission_event)

    de_duplicated_incarceration_releases = de_duplicated_releases(incarceration_periods)

    for incarceration_period in de_duplicated_incarceration_releases:
        release_event = release_event_for_period(incarceration_period, county_of_residence)

        if release_event:
            incarceration_events.append(release_event)

    return incarceration_events


def find_all_end_of_month_state_prison_stay_events(
        _state_code: str,
        original_incarceration_periods: List[StateIncarcerationPeriod],
        county_of_residence: Optional[str],
) -> List[IncarcerationStayEvent]:
    """Given the |original_incarceration_periods| generates and returns all IncarcerationStayEvents based on the
    final day relevant months.
    """
    incarceration_stay_events: List[IncarcerationStayEvent] = []

    incarceration_periods = prepare_incarceration_periods_for_calculations(
        original_incarceration_periods, collapse_transfers=False)

    for incarceration_period in incarceration_periods:
        period_stay_events = find_end_of_month_state_prison_stays(incarceration_period, county_of_residence)

        if period_stay_events:
            incarceration_stay_events.extend(period_stay_events)

    return incarceration_stay_events


def find_end_of_month_state_prison_stays(incarceration_period: StateIncarcerationPeriod,
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

    end_of_month = last_day_of_month(admission_date)

    while end_of_month <= release_date:
        most_serious_offense_statute = find_most_serious_prior_offense_statute_in_sentence_group(
            incarceration_period, end_of_month)

        incarceration_stay_events.append(
            IncarcerationStayEvent(
                state_code=incarceration_period.state_code,
                event_date=end_of_month,
                facility=incarceration_period.facility,
                county_of_residence=county_of_residence,
                most_serious_offense_statute=most_serious_offense_statute,
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            )
        )

        end_of_month = last_day_of_month(end_of_month + relativedelta(days=1))

    return incarceration_stay_events


def find_most_serious_prior_offense_statute_in_sentence_group(incarceration_period, cutoff_date) -> Optional[str]:
    """Finds the most serious offense that occurred prior to the cutoff date and is within the same sentence groups
    that are connected to the incarceration period.

    StateCharges have an optional `ncic_code` field that contains the identifying NCIC code for the offense, as well as
    a `statute` field that describes the offense. NCIC codes decrease in severity as the code increases. E.g. '1010' is
    a more serious offense than '5599'. Therefore, this returns the statute associated with the lowest ranked NCIC code
    attached to the charges in the sentence groups that this incarceration period is attached to. Although most NCIC
    codes are usually numbers, some may contain characters such as the letter 'A', so the codes are sorted
    alphabetically.
    """
    sentence_groups = [
        incarceration_sentence.sentence_group for incarceration_sentence in incarceration_period.incarceration_sentences
        if incarceration_sentence.sentence_group
    ]

    sentence_groups.extend([
        supervision_sentence.sentence_group for supervision_sentence in incarceration_period.supervision_sentences
        if supervision_sentence.sentence_group
    ])

    charges_in_sentence_group: List[StateCharge] = []

    for sentence_group in sentence_groups:
        if sentence_group.incarceration_sentences:
            for incarceration_sentence in sentence_group.incarceration_sentences:
                if incarceration_sentence.charges:
                    charges_in_sentence_group.extend(incarceration_sentence.charges)

        if sentence_group.supervision_sentences:
            for supervision_sentence in sentence_group.supervision_sentences:
                if supervision_sentence.charges:
                    charges_in_sentence_group.extend(supervision_sentence.charges)

    relevant_charges: List[StateCharge] = []

    if charges_in_sentence_group:
        for charge in charges_in_sentence_group:
            if charge.ncic_code and charge.offense_date and charge.offense_date < cutoff_date:
                relevant_charges.append(charge)

        if relevant_charges:
            relevant_charges.sort(key=lambda b: b.ncic_code)
            return relevant_charges[0].statute

    return None


def de_duplicated_admissions(incarceration_periods:
                             List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
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


def de_duplicated_releases(incarceration_periods:
                           List[StateIncarcerationPeriod]) -> \
        List[StateIncarcerationPeriod]:
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
        incarceration_period: StateIncarcerationPeriod,
        county_of_residence: Optional[str]) \
        -> Optional[IncarcerationAdmissionEvent]:
    """Returns an IncarcerationAdmissionEvent if this incarceration period represents an admission to incarceration."""

    admission_date = incarceration_period.admission_date
    admission_reason = incarceration_period.admission_reason
    incarceration_type = incarceration_period.incarceration_type
    specialized_purpose_for_incarceration = incarceration_period.specialized_purpose_for_incarceration

    if admission_date and admission_reason and incarceration_type == StateIncarcerationType.STATE_PRISON:
        return IncarcerationAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=admission_date,
            facility=incarceration_period.facility,
            admission_reason=admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            specialized_purpose_for_incarceration=specialized_purpose_for_incarceration,
            county_of_residence=county_of_residence,
        )

    return None


def release_event_for_period(
        incarceration_period: StateIncarcerationPeriod,
        county_of_residence: Optional[str]) \
        -> Optional[IncarcerationReleaseEvent]:
    """Returns an IncarcerationReleaseEvent if this incarceration period
    represents an release from incarceration."""

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


def get_incarceration_periods_from_sentence_groups(sentence_groups: List[StateSentenceGroup]) -> \
        List[StateIncarcerationPeriod]:
    """Returns a list of unique StateIncarcerationPeriods hanging off of the StateSentenceGroups."""
    incarceration_periods: List[StateIncarcerationPeriod] = []

    incarceration_period_ids: Set[int] = set()

    for sentence_group in sentence_groups:
        incarceration_sentences = sentence_group.incarceration_sentences

        for incarceration_sentence in incarceration_sentences:
            if incarceration_sentence.incarceration_periods:
                for incarceration_period in incarceration_sentence.incarceration_periods:
                    # TODO(2888): Remove this once these bi-directional relationships are hydrated properly
                    # Setting this manually because this direction of hydration doesn't happen in the hydration steps
                    incarceration_period.incarceration_sentences.append(incarceration_sentence)

                    # This is to silence mypy warnings - the incarceration_period_id will always be set
                    if incarceration_period.incarceration_period_id and \
                            incarceration_period.incarceration_period_id not in incarceration_period_ids:
                        incarceration_periods.append(incarceration_period)
                        incarceration_period_ids.add(incarceration_period.incarceration_period_id)

        supervision_sentences = sentence_group.supervision_sentences

        for supervision_sentence in supervision_sentences:
            if supervision_sentence.incarceration_periods:
                for incarceration_period in supervision_sentence.incarceration_periods:
                    # TODO(2888): Remove this once these bi-directional relationships are hydrated properly
                    # Setting this manually because this direction of hydration doesn't happen in the hydration steps
                    incarceration_period.supervision_sentences.append(supervision_sentence)

                    # This is to silence mypy warnings - the incarceration_period_id will always be set
                    if incarceration_period.incarceration_period_id and \
                            incarceration_period.incarceration_period_id not in incarceration_period_ids:
                        incarceration_periods.append(incarceration_period)
                        incarceration_period_ids.add(incarceration_period.incarceration_period_id)

    return incarceration_periods
