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
import logging
from datetime import date
from typing import List, Optional, Any, Dict, Set, Union, Tuple

from dateutil.relativedelta import relativedelta
from pydot import frozendict

from recidiviz.calculator.pipeline.incarceration.incarceration_event import \
    IncarcerationEvent, IncarcerationAdmissionEvent, IncarcerationReleaseEvent, IncarcerationStayEvent
from recidiviz.calculator.pipeline.utils.execution_utils import list_of_dicts_to_dict_with_keys
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import \
    prepare_incarceration_periods_for_calculations
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    get_pre_incarceration_supervision_type, get_post_incarceration_supervision_type
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodStatus, \
    StateIncarcerationPeriodAdmissionReason, is_official_admission, is_official_release
from recidiviz.persistence.entity.entity_utils import get_single_state_code
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSentenceGroup, StateCharge, \
    StateIncarcerationSentence, StateSupervisionSentence, StateSupervisionPeriod, PeriodType


def find_incarceration_events(
        sentence_groups: List[StateSentenceGroup],
        incarceration_period_judicial_district_association: List[Dict[str, Any]],
        county_of_residence: Optional[str]) -> List[IncarcerationEvent]:
    """Finds instances of admission or release from incarceration.

    Transforms the person's StateIncarcerationPeriods, which are connected to their StateSentenceGroups, into
    IncarcerationAdmissionEvents, IncarcerationStayEvents, and IncarcerationReleaseEvents, representing admissions,
    stays in, and releases from incarceration in a state prison.

    Args:
        - sentence_groups: All of the person's StateSentenceGroups
        - incarceration_period_judicial_district_association: A list of dictionaries with information connecting
            StateIncarcerationPeriod ids to the judicial district responsible for the period of incarceration
        - county_of_residence: The person's most recent county of residence

    Returns:
        A list of IncarcerationEvents for the person.
    """
    incarceration_events: List[IncarcerationEvent] = []
    incarceration_sentences = []
    supervision_sentences = []
    for sentence_group in sentence_groups:
        incarceration_sentences.extend(sentence_group.incarceration_sentences)
        supervision_sentences.extend(sentence_group.supervision_sentences)
    incarceration_periods, _supervision_periods = get_unique_periods_from_sentence_groups_and_add_backedges(
        sentence_groups)

    if not incarceration_periods:
        return incarceration_events

    state_code = get_single_state_code(incarceration_periods)

    # Convert the list of dictionaries into one dictionary where the keys are the incarceration_period_id values
    incarceration_period_to_judicial_district = list_of_dicts_to_dict_with_keys(
        incarceration_period_judicial_district_association, key=StateIncarcerationPeriod.get_class_id_name())

    incarceration_events.extend(find_all_stay_events(
        state_code,
        incarceration_sentences,
        supervision_sentences,
        incarceration_periods,
        incarceration_period_to_judicial_district,
        county_of_residence))

    incarceration_events.extend(find_all_admission_release_events(
        state_code,
        incarceration_sentences,
        supervision_sentences,
        incarceration_periods,
        county_of_residence))

    return incarceration_events


def find_all_admission_release_events(
        state_code: str,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        original_incarceration_periods: List[StateIncarcerationPeriod],
        county_of_residence: Optional[str],
) -> List[Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]]:
    """Given the |original_incarceration_periods| generates and returns all IncarcerationAdmissionEvents and
    IncarcerationReleaseEvents.
    """
    incarceration_events: List[Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]] = []

    incarceration_periods_for_admissions = prepare_incarceration_periods_for_calculations(
        state_code,
        original_incarceration_periods,
        collapse_transfers=True,
        collapse_temporary_custody_periods_with_revocation=True,
        collapse_transfers_with_different_pfi=True,
        overwrite_facility_information_in_transfers=False)

    de_duplicated_incarceration_admissions = de_duplicated_admissions(incarceration_periods_for_admissions)

    for incarceration_period in de_duplicated_incarceration_admissions:
        admission_event = admission_event_for_period(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            county_of_residence)

        if admission_event:
            incarceration_events.append(admission_event)

    incarceration_periods_for_releases = prepare_incarceration_periods_for_calculations(
        state_code,
        original_incarceration_periods,
        collapse_transfers=True,
        collapse_temporary_custody_periods_with_revocation=True,
        collapse_transfers_with_different_pfi=True,
        overwrite_facility_information_in_transfers=True)

    de_duplicated_incarceration_releases = de_duplicated_releases(incarceration_periods_for_releases)

    for incarceration_period in de_duplicated_incarceration_releases:
        release_event = release_event_for_period(incarceration_sentences,
                                                 supervision_sentences,
                                                 incarceration_period,
                                                 county_of_residence)

        if release_event:
            incarceration_events.append(release_event)

    return incarceration_events


def find_all_stay_events(
        state_code: str,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        original_incarceration_periods: List[StateIncarcerationPeriod],
        incarceration_period_to_judicial_district: Dict[int, Dict[Any, Any]],
        county_of_residence: Optional[str],
) -> List[IncarcerationStayEvent]:
    """Given the |original_incarceration_periods| generates and returns all IncarcerationStayEvents based on the
    final day relevant months.
    """
    incarceration_stay_events: List[IncarcerationStayEvent] = []

    incarceration_periods = prepare_incarceration_periods_for_calculations(
        state_code,
        original_incarceration_periods,
        collapse_transfers=False,
        collapse_temporary_custody_periods_with_revocation=False,
        collapse_transfers_with_different_pfi=False,
        overwrite_facility_information_in_transfers=False)

    original_admission_reasons_by_period_id = _original_admission_reasons_by_period_id(
        sorted_incarceration_periods=incarceration_periods)

    for incarceration_period in incarceration_periods:
        period_stay_events = find_incarceration_stays(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            original_admission_reasons_by_period_id,
            incarceration_period_to_judicial_district,
            county_of_residence)

        if period_stay_events:
            incarceration_stay_events.extend(period_stay_events)

    return incarceration_stay_events


def find_incarceration_stays(
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        original_admission_reasons_by_period_id:
        Dict[int, Tuple[StateIncarcerationPeriodAdmissionReason, Optional[str]]],
        incarceration_period_to_judicial_district: Dict[int, Dict[Any, Any]],
        county_of_residence: Optional[str]) -> List[IncarcerationStayEvent]:
    """Finds all days for which this person was incarcerated."""
    incarceration_stay_events: List[IncarcerationStayEvent] = []

    admission_date = incarceration_period.admission_date
    release_date = incarceration_period.release_date

    if release_date is None:
        if incarceration_period.status != StateIncarcerationPeriodStatus.IN_CUSTODY:
            # This should not happen after validation.
            raise ValueError("Unexpected missing release date. _infer_missing_dates_and_statuses is not properly"
                             " setting missing dates.")

        # This person is in custody for this period. Set the release date for tomorrow.
        release_date = date.today() + relativedelta(days=1)

    if admission_date is None:
        return incarceration_stay_events

    supervision_type_at_admission = get_pre_incarceration_supervision_type(
        incarceration_sentences, supervision_sentences, incarceration_period)

    sentence_group = _get_sentence_group_for_incarceration_period(incarceration_period)
    judicial_district_code = _get_judicial_district_code(incarceration_period,
                                                         incarceration_period_to_judicial_district)

    stay_date = admission_date

    incarceration_period_id = incarceration_period.incarceration_period_id

    if not incarceration_period_id:
        raise ValueError("Unexpected incarceration period without an incarceration_period_id.")

    original_admission_reason, original_admission_reason_raw_text = \
        original_admission_reasons_by_period_id[incarceration_period_id]

    while stay_date < release_date:
        most_serious_charge = find_most_serious_prior_charge_in_sentence_group(sentence_group, stay_date)
        most_serious_offense_ncic_code = most_serious_charge.ncic_code if most_serious_charge else None
        most_serious_offense_statute = most_serious_charge.statute if most_serious_charge else None

        incarceration_stay_events.append(
            IncarcerationStayEvent(
                state_code=incarceration_period.state_code,
                event_date=stay_date,
                facility=incarceration_period.facility,
                county_of_residence=county_of_residence,
                most_serious_offense_ncic_code=most_serious_offense_ncic_code,
                most_serious_offense_statute=most_serious_offense_statute,
                admission_reason=original_admission_reason,
                admission_reason_raw_text=original_admission_reason_raw_text,
                supervision_type_at_admission=supervision_type_at_admission,
                judicial_district_code=judicial_district_code,
                specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration
            )
        )

        stay_date = stay_date + relativedelta(days=1)

    return incarceration_stay_events


def _get_judicial_district_code(
        incarceration_period: StateIncarcerationPeriod,
        incarceration_period_to_judicial_district: Dict[int, Dict[Any, Any]]) -> Optional[str]:
    """Retrieves the judicial_district_code corresponding to the incarceration period, if one exists."""
    incarceration_period_id = incarceration_period.incarceration_period_id

    if incarceration_period_id is None:
        raise ValueError("Unexpected unset incarceration_period_id.")

    ip_info = incarceration_period_to_judicial_district.get(incarceration_period_id)

    if ip_info is not None:
        return ip_info.get('judicial_district_code')

    return None


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
        # Mypy complains that ncic_code might be None, even though that has already been filtered above.
        return min(relevant_charges, key=lambda b: b.ncic_code) # type: ignore[type-var]

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
        county_of_residence: Optional[str]) -> Optional[IncarcerationAdmissionEvent]:
    """Returns an IncarcerationAdmissionEvent if this incarceration period represents an admission to incarceration."""

    admission_date = incarceration_period.admission_date
    admission_reason = incarceration_period.admission_reason
    supervision_type_at_admission = get_pre_incarceration_supervision_type(
        incarceration_sentences,
        supervision_sentences,
        incarceration_period)
    specialized_purpose_for_incarceration = incarceration_period.specialized_purpose_for_incarceration

    if admission_date and admission_reason:
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
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        county_of_residence: Optional[str]) -> Optional[IncarcerationReleaseEvent]:
    """Returns an IncarcerationReleaseEvent if this incarceration period represents an release from incarceration."""
    release_date = incarceration_period.release_date
    release_reason = incarceration_period.release_reason
    purpose_for_incarceration = incarceration_period.specialized_purpose_for_incarceration

    if release_date and release_reason:
        supervision_type_at_release = get_post_incarceration_supervision_type(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period)

        total_days_incarcerated = None

        if incarceration_period.admission_date:
            total_days_incarcerated = (release_date - incarceration_period.admission_date).days

            if total_days_incarcerated < 0:
                logging.warning("release_date before admission_date on incarceration period: %s", incarceration_period)
                total_days_incarcerated = 0

        return IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=release_date,
            facility=incarceration_period.facility,
            release_reason=release_reason,
            release_reason_raw_text=incarceration_period.release_reason_raw_text,
            purpose_for_incarceration=purpose_for_incarceration,
            county_of_residence=county_of_residence,
            supervision_type_at_release=supervision_type_at_release,
            admission_reason=incarceration_period.admission_reason,
            total_days_incarcerated=total_days_incarcerated
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
        # TODO(#2888): Remove this once these bi-directional relationships are hydrated properly
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


def _original_admission_reasons_by_period_id(sorted_incarceration_periods: List[StateIncarcerationPeriod]) -> \
        Dict[int, Tuple[StateIncarcerationPeriodAdmissionReason, Optional[str]]]:
    """Determines the original admission reason each period of incarceration. Returns a dictionary mapping
    incarceration_period_id values to the original admission_reason and corresponding admission_reason_raw_text that
    started the period of being incarcerated. People are often transferred between facilities during their time
    incarcerated, so this in practice is the most recent non-transfer admission reason for the given incarceration
    period."""
    original_admission_reasons_by_period_id: \
        Dict[int, Tuple[StateIncarcerationPeriodAdmissionReason, Optional[str]]] = {}

    most_recent_official_admission_reason = None
    most_recent_official_admission_reason_raw_text = None

    for index, incarceration_period in enumerate(sorted_incarceration_periods):
        incarceration_period_id = incarceration_period.incarceration_period_id

        if not incarceration_period_id:
            raise ValueError("Unexpected incarceration period without a incarceration_period_id.")

        if not incarceration_period.admission_reason:
            raise ValueError(
                "Incarceration period pre-processing is not setting missing admission_reasons correctly.")

        if index == 0 or is_official_admission(incarceration_period.admission_reason):
            # These indicate that incarceration is "officially" starting
            most_recent_official_admission_reason = incarceration_period.admission_reason
            most_recent_official_admission_reason_raw_text = incarceration_period.admission_reason_raw_text

        if not most_recent_official_admission_reason:
            original_admission_reasons_by_period_id[incarceration_period_id] = \
                (incarceration_period.admission_reason, incarceration_period.admission_reason_raw_text)
        else:
            original_admission_reasons_by_period_id[incarceration_period_id] = \
                (most_recent_official_admission_reason, most_recent_official_admission_reason_raw_text)

        if is_official_release(incarceration_period.release_reason):
            # If the release from this period of incarceration indicates an official end to the period of incarceration,
            # then subsequent periods should not share the most recent admission reason.
            most_recent_official_admission_reason = None
            most_recent_official_admission_reason_raw_text = None

    return original_admission_reasons_by_period_id
