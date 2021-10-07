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
"""Utils for region controller tests."""
import datetime
from typing import List, Optional

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
)
from recidiviz.persistence.entity.state import entities


def populate_person_backedges(persons: List[entities.StatePerson]) -> None:
    for person in persons:
        children = get_all_entities_from_tree(person, CoreEntityFieldIndex())
        for child in children:
            if (
                child is not person
                and hasattr(child, "person")
                and getattr(child, "person", None) is None
            ):
                child.set_field("person", person)


def build_state_person_entity(
    state_code: str,
    full_name: str,
    gender: entities.Gender,
    gender_raw_text: str,
    birthdate: datetime.date,
    id_type: str,
    external_id: Optional[str] = None,
    race_raw_text: Optional[str] = None,
    race: Optional[entities.Race] = None,
    ethnicity_raw_text: Optional[str] = None,
    ethnicity: Optional[entities.Ethnicity] = None,
) -> entities.StatePerson:
    """Build a StatePerson entity with optional state_person_external_id, state_person_race
    and state_person_ethnicity entities appended"""
    state_person = entities.StatePerson.new_with_defaults(
        state_code=state_code,
        full_name=full_name,
        gender=gender,
        gender_raw_text=gender_raw_text,
        birthdate=birthdate,
    )
    if external_id:
        add_external_id_to_person(
            state_person,
            external_id=external_id,
            id_type=id_type,
            state_code=state_code,
        )
    if race and race_raw_text:
        add_race_to_person(
            state_person,
            race=race,
            race_raw_text=race_raw_text,
            state_code=state_code,
        )
    if ethnicity and ethnicity_raw_text:
        add_ethnicity_to_person(
            state_person,
            ethnicity_raw_text=ethnicity_raw_text,
            ethnicity=ethnicity,
            state_code=state_code,
        )

    return state_person


def add_race_to_person(
    person: entities.StatePerson,
    race_raw_text: str,
    race: entities.Race,
    state_code: str,
) -> None:
    """Append race to the person (updates the person entity in place)."""
    race_to_add: entities.StatePersonRace = entities.StatePersonRace.new_with_defaults(
        state_code=state_code,
        race=race,
        race_raw_text=race_raw_text,
        person=person,
    )
    person.races.append(race_to_add)


def add_ethnicity_to_person(
    person: entities.StatePerson,
    ethnicity_raw_text: str,
    ethnicity: entities.Ethnicity,
    state_code: str,
) -> None:
    """Append ethnicity to the person (updates the person entity in place)."""
    ethnicity_to_add: entities.StatePersonEthnicity = (
        entities.StatePersonEthnicity.new_with_defaults(
            state_code=state_code,
            ethnicity=ethnicity,
            ethnicity_raw_text=ethnicity_raw_text,
            person=person,
        )
    )
    person.ethnicities.append(ethnicity_to_add)


def add_external_id_to_person(
    person: entities.StatePerson, external_id: str, id_type: str, state_code: str
) -> None:
    """Append external id to the person (updates the person entity in place)."""
    external_id_to_add: entities.StatePersonExternalId = (
        entities.StatePersonExternalId.new_with_defaults(
            state_code=state_code,
            external_id=external_id,
            id_type=id_type,
            person=person,
        )
    )
    person.external_ids.append(external_id_to_add)


def add_incarceration_period_to_person(
    person: entities.StatePerson,
    state_code: str,
    incarceration_sentence: entities.StateIncarcerationSentence,
    external_id: str,
    status: StateIncarcerationPeriodStatus,
    admission_date: datetime.date,
    release_date: Optional[datetime.date],
    facility: str,
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason],
    admission_reason_raw_text: str,
    release_reason: Optional[StateIncarcerationPeriodReleaseReason],
    release_reason_raw_text: str,
) -> None:
    """Append an incarceration period to the person (updates the person entity in place)."""

    incarceration_period = entities.StateIncarcerationPeriod.new_with_defaults(
        external_id=external_id,
        state_code=state_code,
        status=status,
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        admission_date=admission_date,
        release_date=release_date,
        county_code=None,
        facility=facility,
        admission_reason=admission_reason,
        admission_reason_raw_text=admission_reason_raw_text,
        release_reason=release_reason,
        release_reason_raw_text=release_reason_raw_text,
        person=person,
        incarceration_sentences=[incarceration_sentence],
    )

    incarceration_sentence.incarceration_periods.append(incarceration_period)
