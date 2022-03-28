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

from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
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
    if ethnicity:
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
    ethnicity_raw_text: Optional[str],
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
    external_id: str,
    admission_date: datetime.date,
    release_date: Optional[datetime.date],
    facility: str,
    housing_unit: Optional[str] = None,
    custodial_authority: Optional[StateCustodialAuthority] = None,
    custodial_authority_raw_text: Optional[str] = None,
    admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = None,
    admission_reason_raw_text: Optional[str] = None,
    release_reason: Optional[StateIncarcerationPeriodReleaseReason] = None,
    release_reason_raw_text: Optional[str] = None,
    specialized_purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = None,
    incarceration_type_raw_text: Optional[str] = None,
    specialized_purpose_for_incarceration_raw_text: Optional[str] = None,
) -> None:
    """Append an incarceration period to the person (updates the person entity in place)."""

    incarceration_period = entities.StateIncarcerationPeriod.new_with_defaults(
        external_id=external_id,
        state_code=state_code,
        incarceration_type=StateIncarcerationType.STATE_PRISON,
        admission_date=admission_date,
        release_date=release_date,
        county_code=None,
        facility=facility,
        housing_unit=housing_unit,
        custodial_authority=custodial_authority,
        custodial_authority_raw_text=custodial_authority_raw_text,
        admission_reason=admission_reason,
        admission_reason_raw_text=admission_reason_raw_text,
        release_reason=release_reason,
        release_reason_raw_text=release_reason_raw_text,
        person=person,
        incarceration_type_raw_text=incarceration_type_raw_text,
        specialized_purpose_for_incarceration=specialized_purpose_for_incarceration,
        specialized_purpose_for_incarceration_raw_text=specialized_purpose_for_incarceration_raw_text,
    )

    person.incarceration_periods.append(incarceration_period)


def add_supervision_period_to_person(
    person: entities.StatePerson,
    state_code: str,
    external_id: str,
    supervision_type: StateSupervisionPeriodSupervisionType,
    supervision_type_raw_text: str,
    start_date: datetime.date,
    termination_date: Optional[datetime.date],
    supervision_site: str,
    supervising_officer: Optional[entities.StateAgent],
    admission_reason: Optional[StateSupervisionPeriodAdmissionReason],
    admission_reason_raw_text: str,
    termination_reason: Optional[StateSupervisionPeriodTerminationReason],
    termination_reason_raw_text: Optional[str],
    supervision_level: Optional[StateSupervisionLevel] = None,
    supervision_level_raw_text: Optional[str] = None,
) -> None:
    """Append a supervision period to the person (updates the person entity in place)."""

    supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
        external_id=external_id,
        state_code=state_code,
        supervision_type=supervision_type,
        supervision_type_raw_text=supervision_type_raw_text,
        start_date=start_date,
        termination_date=termination_date,
        county_code=None,
        supervision_site=supervision_site,
        supervising_officer=supervising_officer,
        admission_reason=admission_reason,
        admission_reason_raw_text=admission_reason_raw_text,
        termination_reason=termination_reason,
        termination_reason_raw_text=termination_reason_raw_text,
        person=person,
        supervision_level=supervision_level,
        supervision_level_raw_text=supervision_level_raw_text,
    )

    person.supervision_periods.append(supervision_period)


def add_assessment_to_person(
    person: entities.StatePerson,
    state_code: str,
    external_id: str,
    assessment_class: Optional[StateAssessmentClass],
    assessment_type: Optional[StateAssessmentType],
    assessment_date: datetime.date,
    assessment_score: Optional[int],
    assessment_level: Optional[StateAssessmentLevel],
    assessment_level_raw_text: Optional[str],
    assessment_metadata: Optional[str],
    conducting_agent: Optional[entities.StateAgent],
    assessment_class_raw_text: Optional[str] = None,
    assessment_type_raw_text: Optional[str] = None,
) -> None:
    """Append an assessment to the person (updates the person entity in place)."""

    assessment = entities.StateAssessment.new_with_defaults(
        external_id=external_id,
        state_code=state_code,
        assessment_class=assessment_class,
        assessment_class_raw_text=assessment_class_raw_text,
        assessment_type=assessment_type,
        assessment_type_raw_text=assessment_type_raw_text,
        assessment_date=assessment_date,
        assessment_score=assessment_score,
        assessment_level=assessment_level,
        assessment_level_raw_text=assessment_level_raw_text,
        assessment_metadata=assessment_metadata,
        conducting_agent=conducting_agent,
        person=person,
    )

    person.assessments.append(assessment)


def add_supervision_sentence_to_person(
    person: entities.StatePerson,
    state_code: str,
    external_id: str,
    county_code: Optional[str],
    status: StateSentenceStatus,
    status_raw_text: Optional[str],
    supervision_type: StateSupervisionSentenceSupervisionType,
    supervision_type_raw_text: Optional[str],
    date_imposed: Optional[datetime.date] = None,
    start_date: Optional[datetime.date] = None,
    projected_completion_date: Optional[datetime.date] = None,
    completion_date: Optional[datetime.date] = None,
    min_length_days: Optional[int] = None,
    max_length_days: Optional[int] = None,
    sentence_metadata: Optional[str] = None,
    conditions: Optional[str] = None,
) -> entities.StateSupervisionSentence:
    """Append a supervision sentence to the person (updates the person entity in place)."""

    supervision_sentence = entities.StateSupervisionSentence.new_with_defaults(
        external_id=external_id,
        state_code=state_code,
        status=status,
        status_raw_text=status_raw_text,
        supervision_type=supervision_type,
        supervision_type_raw_text=supervision_type_raw_text,
        date_imposed=date_imposed,
        start_date=start_date,
        projected_completion_date=projected_completion_date,
        completion_date=completion_date,
        county_code=county_code,
        min_length_days=min_length_days,
        max_length_days=max_length_days,
        sentence_metadata=sentence_metadata,
        conditions=conditions,
        person=person,
    )

    person.supervision_sentences.append(supervision_sentence)
    return supervision_sentence


def add_incarceration_sentence_to_person(
    person: entities.StatePerson,
    state_code: str,
    external_id: str,
    status: StateSentenceStatus,
    status_raw_text: Optional[str],
    incarceration_type: StateIncarcerationType,
    incarceration_type_raw_text: Optional[str],
    county_code: Optional[str],
    date_imposed: Optional[datetime.date] = None,
    start_date: Optional[datetime.date] = None,
    projected_min_release_date: Optional[datetime.date] = None,
    projected_max_release_date: Optional[datetime.date] = None,
    completion_date: Optional[datetime.date] = None,
    earned_time_days: Optional[int] = None,
    initial_time_served_days: Optional[int] = None,
    min_length_days: Optional[int] = None,
    max_length_days: Optional[int] = None,
    is_life: Optional[bool] = False,
    is_capital_punishment: Optional[bool] = False,
    sentence_metadata: Optional[str] = None,
    conditions: Optional[str] = None,
) -> entities.StateIncarcerationSentence:
    """Append an incarceration sentence to the person (updates the person entity in place)."""

    incarceration_sentence = entities.StateIncarcerationSentence.new_with_defaults(
        external_id=external_id,
        state_code=state_code,
        status=status,
        status_raw_text=status_raw_text,
        incarceration_type=incarceration_type,
        incarceration_type_raw_text=incarceration_type_raw_text,
        date_imposed=date_imposed,
        start_date=start_date,
        projected_min_release_date=projected_min_release_date,
        projected_max_release_date=projected_max_release_date,
        completion_date=completion_date,
        county_code=county_code,
        min_length_days=min_length_days,
        max_length_days=max_length_days,
        is_life=is_life,
        is_capital_punishment=is_capital_punishment,
        earned_time_days=earned_time_days,
        initial_time_served_days=initial_time_served_days,
        sentence_metadata=sentence_metadata,
        conditions=conditions,
        person=person,
    )

    person.incarceration_sentences.append(incarceration_sentence)
    return incarceration_sentence


def add_supervision_contact_to_person(
    person: entities.StatePerson,
    state_code: str,
    external_id: str,
    contact_date: Optional[datetime.date] = None,
    contact_reason: Optional[StateSupervisionContactReason] = None,
    contact_reason_raw_text: Optional[str] = None,
    contact_type: Optional[StateSupervisionContactType] = None,
    contact_type_raw_text: Optional[str] = None,
    contact_method: Optional[StateSupervisionContactMethod] = None,
    contact_method_raw_text: Optional[str] = None,
    location: Optional[StateSupervisionContactLocation] = None,
    location_raw_text: Optional[str] = None,
    resulted_in_arrest: Optional[bool] = None,
    status: Optional[StateSupervisionContactStatus] = None,
    status_raw_text: Optional[str] = None,
    verified_employment: Optional[bool] = None,
    supervision_contact_id: Optional[int] = None,
    contacted_agent: Optional[entities.StateAgent] = None,
) -> entities.StateSupervisionContact:
    """Append a supervision contact to the person (updates the person entity in place)."""

    supervision_contact = entities.StateSupervisionContact.new_with_defaults(
        external_id=external_id,
        state_code=state_code,
        contact_date=contact_date,
        contact_reason=contact_reason,
        contact_reason_raw_text=contact_reason_raw_text,
        contact_type=contact_type,
        contact_type_raw_text=contact_type_raw_text,
        contact_method=contact_method,
        contact_method_raw_text=contact_method_raw_text,
        location=location,
        location_raw_text=location_raw_text,
        resulted_in_arrest=resulted_in_arrest,
        status=status,
        status_raw_text=status_raw_text,
        verified_employment=verified_employment,
        supervision_contact_id=supervision_contact_id,
        contacted_agent=contacted_agent,
        person=person,
    )

    person.supervision_contacts.append(supervision_contact)
    return supervision_contact
