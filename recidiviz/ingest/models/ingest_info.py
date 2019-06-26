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

"""Represents data scraped for a single individual."""
from abc import abstractmethod
from typing import List, Optional


PLURALS = {'person': 'people', 'booking': 'bookings', 'charge': 'charges',
           'hold': 'holds',
           'state_person': 'state_people',
           'state_person_race': 'state_person_races',
           'state_person_ethnicity': 'state_person_ethnicities',
           'state_alias': 'state_aliases',
           'state_assessment': 'state_assessments',
           'state_person_external_id': 'state_person_external_ids',
           'state_sentence_group': 'state_sentence_groups',
           'state_supervision_sentence': 'state_supervision_sentences',
           'state_incarceration_sentence': 'state_incarceration_sentences',
           'state_fine': 'state_fines',
           'state_supervision_period': 'state_supervision_periods',
           'state_incarceration_period': 'state_incarceration_periods',
           'state_charge': 'state_charges',
           'state_incarceration_incident': 'state_incarceration_incidents',
           'state_parole_decision': 'state_parole_decisions',
           'state_supervision_violation': 'state_supervision_violations',
           'state_supervision_violation_response':
               'state_supervision_violation_responses'
           }


class IngestObject:
    """Abstract base class for all the objects contained by IngestInfo"""

    def __eq__(self, other):
        return eq(self, other)

    def __lt__(self, other):
        return str(self) < str(other)

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    @abstractmethod
    def __setattr__(self, key, value):
        """Implement using restricted_setattr"""


class IngestInfo(IngestObject):
    """Class for information about multiple people."""

    def __init__(self, people=None, state_people=None):
        self.people: List[Person] = people or []
        self.state_people: List[StatePerson] = state_people or []

    def __eq__(self, other):
        return eq(self, other, exclude=['_state_people_by_id'])

    def __bool__(self):
        return to_bool(self, exclude=['_state_people_by_id'])

    def __str__(self):
        return to_string(self, exclude=['_state_people_by_id'])

    def __repr__(self):
        return to_repr(self, exclude=['_state_people_by_id'])

    def __setattr__(self, name, value):
        restricted_setattr(self, '_state_people_by_id', name, value)

    def create_person(self, **kwargs) -> 'Person':
        person = Person(**kwargs)
        self.people.append(person)
        return person

    def get_recent_person(self) -> Optional['Person']:
        if self.people:
            return self.people[-1]
        return None

    def create_state_person(self, **kwargs) -> 'StatePerson':
        person = StatePerson(**kwargs)
        self.state_people.append(person)
        return person

    def get_recent_state_person(self) -> Optional['StatePerson']:
        if self.state_people:
            return self.state_people[-1]
        return None

    def get_state_person_by_id(self, state_person_id) \
            -> Optional['StatePerson']:
        return next((sp for sp in self.state_people
                     if sp.state_person_id == state_person_id), None)

    def prune(self) -> 'IngestInfo':
        self.people = [person.prune() for person in self.people if person]
        self.state_people = [person.prune() for person
                             in self.state_people if person]
        return self

    def sort(self):
        for person in self.people:
            person.sort()
        self.people.sort()

        for person in self.state_people:
            person.sort()
        self.state_people.sort()

    def get_all_people(self, predicate=lambda _: True) -> List['Person']:
        return [person for person in self.people if predicate(person)]

    def get_all_bookings(self, predicate=lambda _: True) -> List['Booking']:
        return [booking for person in self.get_all_people()
                for booking in person.bookings if predicate(booking)]

    def get_all_arrests(self, predicate=lambda _: True) -> List['Arrest']:
        return [booking.arrest for booking in self.get_all_bookings()
                if booking.arrest is not None and predicate(booking.arrest)]

    def get_all_charges(self, predicate=lambda _: True) -> List['Charge']:
        return [charge for booking in self.get_all_bookings()
                for charge in booking.charges if predicate(charge)]

    def get_all_holds(self, predicate=lambda _: True) -> List['Hold']:
        return [hold for booking in self.get_all_bookings()
                for hold in booking.holds if predicate(hold)]

    def get_all_bonds(self, predicate=lambda _: True) -> List['Bond']:
        return [charge.bond for charge in self.get_all_charges()
                if charge.bond is not None and predicate(charge.bond)]

    def get_all_sentences(self, predicate=lambda _: True) -> List['Sentence']:
        return [charge.sentence for charge in self.get_all_charges()
                if charge.sentence is not None and predicate(charge.sentence)]

    def get_all_state_people(self, predicate=lambda _: True) \
            -> List['StatePerson']:
        return [person for person in self.state_people if predicate(person)]


class Person(IngestObject):
    """Class for information about a person.
    Referenced from IngestInfo.
    """

    def __init__(
            self, person_id=None, full_name=None, surname=None,
            given_names=None, middle_names=None, name_suffix=None,
            birthdate=None, gender=None, age=None, race=None, ethnicity=None,
            place_of_residence=None, jurisdiction_id=None, bookings=None):
        self.person_id: Optional[str] = person_id
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix
        self.full_name: Optional[str] = full_name
        self.birthdate: Optional[str] = birthdate
        self.gender: Optional[str] = gender
        self.age: Optional[str] = age
        self.race: Optional[str] = race
        self.ethnicity: Optional[str] = ethnicity
        self.place_of_residence: Optional[str] = place_of_residence
        self.jurisdiction_id: Optional[str] = jurisdiction_id

        self.bookings: List[Booking] = bookings or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'bookings', name, value)

    def create_booking(self, **kwargs) -> 'Booking':
        booking = Booking(**kwargs)
        self.bookings.append(booking)
        return booking

    def get_recent_booking(self) -> Optional['Booking']:
        if self.bookings:
            return self.bookings[-1]
        return None

    def prune(self) -> 'Person':
        self.bookings = [booking.prune() for booking
                         in self.bookings if booking]
        return self

    def sort(self):
        for booking in self.bookings:
            booking.sort()
        self.bookings.sort()


class Booking(IngestObject):
    """Class for information about a booking.
    Referenced from Person.
    """

    def __init__(
            self, booking_id=None, admission_date=None,
            admission_reason=None, projected_release_date=None,
            release_date=None, release_reason=None, custody_status=None,
            facility=None, classification=None, total_bond_amount=None,
            arrest=None, charges=None, holds=None):
        self.booking_id: Optional[str] = booking_id
        self.admission_date: Optional[str] = admission_date
        self.admission_reason: Optional[str] = admission_reason
        self.projected_release_date: Optional[str] = projected_release_date
        self.release_date: Optional[str] = release_date
        self.release_reason: Optional[str] = release_reason
        self.custody_status: Optional[str] = custody_status
        self.facility: Optional[str] = facility
        self.classification: Optional[str] = classification
        self.total_bond_amount: Optional[str] = total_bond_amount

        self.arrest: Optional[Arrest] = arrest
        self.charges: List[Charge] = charges or []
        self.holds: List[Hold] = holds or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'holds', name, value)

    def create_arrest(self, **kwargs) -> 'Arrest':
        self.arrest = Arrest(**kwargs)
        return self.arrest

    def create_charge(self, **kwargs) -> 'Charge':
        charge = Charge(**kwargs)
        self.charges.append(charge)
        return charge

    def create_hold(self, **kwargs) -> 'Hold':
        hold = Hold(**kwargs)
        self.holds.append(hold)
        return hold

    def get_recent_charge(self) -> Optional['Charge']:
        if self.charges:
            return self.charges[-1]
        return None

    def get_recent_hold(self) -> Optional['Hold']:
        if self.holds:
            return self.holds[-1]
        return None

    def get_recent_arrest(self) -> Optional['Arrest']:
        return self.arrest

    def prune(self) -> 'Booking':
        self.charges = [charge.prune() for charge in self.charges if charge]
        self.holds = [hold for hold in self.holds if hold]
        if not self.arrest:
            self.arrest = None
        return self

    def sort(self):
        self.charges.sort()
        self.holds.sort()


class Arrest(IngestObject):
    """Class for information about an arrest.
    Referenced from Booking.
    """

    def __init__(
            self, arrest_id=None, arrest_date=None, location=None,
            officer_name=None, officer_id=None, agency=None):
        self.arrest_id: Optional[str] = arrest_id
        self.arrest_date: Optional[str] = arrest_date
        self.location: Optional[str] = location
        self.officer_name: Optional[str] = officer_name
        self.officer_id: Optional[str] = officer_id
        self.agency: Optional[str] = agency

    def __setattr__(self, name, value):
        restricted_setattr(self, 'agency', name, value)


class Charge(IngestObject):
    """Class for information about a charge.
    Referenced from Booking.
    """

    def __init__(
            self, charge_id=None, offense_date=None, statute=None,
            name=None, attempted=None, degree=None,
            charge_class=None, level=None, fee_dollars=None,
            charging_entity=None, status=None,
            number_of_counts=None, court_type=None,
            case_number=None, next_court_date=None, judge_name=None,
            bond=None, sentence=None, charge_notes=None):
        self.charge_id: Optional[str] = charge_id
        self.offense_date: Optional[str] = offense_date
        self.statute: Optional[str] = statute
        self.name: Optional[str] = name
        self.attempted: Optional[str] = attempted
        self.degree: Optional[str] = degree
        self.charge_class: Optional[str] = charge_class
        self.level: Optional[str] = level
        self.fee_dollars: Optional[str] = fee_dollars
        self.charging_entity: Optional[str] = charging_entity
        self.status: Optional[str] = status
        self.number_of_counts: Optional[str] = number_of_counts
        self.court_type: Optional[str] = court_type
        self.case_number: Optional[str] = case_number
        self.next_court_date: Optional[str] = next_court_date
        self.judge_name: Optional[str] = judge_name
        self.charge_notes: Optional[str] = charge_notes

        self.bond: Optional[Bond] = bond
        self.sentence: Optional[Sentence] = sentence

    def __setattr__(self, name, value):
        restricted_setattr(self, 'sentence', name, value)

    def create_bond(self, **kwargs) -> 'Bond':
        self.bond = Bond(**kwargs)
        return self.bond

    def create_sentence(self, **kwargs) -> 'Sentence':
        self.sentence = Sentence(**kwargs)
        return self.sentence

    def get_recent_bond(self) -> Optional['Bond']:
        return self.bond

    def get_recent_sentence(self) -> Optional['Sentence']:
        return self.sentence

    def prune(self) -> 'Charge':
        if not self.bond:
            self.bond = None
        if not self.sentence:
            self.sentence = None
        return self


class Hold(IngestObject):
    """Class for information about a hold.
    Referenced from Booking.

    Holds will default to the jurisdiction_name "UNSPECIFIED", since we may
    only know that a booking has a hold. Then booking.create_hold() will create
    a hold that doesn't get pruned later.
    """

    def __init__(self, hold_id=None, jurisdiction_name='UNSPECIFIED',
                 status=None):
        self.hold_id: Optional[str] = hold_id
        self.jurisdiction_name: Optional[str] = jurisdiction_name
        self.status: Optional[str] = status

    def __setattr__(self, name, value):
        restricted_setattr(self, 'status', name, value)


class Bond(IngestObject):
    """Class for information about a bond.
    Referenced from Charge.
    """

    def __init__(
            self, bond_id=None, amount=None, bond_type=None, status=None,
            bond_agent=None):
        self.bond_id: Optional[str] = bond_id
        self.amount: Optional[str] = amount
        self.bond_type: Optional[str] = bond_type
        self.bond_agent: Optional[str] = bond_agent
        self.status: Optional[str] = status

    def __setattr__(self, name, value):
        restricted_setattr(self, 'status', name, value)


class Sentence(IngestObject):
    """Class for information about a sentence.
    Referenced from Charge.
    """

    def __init__(
            self, sentence_id=None, date_imposed=None, status=None,
            sentencing_region=None, min_length=None, max_length=None,
            is_life=None, is_probation=None, is_suspended=None,
            fine_dollars=None, parole_possible=None,
            post_release_supervision_length=None, completion_date=None,
            projected_completion_date=None, sentence_relationships=None):
        self.sentence_id: Optional[str] = sentence_id
        self.date_imposed: Optional[str] = date_imposed
        self.status: Optional[str] = status
        self.sentencing_region: Optional[str] = sentencing_region
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length
        self.is_life: Optional[str] = is_life
        self.is_probation: Optional[str] = is_probation
        self.is_suspended: Optional[str] = is_suspended
        self.fine_dollars: Optional[str] = fine_dollars
        self.parole_possible: Optional[str] = parole_possible
        self.completion_date: Optional[str] = completion_date
        self.projected_completion_date: Optional[str] = \
            projected_completion_date

        self.post_release_supervision_length: Optional[str] = \
            post_release_supervision_length

        self.sentence_relationships: List[SentenceRelationship] = \
            sentence_relationships or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'sentence_relationships', name, value)

    def create_sentence_relationship(
            self, other_sentence, relationship_type) -> 'SentenceRelationship':
        relationship = SentenceRelationship(
            sentence_a=self, sentence_b=other_sentence,
            relationship_type=relationship_type)
        self.sentence_relationships.append(relationship)
        return relationship


# TODO(1145): add logic to convert to SentenceRelationship proto (and also add
# normalization logic to remove duplicates where A->B and B->A relationships
# are both provided)
class SentenceRelationship(IngestObject):
    """Class for information about the relationship between two sentences.
    Referenced from Sentence.
    """

    def __init__(
            self, sentence_relationship_id=None, sentence_a=None,
            sentence_b=None, relationship_type=None):
        self.sentence_relationship_id: Optional[str] = sentence_relationship_id
        self.sentence_a: Optional[Sentence] = sentence_a
        self.sentence_b: Optional[Sentence] = sentence_b
        self.relationship_type: Optional[str] = relationship_type

    def __setattr__(self, name, value):
        restricted_setattr(self, 'relationship_type', name, value)


class StatePerson(IngestObject):
    """Class for information about a person at the state level.
    Referenced from IngestInfo.
    """

    def __init__(
            self, state_person_id=None, full_name=None, surname=None,
            given_names=None, middle_names=None, name_suffix=None,
            birthdate=None, gender=None, age=None, current_address=None,
            residency_status=None, state_person_races=None,
            state_person_ethnicities=None, state_aliases=None,
            state_person_external_ids=None, state_assessments=None,
            state_sentence_groups=None):
        self.state_person_id: Optional[str] = state_person_id
        self.full_name: Optional[str] = full_name
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix
        self.birthdate: Optional[str] = birthdate
        self.gender: Optional[str] = gender
        self.age: Optional[str] = age
        self.current_address: Optional[str] = current_address
        self.residency_status: Optional[str] = residency_status

        self.state_person_races: List[StatePersonRace] = \
            state_person_races or []
        self.state_person_ethnicities: List[StatePersonEthnicity] = \
            state_person_ethnicities or []
        self.state_aliases: List[StateAlias] = state_aliases or []
        self.state_person_external_ids: List[StatePersonExternalId] = \
            state_person_external_ids or []
        self.state_assessments: List[StateAssessment] = state_assessments or []
        self.state_sentence_groups: List[StateSentenceGroup] = \
            state_sentence_groups or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_sentence_groups', name, value)

    def create_state_person_race(self, **kwargs) -> 'StatePersonRace':
        race = StatePersonRace(**kwargs)
        self.state_person_races.append(race)
        return race

    def create_state_person_ethnicity(self, **kwargs) -> 'StatePersonEthnicity':
        ethnicity = StatePersonEthnicity(**kwargs)
        self.state_person_ethnicities.append(ethnicity)
        return ethnicity

    def create_state_alias(self, **kwargs) -> 'StateAlias':
        alias = StateAlias(**kwargs)
        self.state_aliases.append(alias)
        return alias

    def create_state_person_external_id(self, **kwargs) \
            -> 'StatePersonExternalId':
        external_id = StatePersonExternalId(**kwargs)
        self.state_person_external_ids.append(external_id)
        return external_id

    def create_state_assessment(self, **kwargs) -> 'StateAssessment':
        assessment = StateAssessment(**kwargs)
        self.state_assessments.append(assessment)
        return assessment

    def create_state_sentence_group(self, **kwargs) -> 'StateSentenceGroup':
        sentence_group = StateSentenceGroup(**kwargs)
        self.state_sentence_groups.append(sentence_group)
        return sentence_group

    def get_state_person_race_by_id(self, state_person_race_id) \
            -> Optional['StatePersonRace']:
        return next((spr for spr in self.state_person_races
                     if spr.state_person_race_id == state_person_race_id), None)

    def get_recent_state_person_race(self) -> Optional['StatePersonRace']:
        if self.state_person_races:
            return self.state_person_races[-1]
        return None

    def get_recent_state_person_ethnicity(self) \
            -> Optional['StatePersonEthnicity']:
        if self.state_person_ethnicities:
            return self.state_person_ethnicities[-1]
        return None

    def get_state_alias_by_id(self, state_alias_id) \
            -> Optional['StateAlias']:
        return next((sa for sa in self.state_aliases
                     if sa.state_alias_id == state_alias_id), None)

    def get_recent_state_alias(self) -> Optional['StateAlias']:
        if self.state_aliases:
            return self.state_aliases[-1]
        return None

    def get_state_person_external_id_by_id(self, state_person_external_id_id) \
            -> Optional['StatePersonExternalId']:
        return next((eid for eid in self.state_person_external_ids
                     if eid.state_person_external_id_id ==
                     state_person_external_id_id), None)

    def get_recent_state_person_external_id(self) \
            -> Optional['StatePersonExternalId']:
        if self.state_person_external_ids:
            return self.state_person_external_ids[-1]
        return None

    def get_state_assessment_by_id(self, state_assessment_id) \
            -> Optional['StateAssessment']:
        return next((sa for sa in self.state_assessments
                     if sa.state_assessment_id == state_assessment_id), None)

    def get_recent_state_assessment(self) -> Optional['StateAssessment']:
        if self.state_assessments:
            return self.state_assessments[-1]
        return None

    def get_recent_state_sentence_group(self) -> Optional['StateSentenceGroup']:
        if self.state_sentence_groups:
            return self.state_sentence_groups[-1]
        return None

    def get_state_sentence_group_by_id(self, sentence_group_id) \
            -> Optional['StateSentenceGroup']:
        return next((sg for sg in self.state_sentence_groups
                     if sg.state_sentence_group_id == sentence_group_id), None)

    def prune(self) -> 'StatePerson':
        self.state_sentence_groups = [sg.prune() for sg
                                      in self.state_sentence_groups if sg]
        return self

    def sort(self):
        self.state_person_races.sort()
        self.state_person_ethnicities.sort()
        self.state_aliases.sort()
        self.state_person_external_ids.sort()
        self.state_assessments.sort()

        for sentence_group in self.state_sentence_groups:
            sentence_group.sort()
        self.state_sentence_groups.sort()


class StatePersonExternalId(IngestObject):
    """Class for information about one of a StatePerson's external ids.
    Referenced from StatePerson.
    """

    def __init__(self, state_person_external_id_id=None,
                 id_type=None, state_code=None):
        self.state_person_external_id_id: Optional[str] = \
            state_person_external_id_id
        self.id_type: Optional[str] = id_type
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_code', name, value)


class StateAlias(IngestObject):
    """Class for information about an alias.
    Referenced from StatePerson.
    """

    def __init__(self, state_alias_id=None, state_code=None, full_name=None,
                 surname=None, given_names=None, middle_names=None,
                 name_suffix=None):
        self.state_alias_id: Optional[str] = state_alias_id
        self.state_code: Optional[str] = state_code
        self.full_name: Optional[str] = full_name
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix

    def __setattr__(self, name, value):
        restricted_setattr(self, 'name_suffix', name, value)


class StatePersonRace(IngestObject):
    """Class for information about a state person's race.
    Referenced from StatePerson.
    """

    def __init__(self, state_person_race_id=None, race=None, state_code=None):
        self.state_person_race_id: Optional[str] = state_person_race_id
        self.race: Optional[str] = race
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_code', name, value)


class StatePersonEthnicity(IngestObject):
    """Class for information about a state person's ethnicity.
    Referenced from StatePerson.
    """

    def __init__(self, state_person_ethnicity_id=None, ethnicity=None,
                 state_code=None):
        self.state_person_ethnicity_id: Optional[str] = \
            state_person_ethnicity_id
        self.ethnicity: Optional[str] = ethnicity
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_code', name, value)


class StateAssessment(IngestObject):
    """Class for information about an assessment.
    Referenced from StatePerson.
    """

    def __init__(self, state_assessment_id=None, assessment_class=None,
                 assessment_type=None, assessment_date=None, state_code=None,
                 assessment_score=None, assessment_level=None,
                 assessment_metadata=None, conducting_agent=None):
        self.state_assessment_id: Optional[str] = state_assessment_id
        self.assessment_class: Optional[str] = assessment_class
        self.assessment_type: Optional[str] = assessment_type
        self.assessment_date: Optional[str] = assessment_date
        self.state_code: Optional[str] = state_code
        self.assessment_score: Optional[str] = assessment_score
        self.assessment_level: Optional[str] = assessment_level
        self.assessment_metadata: Optional[str] = assessment_metadata

        self.conducting_agent: Optional[StateAgent] = conducting_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, 'conducting_agent', name, value)

    def create_state_agent(self, **kwargs) -> 'StateAgent':
        self.conducting_agent = StateAgent(**kwargs)
        return self.conducting_agent

    def get_recent_state_agent(self) -> Optional['StateAgent']:
        return self.conducting_agent

    def prune(self) -> 'StateAssessment':
        if not self.conducting_agent:
            self.conducting_agent = None
        return self


class StateSentenceGroup(IngestObject):
    """Class for information about a group of related sentences.
    Referenced from StatePerson.
    """

    def __init__(self, state_sentence_group_id=None, status=None,
                 date_imposed=None, state_code=None, county_code=None,
                 min_length=None, max_length=None,
                 state_supervision_sentences=None,
                 state_incarceration_sentences=None, state_fines=None):
        self.state_sentence_group_id: Optional[str] = state_sentence_group_id
        self.status: Optional[str] = status
        self.date_imposed: Optional[str] = date_imposed
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length

        self.state_supervision_sentences: List[StateSupervisionSentence] = \
            state_supervision_sentences or []
        self.state_incarceration_sentences: List[StateIncarcerationSentence] = \
            state_incarceration_sentences or []
        self.state_fines: List[StateFine] = state_fines or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_fines', name, value)

    def create_state_supervision_sentence(self, **kwargs) \
            -> 'StateSupervisionSentence':
        supervision_sentence = StateSupervisionSentence(**kwargs)
        self.state_supervision_sentences.append(supervision_sentence)
        return supervision_sentence

    def create_state_incarceration_sentence(self, **kwargs) \
            -> 'StateIncarcerationSentence':
        incarceration_sentence = StateIncarcerationSentence(**kwargs)
        self.state_incarceration_sentences.append(incarceration_sentence)
        return incarceration_sentence

    def create_state_fine(self, **kwargs) -> 'StateFine':
        fine = StateFine(**kwargs)
        self.state_fines.append(fine)
        return fine

    def get_recent_state_supervision_sentence(self) \
            -> Optional['StateSupervisionSentence']:
        if self.state_supervision_sentences:
            return self.state_supervision_sentences[-1]
        return None

    def get_state_supervision_sentence_by_id(self,
                                             supervision_sentence_id) \
            -> Optional['StateSupervisionSentence']:
        return next((ss for ss in self.state_supervision_sentences
                     if ss.state_supervision_sentence_id
                     == supervision_sentence_id), None)

    def get_recent_state_incarceration_sentence(self)\
            -> Optional['StateIncarcerationSentence']:
        if self.state_incarceration_sentences:
            return self.state_incarceration_sentences[-1]
        return None

    def get_state_incarceration_sentence_by_id(self,
                                               incarceration_sentence_id) \
            -> Optional['StateIncarcerationSentence']:
        return next((ins for ins in self.state_incarceration_sentences
                     if ins.state_incarceration_sentence_id
                     == incarceration_sentence_id), None)

    def get_recent_state_fine(self) -> Optional['StateFine']:
        if self.state_fines:
            return self.state_fines[-1]
        return None

    def prune(self) -> 'StateSentenceGroup':
        self.state_supervision_sentences = \
            [ss.prune() for ss in self.state_supervision_sentences if ss]

        self.state_incarceration_sentences = \
            [ins.prune() for ins in self.state_incarceration_sentences if ins]

        self.state_fines = [fine.prune() for fine in self.state_fines if fine]

        return self

    def sort(self):
        for supervision_sentence in self.state_supervision_sentences:
            supervision_sentence.sort()
        self.state_supervision_sentences.sort()

        for incarceration_sentence in self.state_incarceration_sentences:
            incarceration_sentence.sort()
        self.state_incarceration_sentences.sort()

        self.state_fines.sort()


class StateSupervisionSentence(IngestObject):
    """Class for information about a sentence to supervision.
    Referenced from SentenceGroup.
    """

    def __init__(self, state_supervision_sentence_id=None, status=None,
                 supervision_type=None, projected_completion_date=None,
                 completion_date=None, state_code=None, county_code=None,
                 min_length=None, max_length=None, state_charges=None,
                 state_incarceration_periods=None,
                 state_supervision_periods=None):
        self.state_supervision_sentence_id: Optional[str] = \
            state_supervision_sentence_id
        self.status: Optional[str] = status
        self.supervision_type: Optional[str] = supervision_type
        self.projected_completion_date: Optional[str] = \
            projected_completion_date
        self.completion_date: Optional[str] = completion_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length

        self.state_charges: List[StateCharge] = state_charges or []
        self.state_incarceration_periods: List[StateIncarcerationPeriod] = \
            state_incarceration_periods or []
        self.state_supervision_periods: List[StateSupervisionPeriod] = \
            state_supervision_periods or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_supervision_periods', name, value)

    def create_state_charge(self, **kwargs) -> 'StateCharge':
        charge = StateCharge(**kwargs)
        self.state_charges.append(charge)
        return charge

    def create_state_incarceration_period(self, **kwargs) \
            -> 'StateIncarcerationPeriod':
        incarceration_period = StateIncarcerationPeriod(**kwargs)
        self.state_incarceration_periods.append(incarceration_period)
        return incarceration_period

    def create_state_supervision_period(self, **kwargs) \
            -> 'StateSupervisionPeriod':
        supervision_period = StateSupervisionPeriod(**kwargs)
        self.state_supervision_periods.append(supervision_period)
        return supervision_period

    def get_state_charge_by_id(self, state_charge_id) \
            -> Optional['StateCharge']:
        return next((sc for sc in self.state_charges
                     if sc.state_charge_id == state_charge_id), None)

    def get_recent_state_charge(self) -> Optional['StateCharge']:
        if self.state_charges:
            return self.state_charges[-1]
        return None

    def get_recent_state_incarceration_period(self)\
            -> Optional['StateIncarcerationPeriod']:
        if self.state_incarceration_periods:
            return self.state_incarceration_periods[-1]
        return None

    def get_state_supervision_period_by_id(self, state_supervision_period_id) \
            -> Optional['StateSupervisionPeriod']:
        return next((sc for sc in self.state_supervision_periods
                     if sc.state_supervision_period_id ==
                     state_supervision_period_id), None)

    def get_recent_state_supervision_period(self) \
            -> Optional['StateSupervisionPeriod']:
        if self.state_supervision_periods:
            return self.state_supervision_periods[-1]
        return None

    def prune(self) -> 'StateSupervisionSentence':
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]

        self.state_incarceration_periods = \
            [ip.prune() for ip in self.state_incarceration_periods if ip]

        self.state_supervision_periods = \
            [sp.prune() for sp in self.state_supervision_periods if sp]

        return self

    def sort(self):
        self.state_charges.sort()

        for incarceration_period in self.state_incarceration_periods:
            incarceration_period.sort()
        self.state_incarceration_periods.sort()

        for supervision_period in self.state_supervision_periods:
            supervision_period.sort()
        self.state_supervision_periods.sort()


class StateIncarcerationSentence(IngestObject):
    """Class for information about a sentence to incarceration.
    Referenced from SentenceGroup.
    """

    def __init__(self, state_incarceration_sentence_id=None, status=None,
                 incarceration_type=None, date_imposed=None,
                 projected_min_release_date=None,
                 projected_max_release_date=None, parole_eligibility_date=None,
                 state_code=None, county_code=None, min_length=None,
                 max_length=None, is_life=None, parole_possible=None,
                 initial_time_served=None, good_time=None, earned_time=None,
                 state_charges=None, state_incarceration_periods=None,
                 state_supervision_periods=None):
        self.state_incarceration_sentence_id: Optional[str] = \
            state_incarceration_sentence_id
        self.status: Optional[str] = status
        self.incarceration_type: Optional[str] = incarceration_type
        self.date_imposed: Optional[str] = date_imposed
        self.projected_min_release_date: Optional[str] = \
            projected_min_release_date
        self.projected_max_release_date: Optional[str] = \
            projected_max_release_date
        self.parole_eligibility_date: Optional[str] = parole_eligibility_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length
        self.is_life: Optional[str] = is_life
        self.parole_possible: Optional[str] = parole_possible
        self.initial_time_served: Optional[str] = initial_time_served
        self.good_time: Optional[str] = good_time
        self.earned_time: Optional[str] = earned_time

        self.state_charges: List[StateCharge] = state_charges or []
        self.state_incarceration_periods: List[StateIncarcerationPeriod] = \
            state_incarceration_periods or []
        self.state_supervision_periods: List[StateSupervisionPeriod] = \
            state_supervision_periods or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_supervision_periods', name, value)

    def create_state_charge(self, **kwargs) -> 'StateCharge':
        state_charge = StateCharge(**kwargs)
        self.state_charges.append(state_charge)
        return state_charge

    def create_state_incarceration_period(self, **kwargs) \
            -> 'StateIncarcerationPeriod':
        incarceration_period = StateIncarcerationPeriod(**kwargs)
        self.state_incarceration_periods.append(incarceration_period)
        return incarceration_period

    def create_state_supervision_period(self, **kwargs) \
            -> 'StateSupervisionPeriod':
        supervision_period = StateSupervisionPeriod(**kwargs)
        self.state_supervision_periods.append(supervision_period)
        return supervision_period

    def get_recent_state_charge(self) -> Optional['StateCharge']:
        if self.state_charges:
            return self.state_charges[-1]
        return None

    def get_state_charge_by_id(self, state_charge_id) \
            -> Optional['StateCharge']:
        return next((sc for sc in self.state_charges
                     if sc.state_charge_id == state_charge_id), None)

    def get_state_incarceration_period_by_id(self,
                                             incarceration_period_id) \
            -> Optional['StateIncarcerationPeriod']:
        return next((ip for ip in self.state_incarceration_periods
                     if ip.state_incarceration_period_id
                     == incarceration_period_id), None)

    def get_recent_state_incarceration_period(self)\
            -> Optional['StateIncarcerationPeriod']:
        if self.state_incarceration_periods:
            return self.state_incarceration_periods[-1]
        return None

    def get_recent_state_supervision_period(self) \
            -> Optional['StateSupervisionPeriod']:
        if self.state_supervision_periods:
            return self.state_supervision_periods[-1]
        return None

    def prune(self) -> 'StateIncarcerationSentence':
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]

        self.state_incarceration_periods = \
            [ip.prune() for ip in self.state_incarceration_periods if ip]

        self.state_supervision_periods = \
            [sp.prune() for sp in self.state_supervision_periods if sp]

        return self

    def sort(self):
        self.state_charges.sort()

        for incarceration_period in self.state_incarceration_periods:
            incarceration_period.sort()
        self.state_incarceration_periods.sort()

        for supervision_period in self.state_supervision_periods:
            supervision_period.sort()
        self.state_supervision_periods.sort()


class StateFine(IngestObject):
    """Class for information about a fine associated with a sentence.
    Referenced from SentenceGroup.
    """

    def __init__(self, state_fine_id=None, status=None, date_paid=None,
                 state_code=None, county_code=None, fine_dollars=None,
                 state_charges=None):
        self.state_fine_id: Optional[str] = state_fine_id
        self.status: Optional[str] = status
        self.date_paid: Optional[str] = date_paid
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.fine_dollars: Optional[str] = fine_dollars

        self.state_charges: List[StateCharge] = state_charges or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_charges', name, value)

    def create_state_charge(self, **kwargs) -> 'StateCharge':
        charge = StateCharge(**kwargs)
        self.state_charges.append(charge)
        return charge

    def get_recent_state_charge(self) -> Optional['StateCharge']:
        if self.state_charges:
            return self.state_charges[-1]
        return None

    def prune(self) -> 'StateFine':
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]
        return self

    def sort(self):
        self.state_charges.sort()


class StateCharge(IngestObject):
    """Class for information about a charge.
    Referenced from IncarcerationSentence, SupervisionSentence, and Fine.
    """

    def __init__(self, state_charge_id=None, status=None, offense_date=None,
                 date_charged=None, state_code=None, county_code=None,
                 statute=None, description=None, attempted=None,
                 charge_classification=None, degree=None, counts=None,
                 charge_notes=None, charging_entity=None, state_court_case=None,
                 state_bond=None):
        self.state_charge_id: Optional[str] = state_charge_id
        self.status: Optional[str] = status
        self.offense_date: Optional[str] = offense_date
        self.date_charged: Optional[str] = date_charged
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.statute: Optional[str] = statute
        self.description: Optional[str] = description
        self.attempted: Optional[str] = attempted
        self.charge_classification: Optional[str] = charge_classification
        self.degree: Optional[str] = degree
        self.counts: Optional[str] = counts
        self.charge_notes: Optional[str] = charge_notes
        self.charging_entity: Optional[str] = charging_entity

        self.state_court_case: Optional[StateCourtCase] = state_court_case
        self.state_bond: Optional[StateBond] = state_bond

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_bond', name, value)

    def create_state_court_case(self, **kwargs) -> 'StateCourtCase':
        court_case = StateCourtCase(**kwargs)
        self.state_court_case = court_case
        return self.state_court_case

    def create_state_bond(self, **kwargs) -> 'StateBond':
        self.state_bond = StateBond(**kwargs)
        return self.state_bond

    def get_recent_state_court_case(self) -> Optional['StateCourtCase']:
        return self.state_court_case

    def get_state_court_case_by_id(self, court_case_id) \
            -> Optional['StateCourtCase']:
        if self.state_court_case and \
                self.state_court_case.state_court_case_id == court_case_id:
            return self.state_court_case
        return None

    def get_recent_state_bond(self) -> Optional['StateBond']:
        return self.state_bond

    def prune(self) -> 'StateCharge':
        if not self.state_court_case:
            self.state_court_case = None
        if not self.state_bond:
            self.state_bond = None
        return self


class StateCourtCase(IngestObject):
    """Class for information about a court case.
    Referenced from StateCharge.
    """

    def __init__(self, state_court_case_id=None, status=None, court_type=None,
                 date_convicted=None, next_court_date=None, state_code=None,
                 county_code=None, court_fee_dollars=None, judge=None):
        self.state_court_case_id: Optional[str] = state_court_case_id
        self.status: Optional[str] = status
        self.court_type: Optional[str] = court_type
        self.date_convicted: Optional[str] = date_convicted
        self.next_court_date: Optional[str] = next_court_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.court_fee_dollars: Optional[str] = court_fee_dollars

        self.judge: Optional[StateAgent] = judge

    def create_state_agent(self, **kwargs) -> 'StateAgent':
        self.judge = StateAgent(**kwargs)
        return self.judge

    def get_state_agent_by_id(self, state_agent_id) -> Optional['StateAgent']:
        if self.judge and self.judge.state_agent_id == state_agent_id:
            return self.judge
        return None

    def get_recent_state_agent(self) -> Optional['StateAgent']:
        return self.judge

    def __setattr__(self, name, value):
        restricted_setattr(self, 'judge', name, value)


class StateBond(IngestObject):
    """Class for information about a bond.
    Referenced from StateCharge.
    """

    def __init__(self, state_bond_id=None, status=None, bond_type=None,
                 date_paid=None, state_code=None, county_code=None,
                 amount=None, bond_agent=None):
        self.state_bond_id: Optional[str] = state_bond_id
        self.status: Optional[str] = status
        self.bond_type: Optional[str] = bond_type
        self.date_paid: Optional[str] = date_paid
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.amount: Optional[str] = amount
        self.bond_agent: Optional[str] = bond_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, 'bond_agent', name, value)


class StateIncarcerationPeriod(IngestObject):
    """Class for information about a period of incarceration.
    Referenced from IncarcerationSentence and SupervisionSentence.
    """

    def __init__(self, state_incarceration_period_id=None, status=None,
                 incarceration_type=None, admission_date=None,
                 release_date=None, state_code=None, county_code=None,
                 facility=None, housing_unit=None, facility_security_level=None,
                 admission_reason=None, projected_release_reason=None,
                 release_reason=None, state_incarceration_incidents=None,
                 state_parole_decisions=None, state_assessments=None):
        self.state_incarceration_period_id: Optional[str] = \
            state_incarceration_period_id
        self.status: Optional[str] = status
        self.incarceration_type: Optional[str] = incarceration_type
        self.admission_date: Optional[str] = admission_date
        self.release_date: Optional[str] = release_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.facility: Optional[str] = facility
        self.housing_unit: Optional[str] = housing_unit
        self.facility_security_level: Optional[str] = facility_security_level
        self.admission_reason: Optional[str] = admission_reason
        self.projected_release_reason: Optional[str] = projected_release_reason
        self.release_reason: Optional[str] = release_reason

        self.state_incarceration_incidents: List[StateIncarcerationIncident] = \
            state_incarceration_incidents or []
        self.state_parole_decisions: List[StateParoleDecision] = \
            state_parole_decisions or []
        self.state_assessments: List[StateAssessment] = state_assessments or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_assessments', name, value)

    def create_state_incarceration_incident(self, **kwargs) \
            -> 'StateIncarcerationIncident':
        incarceration_incident = StateIncarcerationIncident(**kwargs)
        self.state_incarceration_incidents.append(incarceration_incident)
        return incarceration_incident

    def create_state_parole_decision(self, **kwargs) -> 'StateParoleDecision':
        parole_decision = StateParoleDecision(**kwargs)
        self.state_parole_decisions.append(parole_decision)
        return parole_decision

    def create_state_assessment(self, **kwargs) -> 'StateAssessment':
        assessment = StateAssessment(**kwargs)
        self.state_assessments.append(assessment)
        return assessment

    def get_state_incarceration_incident_by_id(
            self, state_incarceration_incident_id) \
            -> Optional['StateIncarcerationIncident']:
        return next((ii for ii in self.state_incarceration_incidents
                     if ii.state_incarceration_incident_id
                     == state_incarceration_incident_id), None)

    def get_recent_state_incarceration_incident(self) \
            -> Optional['StateIncarcerationIncident']:
        if self.state_incarceration_incidents:
            return self.state_incarceration_incidents[-1]
        return None

    def get_recent_state_parole_decision(self) \
            -> Optional['StateParoleDecision']:
        if self.state_parole_decisions:
            return self.state_parole_decisions[-1]
        return None

    def get_recent_state_assessment(self) -> Optional['StateAssessment']:
        if self.state_assessments:
            return self.state_assessments[-1]
        return None

    def prune(self) -> 'StateIncarcerationPeriod':
        self.state_incarceration_incidents = \
            [ii for ii in self.state_incarceration_incidents if ii]

        self.state_parole_decisions = \
            [pd for pd in self.state_parole_decisions if pd]

        self.state_assessments = [a for a in self.state_assessments if a]

        return self

    def sort(self):
        self.state_incarceration_incidents.sort()
        self.state_parole_decisions.sort()
        self.state_assessments.sort()


class StateSupervisionPeriod(IngestObject):
    """Class for information about a period of supervision.
    Referenced from IncarcerationSentence and SupervisionSentence.
    """

    def __init__(self, state_supervision_period_id=None, status=None,
                 supervision_type=None, start_date=None,
                 termination_date=None, state_code=None, county_code=None,
                 admission_reason=None, termination_reason=None,
                 supervision_level=None, conditions=None,
                 state_supervision_violations=None, state_assessments=None):
        self.state_supervision_period_id: Optional[str] = \
            state_supervision_period_id
        self.status: Optional[str] = status
        self.supervision_type: Optional[str] = supervision_type
        self.start_date: Optional[str] = start_date
        self.termination_date: Optional[str] = termination_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.admission_reason: Optional[str] = admission_reason
        self.termination_reason: Optional[str] = termination_reason
        self.supervision_level: Optional[str] = supervision_level
        self.conditions: List[str] = conditions or []

        self.state_supervision_violations: List[StateSupervisionViolation] = \
            state_supervision_violations or []
        self.state_assessments: List[StateAssessment] = state_assessments or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_assessments', name, value)

    def create_state_supervision_violation(self, **kwargs) \
            -> 'StateSupervisionViolation':
        supervision_violation = StateSupervisionViolation(**kwargs)
        self.state_supervision_violations.append(supervision_violation)
        return supervision_violation

    def create_state_assessment(self, **kwargs) -> 'StateAssessment':
        assessment = StateAssessment(**kwargs)
        self.state_assessments.append(assessment)
        return assessment

    def get_state_supervision_violation_by_id(
            self, state_supervision_violation_id
    ) -> Optional['StateSupervisionViolation']:
        return next((sc for sc in self.state_supervision_violations
                     if sc.state_supervision_violation_id ==
                     state_supervision_violation_id), None)

    def get_recent_state_supervision_violation(self)\
            -> Optional['StateSupervisionViolation']:
        if self.state_supervision_violations:
            return self.state_supervision_violations[-1]
        return None

    def get_recent_state_assessment(self) -> Optional['StateAssessment']:
        if self.state_assessments:
            return self.state_assessments[-1]
        return None

    def prune(self) -> 'StateSupervisionPeriod':
        self.state_supervision_violations = \
            [sv for sv in self.state_supervision_violations if sv]
        self.state_assessments = [a for a in self.state_assessments if a]

        return self

    def sort(self):
        self.state_supervision_violations.sort()
        self.state_assessments.sort()


class StateIncarcerationIncident(IngestObject):
    """Class for information about an incident during incarceration.
    Referenced from IncarcerationPeriod.
    """

    def __init__(self, state_incarceration_incident_id=None, incident_type=None,
                 incident_date=None, state_code=None, facility=None,
                 location_within_facility=None, incident_details=None,
                 responding_officer=None,
                 state_incarceration_incident_outcomes=None):
        self.state_incarceration_incident_id: Optional[str] = \
            state_incarceration_incident_id
        self.incident_type: Optional[str] = incident_type
        self.incident_date: Optional[str] = incident_date
        self.state_code: Optional[str] = state_code
        self.facility: Optional[str] = facility
        self.location_within_facility: Optional[str] = location_within_facility
        self.incident_details: Optional[str] = incident_details

        self.responding_officer: Optional[StateAgent] = responding_officer
        self.state_incarceration_incident_outcomes: \
            List[StateIncarcerationIncidentOutcome] = \
            state_incarceration_incident_outcomes or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'incarceration_incident_outcomes', name, value)

    def create_state_agent(self, **kwargs) -> 'StateAgent':
        self.responding_officer = StateAgent(**kwargs)
        return self.responding_officer

    def get_recent_state_agent(self) -> Optional['StateAgent']:
        return self.responding_officer

    def create_state_incarceration_incident_outcome(
            self, **kwargs) -> 'StateIncarcerationIncidentOutcome':
        outcome = StateIncarcerationIncidentOutcome(**kwargs)
        self.state_incarceration_incident_outcomes.append(outcome)
        return outcome

    def get_recent_state_incarceration_incident_outcome(
            self) -> Optional['StateIncarcerationIncidentOutcome']:
        if self.state_incarceration_incident_outcomes:
            return self.state_incarceration_incident_outcomes[-1]
        return None

    def prune(self) -> 'StateIncarcerationIncident':
        if not self.responding_officer:
            self.responding_officer = None
        self.state_incarceration_incident_outcomes = \
            [iio for iio in self.state_incarceration_incident_outcomes if iio]
        return self

    def sort(self):
        self.state_incarceration_incident_outcomes.sort()


class StateIncarcerationIncidentOutcome(IngestObject):
    """Class for information about a supervision violation response.
    Referenced from StateSupervisionViolationResponse.
    """

    def __init__(self, state_incarceration_incident_outcome_id=None,
                 outcome_type=None, date_effective=None, state_code=None,
                 outcome_description=None, punishment_length_days=None):
        self.state_incarceration_incident_outcome_id: Optional[str] = \
            state_incarceration_incident_outcome_id
        self.outcome_type: Optional[str] = outcome_type
        self.date_effective: Optional[str] = date_effective
        self.state_code: Optional[str] = state_code
        self.outcome_description: Optional[str] = outcome_description
        self.punishment_length_days: Optional[str] = punishment_length_days

    def __setattr__(self, name, value):
        restricted_setattr(self, 'punishment_length_days', name, value)


class StateParoleDecision(IngestObject):
    """Class for information about a parole decision.
    Referenced from IncarcerationPeriod.
    """

    def __init__(self, state_parole_decision_id=None, received_parole=None,
                 decision_date=None, corrective_action_deadline=None,
                 state_code=None, county_code=None, decision_outcome=None,
                 decision_reasoning=None, corrective_action=None,
                 decision_agents=None):
        self.state_parole_decision_id: Optional[str] = state_parole_decision_id
        self.received_parole: Optional[str] = received_parole
        self.decision_date: Optional[str] = decision_date
        self.corrective_action_deadline: Optional[str] = \
            corrective_action_deadline
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.decision_outcome: Optional[str] = decision_outcome
        self.decision_reasoning: Optional[str] = decision_reasoning
        self.corrective_action: Optional[str] = corrective_action

        self.decision_agents: List[StateAgent] = decision_agents or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'decision_agents', name, value)

    def create_state_agent(self, **kwargs) -> 'StateAgent':
        decision_agent = StateAgent(**kwargs)
        self.decision_agents.append(decision_agent)
        return decision_agent

    def get_recent_state_agent(self) -> Optional['StateAgent']:
        if self.decision_agents:
            return self.decision_agents[-1]
        return None

    def prune(self) -> 'StateParoleDecision':
        self.decision_agents = [da for da in self.decision_agents if da]
        return self

    def sort(self):
        self.decision_agents.sort()


class StateSupervisionViolation(IngestObject):
    """Class for information about a supervision violation.
    Referenced from SupervisionViolation.
    """

    def __init__(self, state_supervision_violation_id=None, violation_type=None,
                 violation_date=None, state_code=None, is_violent=None,
                 violated_conditions=None,
                 state_supervision_violation_responses=None):
        self.state_supervision_violation_id: Optional[str] = \
            state_supervision_violation_id
        self.violation_type: Optional[str] = violation_type
        self.violation_date: Optional[str] = violation_date
        self.state_code: Optional[str] = state_code
        self.is_violent: Optional[str] = is_violent
        self.violated_conditions: List[str] = violated_conditions or []

        self.state_supervision_violation_responses: \
            List[StateSupervisionViolationResponse] = \
            state_supervision_violation_responses or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'state_supervision_violation_responses',
                           name, value)

    def create_state_supervision_violation_response(self, **kwargs) \
            -> 'StateSupervisionViolationResponse':
        violation_response = StateSupervisionViolationResponse(**kwargs)
        self.state_supervision_violation_responses.append(violation_response)
        return violation_response

    def get_recent_state_supervision_violation_response(self) \
            -> Optional['StateSupervisionViolationResponse']:
        if self.state_supervision_violation_responses:
            return self.state_supervision_violation_responses[-1]
        return None

    def prune(self) -> 'StateSupervisionViolation':
        self.state_supervision_violation_responses = \
            [vr for vr in self.state_supervision_violation_responses if vr]

        return self

    def sort(self):
        self.state_supervision_violation_responses.sort()


class StateSupervisionViolationResponse(IngestObject):
    """Class for information about a supervision violation response.
    Referenced from StateSupervisionViolationResponse.
    """

    def __init__(self, state_supervision_violation_response_id=None,
                 response_type=None, response_date=None, state_code=None,
                 decision=None, revocation_type=None, deciding_body_type=None):
        self.state_supervision_violation_response_id: Optional[str] = \
            state_supervision_violation_response_id
        self.response_type: Optional[str] = response_type
        self.response_date: Optional[str] = response_date
        self.state_code: Optional[str] = state_code
        self.decision: Optional[str] = decision
        self.revocation_type: Optional[str] = revocation_type
        self.deciding_body_type: Optional[str] = deciding_body_type

    def __setattr__(self, name, value):
        restricted_setattr(self, 'deciding_body_type', name, value)


class StateAgent(IngestObject):
    """Class for information about some agent working within the justice system.
    Referenced from several state-level entities.
    """

    def __init__(self, state_agent_id=None, agent_type=None, state_code=None,
                 full_name=None):
        self.state_agent_id: Optional[str] = state_agent_id
        self.agent_type: Optional[str] = agent_type
        self.state_code: Optional[str] = state_code
        self.full_name: Optional[str] = full_name

    def __setattr__(self, name, value):
        restricted_setattr(self, 'full_name', name, value)


def eq(self, other, exclude=None):
    if other is None:
        return False
    if exclude is None:
        return self.__dict__ == other.__dict__

    return _without_exclusions(self, exclude) \
        == _without_exclusions(other, exclude)


def _without_exclusions(obj, exclude=None):
    if exclude is None:
        exclude = []
    return {k: v for k, v in obj.__dict__.items() if k not in exclude}


def to_bool(obj, exclude=None):
    if exclude is None:
        exclude = []
    return any(any(v) if isinstance(v, list) else v
               for k, v in obj.__dict__.items() if k not in exclude)


def to_string(obj, exclude=None):
    if exclude is None:
        exclude = []
    out = [obj.__class__.__name__ + ':']
    for key, val in vars(obj).items():
        if key in exclude:
            continue
        if isinstance(val, list):
            for index, elem in enumerate(val):
                out += '{}[{}]: {}'.format(key, index, elem).split('\n')
        elif val:
            out += '{}: {}'.format(key, val).split('\n')
    return '\n   '.join(out)


def to_repr(obj, exclude=None):
    if exclude is None:
        exclude = []
    args = []
    for key, val in vars(obj).items():
        if key in exclude:
            continue
        if val:
            args.append('{}={}'.format(key, repr(val)))

    return '{}({})'.format(obj.__class__.__name__, ', '.join(args))


def restricted_setattr(self, last_field, name, value):
    if isinstance(value, str) and (value == '' or value.isspace()):
        value = None
    if hasattr(self, last_field) and not hasattr(self, name):
        raise AttributeError("No field {} in object {}".format(name,
                                                               type(self)))
    self.__dict__[name] = value
