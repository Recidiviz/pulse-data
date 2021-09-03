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

from recidiviz.common.str_field_utils import to_snake_case

PLURALS = {
    "person": "people",
    "booking": "bookings",
    "charge": "charges",
    "hold": "holds",
    # TODO(#8905): Delete all references to state schema objects from this map once
    #  ingest mappings overhaul is complete for all states.
    "state_person": "state_people",
    "state_person_race": "state_person_races",
    "state_person_ethnicity": "state_person_ethnicities",
    "state_alias": "state_aliases",
    "state_assessment": "state_assessments",
    "state_program_assignment": "state_program_assignments",
    "state_person_external_id": "state_person_external_ids",
    "state_sentence_group": "state_sentence_groups",
    "state_supervision_sentence": "state_supervision_sentences",
    "state_incarceration_sentence": "state_incarceration_sentences",
    "state_fine": "state_fines",
    "state_supervision_period": "state_supervision_periods",
    "state_incarceration_period": "state_incarceration_periods",
    "state_charge": "state_charges",
    "state_incarceration_incident": "state_incarceration_incidents",
    "state_parole_decision": "state_parole_decisions",
    "state_supervision_violation": "state_supervision_violations",
    "state_supervision_violation_response": "state_supervision_violation_responses",
}


# TODO(#8905): Delete all references to state schema objects from this file once
#  ingest mappings overhaul is complete for all states.
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

    def class_name(self) -> str:
        return to_snake_case(self.__class__.__name__)


class IngestInfo(IngestObject):
    """Class for information about multiple people."""

    def __init__(self, people=None, state_people=None):
        self.people: List[Person] = people or []
        self.state_people: List[StatePerson] = state_people or []

    def __eq__(self, other):
        return eq(self, other, exclude=["_state_people_by_id"])

    def __bool__(self):
        return to_bool(self, exclude=["_state_people_by_id"])

    def __str__(self):
        return to_string(self, exclude=["_state_people_by_id"])

    def __repr__(self):
        return to_repr(self, exclude=["_state_people_by_id"])

    def __setattr__(self, name, value):
        restricted_setattr(self, "_state_people_by_id", name, value)

    def create_person(self, **kwargs) -> "Person":
        person = Person(**kwargs)
        self.people.append(person)
        return person

    def get_recent_person(self) -> Optional["Person"]:
        if self.people:
            return self.people[-1]
        return None

    def get_person_by_id(self, person_id) -> Optional["Person"]:
        return next((p for p in self.people if p.person_id == person_id), None)

    def create_state_person(self, **kwargs) -> "StatePerson":
        person = StatePerson(**kwargs)
        self.state_people.append(person)
        return person

    def get_recent_state_person(self) -> Optional["StatePerson"]:
        if self.state_people:
            return self.state_people[-1]
        return None

    def get_state_person_by_id(self, state_person_id) -> Optional["StatePerson"]:
        return next(
            (sp for sp in self.state_people if sp.state_person_id == state_person_id),
            None,
        )

    def prune(self) -> "IngestInfo":
        self.people = [person.prune() for person in self.people if person]
        self.state_people = [person.prune() for person in self.state_people if person]
        return self

    def sort(self):
        for person in self.people:
            person.sort()
        self.people.sort()

        for person in self.state_people:
            person.sort()
        self.state_people.sort()

    def get_all_people(self, predicate=lambda _: True) -> List["Person"]:
        return [person for person in self.people if predicate(person)]

    def get_all_bookings(self, predicate=lambda _: True) -> List["Booking"]:
        return [
            booking
            for person in self.get_all_people()
            for booking in person.bookings
            if predicate(booking)
        ]

    def get_all_arrests(self, predicate=lambda _: True) -> List["Arrest"]:
        return [
            booking.arrest
            for booking in self.get_all_bookings()
            if booking.arrest is not None and predicate(booking.arrest)
        ]

    def get_all_charges(self, predicate=lambda _: True) -> List["Charge"]:
        return [
            charge
            for booking in self.get_all_bookings()
            for charge in booking.charges
            if predicate(charge)
        ]

    def get_all_holds(self, predicate=lambda _: True) -> List["Hold"]:
        return [
            hold
            for booking in self.get_all_bookings()
            for hold in booking.holds
            if predicate(hold)
        ]

    def get_all_bonds(self, predicate=lambda _: True) -> List["Bond"]:
        return [
            charge.bond
            for charge in self.get_all_charges()
            if charge.bond is not None and predicate(charge.bond)
        ]

    def get_all_sentences(self, predicate=lambda _: True) -> List["Sentence"]:
        return [
            charge.sentence
            for charge in self.get_all_charges()
            if charge.sentence is not None and predicate(charge.sentence)
        ]

    def get_all_state_people(self, predicate=lambda _: True) -> List["StatePerson"]:
        return [person for person in self.state_people if predicate(person)]


class Person(IngestObject):
    """Class for information about a person. Referenced from IngestInfo."""

    def __init__(
        self,
        person_id=None,
        full_name=None,
        surname=None,
        given_names=None,
        middle_names=None,
        name_suffix=None,
        birthdate=None,
        gender=None,
        age=None,
        race=None,
        ethnicity=None,
        place_of_residence=None,
        jurisdiction_id=None,
        bookings=None,
    ):
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
        restricted_setattr(self, "bookings", name, value)

    def create_booking(self, **kwargs) -> "Booking":
        booking = Booking(**kwargs)
        self.bookings.append(booking)
        return booking

    def get_recent_booking(self) -> Optional["Booking"]:
        if self.bookings:
            return self.bookings[-1]
        return None

    def get_booking_by_id(self, booking_id) -> Optional["Booking"]:
        return next((b for b in self.bookings if b.booking_id == booking_id), None)

    def prune(self) -> "Person":
        self.bookings = [booking.prune() for booking in self.bookings if booking]
        return self

    def sort(self):
        for booking in self.bookings:
            booking.sort()
        self.bookings.sort()


class Booking(IngestObject):
    """Class for information about a booking. Referenced from Person."""

    def __init__(
        self,
        booking_id=None,
        admission_date=None,
        admission_reason=None,
        projected_release_date=None,
        release_date=None,
        release_reason=None,
        custody_status=None,
        facility=None,
        facility_id=None,
        classification=None,
        total_bond_amount=None,
        arrest=None,
        charges=None,
        holds=None,
    ):
        self.booking_id: Optional[str] = booking_id
        self.admission_date: Optional[str] = admission_date
        self.admission_reason: Optional[str] = admission_reason
        self.projected_release_date: Optional[str] = projected_release_date
        self.release_date: Optional[str] = release_date
        self.release_reason: Optional[str] = release_reason
        self.custody_status: Optional[str] = custody_status
        self.facility: Optional[str] = facility
        self.facility_id: Optional[str] = facility_id
        self.classification: Optional[str] = classification
        self.total_bond_amount: Optional[str] = total_bond_amount

        self.arrest: Optional[Arrest] = arrest
        self.charges: List[Charge] = charges or []
        self.holds: List[Hold] = holds or []

    def __setattr__(self, name, value):
        restricted_setattr(self, "holds", name, value)

    def create_arrest(self, **kwargs) -> "Arrest":
        self.arrest = Arrest(**kwargs)
        return self.arrest

    def create_charge(self, **kwargs) -> "Charge":
        charge = Charge(**kwargs)
        self.charges.append(charge)
        return charge

    def create_hold(self, **kwargs) -> "Hold":
        hold = Hold(**kwargs)
        self.holds.append(hold)
        return hold

    def get_recent_charge(self) -> Optional["Charge"]:
        if self.charges:
            return self.charges[-1]
        return None

    def get_charge_by_id(self, charge_id) -> Optional["Charge"]:
        return next((c for c in self.charges if c.charge_id == charge_id), None)

    def get_recent_hold(self) -> Optional["Hold"]:
        if self.holds:
            return self.holds[-1]
        return None

    def get_hold_by_id(self, hold_id) -> Optional["Hold"]:
        return next((h for h in self.holds if h.hold_id == hold_id), None)

    def get_recent_arrest(self) -> Optional["Arrest"]:
        return self.arrest

    def get_arrest_by_id(self, arrest_id) -> Optional["Arrest"]:
        return (
            self.arrest if self.arrest and self.arrest.arrest_id == arrest_id else None
        )

    def prune(self) -> "Booking":
        self.charges = [charge.prune() for charge in self.charges if charge]
        self.holds = [hold for hold in self.holds if hold]
        if not self.arrest:
            self.arrest = None
        return self

    def sort(self):
        self.charges.sort()
        self.holds.sort()


class Arrest(IngestObject):
    """Class for information about an arrest. Referenced from Booking."""

    def __init__(
        self,
        arrest_id=None,
        arrest_date=None,
        location=None,
        officer_name=None,
        officer_id=None,
        agency=None,
    ):
        self.arrest_id: Optional[str] = arrest_id
        self.arrest_date: Optional[str] = arrest_date
        self.location: Optional[str] = location
        self.officer_name: Optional[str] = officer_name
        self.officer_id: Optional[str] = officer_id
        self.agency: Optional[str] = agency

    def __setattr__(self, name, value):
        restricted_setattr(self, "agency", name, value)


class Charge(IngestObject):
    """Class for information about a charge. Referenced from Booking."""

    def __init__(
        self,
        charge_id=None,
        offense_date=None,
        statute=None,
        name=None,
        attempted=None,
        degree=None,
        charge_class=None,
        level=None,
        fee_dollars=None,
        charging_entity=None,
        status=None,
        number_of_counts=None,
        court_type=None,
        case_number=None,
        next_court_date=None,
        judge_name=None,
        bond=None,
        sentence=None,
        charge_notes=None,
    ):
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
        restricted_setattr(self, "sentence", name, value)

    def create_bond(self, **kwargs) -> "Bond":
        self.bond = Bond(**kwargs)
        return self.bond

    def create_sentence(self, **kwargs) -> "Sentence":
        self.sentence = Sentence(**kwargs)
        return self.sentence

    def get_recent_bond(self) -> Optional["Bond"]:
        return self.bond

    def get_bond_by_id(self, bond_id) -> Optional["Bond"]:
        return self.bond if self.bond and self.bond.bond_id == bond_id else None

    def get_recent_sentence(self) -> Optional["Sentence"]:
        return self.sentence

    def prune(self) -> "Charge":
        if not self.bond:
            self.bond = None
        if not self.sentence:
            self.sentence = None
        return self


class Hold(IngestObject):
    """Class for information about a hold. Referenced from Booking.

    Holds will default to the jurisdiction_name "UNSPECIFIED", since we may only know that a booking has a hold. Then
    booking.create_hold() will create a hold that doesn't get pruned later.
    """

    def __init__(self, hold_id=None, jurisdiction_name="UNSPECIFIED", status=None):
        self.hold_id: Optional[str] = hold_id
        self.jurisdiction_name: Optional[str] = jurisdiction_name
        self.status: Optional[str] = status

    def __setattr__(self, name, value):
        restricted_setattr(self, "status", name, value)


class Bond(IngestObject):
    """Class for information about a bond. Referenced from Charge."""

    def __init__(
        self, bond_id=None, amount=None, bond_type=None, status=None, bond_agent=None
    ):
        self.bond_id: Optional[str] = bond_id
        self.amount: Optional[str] = amount
        self.bond_type: Optional[str] = bond_type
        self.bond_agent: Optional[str] = bond_agent
        self.status: Optional[str] = status

    def __setattr__(self, name, value):
        restricted_setattr(self, "status", name, value)


class Sentence(IngestObject):
    """Class for information about a sentence. Referenced from Charge."""

    def __init__(
        self,
        sentence_id=None,
        date_imposed=None,
        status=None,
        sentencing_region=None,
        min_length=None,
        max_length=None,
        is_life=None,
        is_probation=None,
        is_suspended=None,
        fine_dollars=None,
        parole_possible=None,
        post_release_supervision_length=None,
        completion_date=None,
        projected_completion_date=None,
        sentence_relationships=None,
    ):
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
        self.projected_completion_date: Optional[str] = projected_completion_date

        self.post_release_supervision_length: Optional[
            str
        ] = post_release_supervision_length

        self.sentence_relationships: List[SentenceRelationship] = (
            sentence_relationships or []
        )

    def __setattr__(self, name, value):
        restricted_setattr(self, "sentence_relationships", name, value)

    def create_sentence_relationship(
        self, other_sentence, relationship_type
    ) -> "SentenceRelationship":
        relationship = SentenceRelationship(
            sentence_a=self,
            sentence_b=other_sentence,
            relationship_type=relationship_type,
        )
        self.sentence_relationships.append(relationship)
        return relationship


# TODO(#1145): add logic to convert to SentenceRelationship proto (and also add normalization logic to remove duplicates
#  where A->B and B->A relationship are both provided)
class SentenceRelationship(IngestObject):
    """Class for information about the relationship between two sentences. Referenced from Sentence."""

    def __init__(
        self,
        sentence_relationship_id=None,
        sentence_a=None,
        sentence_b=None,
        relationship_type=None,
    ):
        self.sentence_relationship_id: Optional[str] = sentence_relationship_id
        self.sentence_a: Optional[Sentence] = sentence_a
        self.sentence_b: Optional[Sentence] = sentence_b
        self.relationship_type: Optional[str] = relationship_type

    def __setattr__(self, name, value):
        restricted_setattr(self, "relationship_type", name, value)


class StatePerson(IngestObject):
    """Class for information about a person at the state level. Referenced from IngestInfo."""

    def __init__(
        self,
        state_person_id=None,
        full_name=None,
        surname=None,
        given_names=None,
        middle_names=None,
        name_suffix=None,
        birthdate=None,
        gender=None,
        age=None,
        current_address=None,
        residency_status=None,
        state_person_races=None,
        state_person_ethnicities=None,
        state_aliases=None,
        state_person_external_ids=None,
        state_assessments=None,
        state_sentence_groups=None,
        state_program_assignments=None,
        supervising_officer=None,
        state_code=None,
    ):
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
        self.state_code: Optional[str] = state_code

        self.state_person_races: List[StatePersonRace] = state_person_races or []
        self.state_person_ethnicities: List[StatePersonEthnicity] = (
            state_person_ethnicities or []
        )
        self.state_aliases: List[StateAlias] = state_aliases or []
        self.state_person_external_ids: List[StatePersonExternalId] = (
            state_person_external_ids or []
        )
        self.state_assessments: List[StateAssessment] = state_assessments or []
        self.state_sentence_groups: List[StateSentenceGroup] = (
            state_sentence_groups or []
        )
        self.state_program_assignments: List[StateProgramAssignment] = (
            state_program_assignments or []
        )
        self.supervising_officer: Optional[StateAgent] = supervising_officer

    def __setattr__(self, name, value):
        restricted_setattr(self, "supervising_officer", name, value)

    def create_state_person_race(self, **kwargs) -> "StatePersonRace":
        race = StatePersonRace(**kwargs)
        self.state_person_races.append(race)
        return race

    def create_state_person_ethnicity(self, **kwargs) -> "StatePersonEthnicity":
        ethnicity = StatePersonEthnicity(**kwargs)
        self.state_person_ethnicities.append(ethnicity)
        return ethnicity

    def create_state_alias(self, **kwargs) -> "StateAlias":
        alias = StateAlias(**kwargs)
        self.state_aliases.append(alias)
        return alias

    def create_state_person_external_id(self, **kwargs) -> "StatePersonExternalId":
        external_id = StatePersonExternalId(**kwargs)
        self.state_person_external_ids.append(external_id)
        return external_id

    def create_state_assessment(self, **kwargs) -> "StateAssessment":
        assessment = StateAssessment(**kwargs)
        self.state_assessments.append(assessment)
        return assessment

    def create_state_sentence_group(self, **kwargs) -> "StateSentenceGroup":
        sentence_group = StateSentenceGroup(**kwargs)
        self.state_sentence_groups.append(sentence_group)
        return sentence_group

    def create_state_program_assignment(self, **kwargs) -> "StateProgramAssignment":
        program_assignment = StateProgramAssignment(**kwargs)
        self.state_program_assignments.append(program_assignment)
        return program_assignment

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.supervising_officer = StateAgent(**kwargs)
        return self.supervising_officer

    def get_state_person_race_by_id(
        self, state_person_race_id
    ) -> Optional["StatePersonRace"]:
        return next(
            (
                spr
                for spr in self.state_person_races
                if spr.state_person_race_id == state_person_race_id
            ),
            None,
        )

    def get_state_person_ethnicity_by_id(
        self, state_person_ethnicity_id
    ) -> Optional["StatePersonEthnicity"]:
        return next(
            (
                spe
                for spe in self.state_person_ethnicities
                if spe.state_person_ethnicity_id == state_person_ethnicity_id
            ),
            None,
        )

    def get_state_alias_by_id(self, state_alias_id) -> Optional["StateAlias"]:
        return next(
            (sa for sa in self.state_aliases if sa.state_alias_id == state_alias_id),
            None,
        )

    def get_state_person_external_id_by_id(
        self, state_person_external_id_id
    ) -> Optional["StatePersonExternalId"]:
        return next(
            (
                eid
                for eid in self.state_person_external_ids
                if eid.state_person_external_id_id == state_person_external_id_id
            ),
            None,
        )

    def get_state_assessment_by_id(
        self, state_assessment_id
    ) -> Optional["StateAssessment"]:
        return next(
            (
                sa
                for sa in self.state_assessments
                if sa.state_assessment_id == state_assessment_id
            ),
            None,
        )

    def get_state_program_assignment_by_id(
        self, state_program_assignment_id
    ) -> Optional["StateProgramAssignment"]:
        return next(
            (
                pa
                for pa in self.state_program_assignments
                if pa.state_program_assignment_id == state_program_assignment_id
            ),
            None,
        )

    def get_state_sentence_group_by_id(
        self, sentence_group_id
    ) -> Optional["StateSentenceGroup"]:
        return next(
            (
                sg
                for sg in self.state_sentence_groups
                if sg.state_sentence_group_id == sentence_group_id
            ),
            None,
        )

    def prune(self) -> "StatePerson":
        self.state_sentence_groups = [
            sg.prune() for sg in self.state_sentence_groups if sg
        ]
        if not self.supervising_officer:
            self.supervising_officer = None
        return self

    def sort(self):
        self.state_person_races.sort()
        self.state_person_ethnicities.sort()
        self.state_aliases.sort()
        self.state_person_external_ids.sort()
        self.state_assessments.sort()
        self.state_program_assignments.sort()

        for sentence_group in self.state_sentence_groups:
            sentence_group.sort()
        self.state_sentence_groups.sort()


class StatePersonExternalId(IngestObject):
    """Class for information about one of a StatePerson's external ids. Referenced from StatePerson."""

    def __init__(self, state_person_external_id_id=None, id_type=None, state_code=None):
        self.state_person_external_id_id: Optional[str] = state_person_external_id_id
        self.id_type: Optional[str] = id_type
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateAlias(IngestObject):
    """Class for information about an alias. Referenced from StatePerson."""

    def __init__(
        self,
        state_alias_id=None,
        state_code=None,
        full_name=None,
        surname=None,
        given_names=None,
        middle_names=None,
        name_suffix=None,
        alias_type=None,
    ):
        self.state_alias_id: Optional[str] = state_alias_id
        self.state_code: Optional[str] = state_code
        self.full_name: Optional[str] = full_name
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix
        self.alias_type: Optional[str] = alias_type

    def __setattr__(self, name, value):
        restricted_setattr(self, "alias_type", name, value)


class StatePersonRace(IngestObject):
    """Class for information about a state person's race. Referenced from StatePerson."""

    def __init__(self, state_person_race_id=None, race=None, state_code=None):
        self.state_person_race_id: Optional[str] = state_person_race_id
        self.race: Optional[str] = race
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StatePersonEthnicity(IngestObject):
    """Class for information about a state person's ethnicity. Referenced from StatePerson."""

    def __init__(self, state_person_ethnicity_id=None, ethnicity=None, state_code=None):
        self.state_person_ethnicity_id: Optional[str] = state_person_ethnicity_id
        self.ethnicity: Optional[str] = ethnicity
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateAssessment(IngestObject):
    """Class for information about an assessment. Referenced from StatePerson."""

    def __init__(
        self,
        state_assessment_id=None,
        assessment_class=None,
        assessment_type=None,
        assessment_date=None,
        state_code=None,
        assessment_score=None,
        assessment_level=None,
        assessment_metadata=None,
        conducting_agent=None,
    ):
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
        restricted_setattr(self, "conducting_agent", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.conducting_agent = StateAgent(**kwargs)
        return self.conducting_agent

    def prune(self) -> "StateAssessment":
        if not self.conducting_agent:
            self.conducting_agent = None
        return self


class StateSentenceGroup(IngestObject):
    """Class for information about a group of related sentences. Referenced from StatePerson."""

    def __init__(
        self,
        state_sentence_group_id=None,
        status=None,
        date_imposed=None,
        state_code=None,
        county_code=None,
        min_length=None,
        max_length=None,
        is_life=None,
        state_supervision_sentences=None,
        state_incarceration_sentences=None,
        state_fines=None,
    ):
        self.state_sentence_group_id: Optional[str] = state_sentence_group_id
        self.status: Optional[str] = status
        self.date_imposed: Optional[str] = date_imposed
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length
        # TODO(#2668): Delete this from SentenceGroup
        self.is_life: Optional[str] = is_life

        self.state_supervision_sentences: List[StateSupervisionSentence] = (
            state_supervision_sentences or []
        )
        self.state_incarceration_sentences: List[StateIncarcerationSentence] = (
            state_incarceration_sentences or []
        )
        self.state_fines: List[StateFine] = state_fines or []

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_fines", name, value)

    def create_state_supervision_sentence(self, **kwargs) -> "StateSupervisionSentence":
        supervision_sentence = StateSupervisionSentence(**kwargs)
        self.state_supervision_sentences.append(supervision_sentence)
        return supervision_sentence

    def create_state_incarceration_sentence(
        self, **kwargs
    ) -> "StateIncarcerationSentence":
        incarceration_sentence = StateIncarcerationSentence(**kwargs)
        self.state_incarceration_sentences.append(incarceration_sentence)
        return incarceration_sentence

    def create_state_fine(self, **kwargs) -> "StateFine":
        fine = StateFine(**kwargs)
        self.state_fines.append(fine)
        return fine

    def get_state_supervision_sentence_by_id(
        self, supervision_sentence_id
    ) -> Optional["StateSupervisionSentence"]:
        return next(
            (
                ss
                for ss in self.state_supervision_sentences
                if ss.state_supervision_sentence_id == supervision_sentence_id
            ),
            None,
        )

    def get_state_incarceration_sentence_by_id(
        self, incarceration_sentence_id
    ) -> Optional["StateIncarcerationSentence"]:
        return next(
            (
                ins
                for ins in self.state_incarceration_sentences
                if ins.state_incarceration_sentence_id == incarceration_sentence_id
            ),
            None,
        )

    def prune(self) -> "StateSentenceGroup":
        self.state_supervision_sentences = [
            ss.prune() for ss in self.state_supervision_sentences if ss
        ]

        self.state_incarceration_sentences = [
            ins.prune() for ins in self.state_incarceration_sentences if ins
        ]

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
    """Class for information about a sentence to supervision. Referenced from SentenceGroup."""

    def __init__(
        self,
        state_supervision_sentence_id=None,
        status=None,
        supervision_type=None,
        date_imposed=None,
        start_date=None,
        projected_completion_date=None,
        completion_date=None,
        state_code=None,
        county_code=None,
        min_length=None,
        max_length=None,
        state_charges=None,
        state_incarceration_periods=None,
        state_supervision_periods=None,
        state_early_discharges=None,
    ):
        self.state_supervision_sentence_id: Optional[
            str
        ] = state_supervision_sentence_id
        self.status: Optional[str] = status
        self.supervision_type: Optional[str] = supervision_type
        self.date_imposed: Optional[str] = date_imposed
        self.start_date: Optional[str] = start_date
        self.projected_completion_date: Optional[str] = projected_completion_date
        self.completion_date: Optional[str] = completion_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length

        self.state_charges: List[StateCharge] = state_charges or []
        self.state_incarceration_periods: List[StateIncarcerationPeriod] = (
            state_incarceration_periods or []
        )
        self.state_supervision_periods: List[StateSupervisionPeriod] = (
            state_supervision_periods or []
        )
        self.state_early_discharges: List[StateEarlyDischarge] = (
            state_early_discharges or []
        )

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_early_discharges", name, value)

    def create_state_charge(self, **kwargs) -> "StateCharge":
        charge = StateCharge(**kwargs)
        self.state_charges.append(charge)
        return charge

    def create_state_incarceration_period(self, **kwargs) -> "StateIncarcerationPeriod":
        incarceration_period = StateIncarcerationPeriod(**kwargs)
        self.state_incarceration_periods.append(incarceration_period)
        return incarceration_period

    def create_state_supervision_period(self, **kwargs) -> "StateSupervisionPeriod":
        supervision_period = StateSupervisionPeriod(**kwargs)
        self.state_supervision_periods.append(supervision_period)
        return supervision_period

    def create_state_early_discharge(self, **kwargs) -> "StateEarlyDischarge":
        early_discharge = StateEarlyDischarge(**kwargs)
        self.state_early_discharges.append(early_discharge)
        return early_discharge

    def get_state_charge_by_id(self, state_charge_id) -> Optional["StateCharge"]:
        return next(
            (sc for sc in self.state_charges if sc.state_charge_id == state_charge_id),
            None,
        )

    def get_state_incarceration_period_by_id(
        self, incarceration_period_id
    ) -> Optional["StateIncarcerationPeriod"]:
        return next(
            (
                ip
                for ip in self.state_incarceration_periods
                if ip.state_incarceration_period_id == incarceration_period_id
            ),
            None,
        )

    def get_state_supervision_period_by_id(
        self, state_supervision_period_id
    ) -> Optional["StateSupervisionPeriod"]:
        return next(
            (
                sc
                for sc in self.state_supervision_periods
                if sc.state_supervision_period_id == state_supervision_period_id
            ),
            None,
        )

    def get_state_early_discharge_by_id(
        self, state_early_discharge_id
    ) -> Optional["StateEarlyDischarge"]:
        return next(
            (
                ed
                for ed in self.state_early_discharges
                if ed.state_early_discharge_id == state_early_discharge_id
            ),
            None,
        )

    def prune(self) -> "StateSupervisionSentence":
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]

        self.state_incarceration_periods = [
            ip.prune() for ip in self.state_incarceration_periods if ip
        ]

        self.state_supervision_periods = [
            sp.prune() for sp in self.state_supervision_periods if sp
        ]

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
    """Class for information about a sentence to incarceration. Referenced from SentenceGroup."""

    def __init__(
        self,
        state_incarceration_sentence_id=None,
        status=None,
        incarceration_type=None,
        date_imposed=None,
        start_date=None,
        projected_min_release_date=None,
        projected_max_release_date=None,
        completion_date=None,
        parole_eligibility_date=None,
        state_code=None,
        county_code=None,
        min_length=None,
        max_length=None,
        is_life=None,
        is_capital_punishment=None,
        parole_possible=None,
        initial_time_served=None,
        good_time=None,
        earned_time=None,
        state_charges=None,
        state_incarceration_periods=None,
        state_supervision_periods=None,
        state_early_discharges=None,
    ):
        self.state_incarceration_sentence_id: Optional[
            str
        ] = state_incarceration_sentence_id
        self.status: Optional[str] = status
        self.incarceration_type: Optional[str] = incarceration_type
        self.date_imposed: Optional[str] = date_imposed
        self.start_date: Optional[str] = start_date
        self.projected_min_release_date: Optional[str] = projected_min_release_date
        self.projected_max_release_date: Optional[str] = projected_max_release_date
        self.completion_date: Optional[str] = completion_date
        self.parole_eligibility_date: Optional[str] = parole_eligibility_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.min_length: Optional[str] = min_length
        self.max_length: Optional[str] = max_length
        self.is_life: Optional[str] = is_life
        self.is_capital_punishment: Optional[str] = is_capital_punishment
        self.parole_possible: Optional[str] = parole_possible
        self.initial_time_served: Optional[str] = initial_time_served
        self.good_time: Optional[str] = good_time
        self.earned_time: Optional[str] = earned_time

        self.state_charges: List[StateCharge] = state_charges or []
        self.state_incarceration_periods: List[StateIncarcerationPeriod] = (
            state_incarceration_periods or []
        )
        self.state_supervision_periods: List[StateSupervisionPeriod] = (
            state_supervision_periods or []
        )
        self.state_early_discharges: List[StateEarlyDischarge] = (
            state_early_discharges or []
        )

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_early_discharges", name, value)

    def create_state_charge(self, **kwargs) -> "StateCharge":
        state_charge = StateCharge(**kwargs)
        self.state_charges.append(state_charge)
        return state_charge

    def create_state_incarceration_period(self, **kwargs) -> "StateIncarcerationPeriod":
        incarceration_period = StateIncarcerationPeriod(**kwargs)
        self.state_incarceration_periods.append(incarceration_period)
        return incarceration_period

    def create_state_supervision_period(self, **kwargs) -> "StateSupervisionPeriod":
        supervision_period = StateSupervisionPeriod(**kwargs)
        self.state_supervision_periods.append(supervision_period)
        return supervision_period

    def create_state_early_discharge(self, **kwargs) -> "StateEarlyDischarge":
        early_discharge = StateEarlyDischarge(**kwargs)
        self.state_early_discharges.append(early_discharge)
        return early_discharge

    def get_state_charge_by_id(self, state_charge_id) -> Optional["StateCharge"]:
        return next(
            (sc for sc in self.state_charges if sc.state_charge_id == state_charge_id),
            None,
        )

    def get_state_incarceration_period_by_id(
        self, incarceration_period_id
    ) -> Optional["StateIncarcerationPeriod"]:
        return next(
            (
                ip
                for ip in self.state_incarceration_periods
                if ip.state_incarceration_period_id == incarceration_period_id
            ),
            None,
        )

    def get_state_supervision_period_by_id(
        self, supervision_period_id
    ) -> Optional["StateSupervisionPeriod"]:
        return next(
            (
                sp
                for sp in self.state_supervision_periods
                if sp.state_supervision_period_id == supervision_period_id
            ),
            None,
        )

    def get_state_early_discharge_by_id(
        self, state_early_discharge_id
    ) -> Optional["StateEarlyDischarge"]:
        return next(
            (
                ed
                for ed in self.state_early_discharges
                if ed.state_early_discharge_id == state_early_discharge_id
            ),
            None,
        )

    def prune(self) -> "StateIncarcerationSentence":
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]

        self.state_incarceration_periods = [
            ip.prune() for ip in self.state_incarceration_periods if ip
        ]

        self.state_supervision_periods = [
            sp.prune() for sp in self.state_supervision_periods if sp
        ]

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
    """Class for information about a fine associated with a sentence. Referenced from SentenceGroup."""

    def __init__(
        self,
        state_fine_id=None,
        status=None,
        date_paid=None,
        state_code=None,
        county_code=None,
        fine_dollars=None,
        state_charges=None,
    ):
        self.state_fine_id: Optional[str] = state_fine_id
        self.status: Optional[str] = status
        self.date_paid: Optional[str] = date_paid
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.fine_dollars: Optional[str] = fine_dollars

        self.state_charges: List[StateCharge] = state_charges or []

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_charges", name, value)

    def create_state_charge(self, **kwargs) -> "StateCharge":
        charge = StateCharge(**kwargs)
        self.state_charges.append(charge)
        return charge

    def prune(self) -> "StateFine":
        self.state_charges = [sc.prune() for sc in self.state_charges if sc]
        return self

    def sort(self):
        self.state_charges.sort()


class StateCharge(IngestObject):
    """Class for information about a charge. Referenced from IncarcerationSentence, SupervisionSentence, and Fine."""

    def __init__(
        self,
        state_charge_id=None,
        status=None,
        offense_date=None,
        date_charged=None,
        state_code=None,
        county_code=None,
        ncic_code=None,
        statute=None,
        description=None,
        attempted=None,
        classification_type=None,
        classification_subtype=None,
        offense_type=None,
        is_violent=None,
        is_sex_offense=None,
        counts=None,
        charge_notes=None,
        is_controlling=None,
        charging_entity=None,
        state_court_case=None,
        state_bond=None,
    ):
        self.state_charge_id: Optional[str] = state_charge_id
        self.status: Optional[str] = status
        self.offense_date: Optional[str] = offense_date
        self.date_charged: Optional[str] = date_charged
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.ncic_code: Optional[str] = ncic_code
        self.statute: Optional[str] = statute
        self.description: Optional[str] = description
        self.attempted: Optional[str] = attempted
        self.classification_type: Optional[str] = classification_type
        self.classification_subtype: Optional[str] = classification_subtype
        self.offense_type: Optional[str] = offense_type
        self.is_violent: Optional[str] = is_violent
        self.is_sex_offense: Optional[str] = is_sex_offense
        self.counts: Optional[str] = counts
        self.charge_notes: Optional[str] = charge_notes
        self.is_controlling: Optional[str] = is_controlling
        self.charging_entity: Optional[str] = charging_entity

        self.state_court_case: Optional[StateCourtCase] = state_court_case
        self.state_bond: Optional[StateBond] = state_bond

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_bond", name, value)

    def create_state_court_case(self, **kwargs) -> "StateCourtCase":
        court_case = StateCourtCase(**kwargs)
        self.state_court_case = court_case
        return self.state_court_case

    def create_state_bond(self, **kwargs) -> "StateBond":
        bond = StateBond(**kwargs)
        self.state_bond = bond
        return self.state_bond

    def get_state_court_case_by_id(self, court_case_id) -> Optional["StateCourtCase"]:
        if (
            self.state_court_case
            and self.state_court_case.state_court_case_id == court_case_id
        ):
            return self.state_court_case
        return None

    def prune(self) -> "StateCharge":
        if not self.state_court_case:
            self.state_court_case = None
        if not self.state_bond:
            self.state_bond = None
        return self


class StateCourtCase(IngestObject):
    """Class for information about a court case. Referenced from StateCharge."""

    def __init__(
        self,
        state_court_case_id=None,
        status=None,
        court_type=None,
        date_convicted=None,
        next_court_date=None,
        state_code=None,
        county_code=None,
        judicial_district_code=None,
        court_fee_dollars=None,
        judge=None,
    ):
        self.state_court_case_id: Optional[str] = state_court_case_id
        self.status: Optional[str] = status
        self.court_type: Optional[str] = court_type
        self.date_convicted: Optional[str] = date_convicted
        self.next_court_date: Optional[str] = next_court_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.judicial_district_code: Optional[str] = judicial_district_code
        self.court_fee_dollars: Optional[str] = court_fee_dollars

        self.judge: Optional[StateAgent] = judge

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.judge = StateAgent(**kwargs)
        return self.judge

    def get_state_agent_by_id(self, state_agent_id) -> Optional["StateAgent"]:
        if self.judge and self.judge.state_agent_id == state_agent_id:
            return self.judge
        return None

    def __setattr__(self, name, value):
        restricted_setattr(self, "judge", name, value)


class StateBond(IngestObject):
    """Class for information about a bond.
    Referenced from StateCharge.
    """

    def __init__(
        self,
        state_bond_id=None,
        status=None,
        bond_type=None,
        date_paid=None,
        state_code=None,
        county_code=None,
        amount=None,
        bond_agent=None,
    ):
        self.state_bond_id: Optional[str] = state_bond_id
        self.status: Optional[str] = status
        self.bond_type: Optional[str] = bond_type
        self.date_paid: Optional[str] = date_paid
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.amount: Optional[str] = amount
        self.bond_agent: Optional[str] = bond_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, "bond_agent", name, value)


class StateIncarcerationPeriod(IngestObject):
    """Class for information about a period of incarceration. Referenced from IncarcerationSentence and
    SupervisionSentence.
    """

    def __init__(
        self,
        state_incarceration_period_id=None,
        status=None,
        incarceration_type=None,
        admission_date=None,
        release_date=None,
        state_code=None,
        county_code=None,
        facility=None,
        housing_unit=None,
        facility_security_level=None,
        admission_reason=None,
        projected_release_reason=None,
        release_reason=None,
        specialized_purpose_for_incarceration=None,
        state_incarceration_incidents=None,
        state_parole_decisions=None,
        state_assessments=None,
        state_program_assignments=None,
        custodial_authority=None,
    ):
        self.state_incarceration_period_id: Optional[
            str
        ] = state_incarceration_period_id
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
        self.specialized_purpose_for_incarceration: Optional[
            str
        ] = specialized_purpose_for_incarceration
        self.custodial_authority = custodial_authority

        self.state_incarceration_incidents: List[StateIncarcerationIncident] = (
            state_incarceration_incidents or []
        )
        self.state_parole_decisions: List[StateParoleDecision] = (
            state_parole_decisions or []
        )
        self.state_assessments: List[StateAssessment] = state_assessments or []
        self.state_program_assignments: List[StateProgramAssignment] = (
            state_program_assignments or []
        )

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_program_assignments", name, value)

    def create_state_incarceration_incident(
        self, **kwargs
    ) -> "StateIncarcerationIncident":
        incarceration_incident = StateIncarcerationIncident(**kwargs)
        self.state_incarceration_incidents.append(incarceration_incident)
        return incarceration_incident

    def create_state_parole_decision(self, **kwargs) -> "StateParoleDecision":
        parole_decision = StateParoleDecision(**kwargs)
        self.state_parole_decisions.append(parole_decision)
        return parole_decision

    def create_state_assessment(self, **kwargs) -> "StateAssessment":
        assessment = StateAssessment(**kwargs)
        self.state_assessments.append(assessment)
        return assessment

    def create_state_program_assignment(self, **kwargs) -> "StateProgramAssignment":
        program_assignment = StateProgramAssignment(**kwargs)
        self.state_program_assignments.append(program_assignment)
        return program_assignment

    def get_state_incarceration_incident_by_id(
        self, state_incarceration_incident_id
    ) -> Optional["StateIncarcerationIncident"]:
        return next(
            (
                ii
                for ii in self.state_incarceration_incidents
                if ii.state_incarceration_incident_id == state_incarceration_incident_id
            ),
            None,
        )

    def prune(self) -> "StateIncarcerationPeriod":
        self.state_incarceration_incidents = [
            ii for ii in self.state_incarceration_incidents if ii
        ]
        self.state_parole_decisions = [pd for pd in self.state_parole_decisions if pd]
        self.state_assessments = [a for a in self.state_assessments if a]
        self.state_program_assignments = [
            p.prune() for p in self.state_program_assignments if p
        ]

        return self

    def sort(self):
        self.state_incarceration_incidents.sort()
        self.state_parole_decisions.sort()
        self.state_assessments.sort()
        self.state_program_assignments.sort()


class StateSupervisionPeriod(IngestObject):
    """Class for information about a period of supervision. Referenced from IncarcerationSentence and
    SupervisionSentence.
    """

    def __init__(
        self,
        state_supervision_period_id=None,
        status=None,
        supervision_type=None,
        start_date=None,
        termination_date=None,
        state_code=None,
        county_code=None,
        supervision_site=None,
        admission_reason=None,
        termination_reason=None,
        supervision_level=None,
        conditions=None,
        supervising_officer=None,
        state_supervision_violation_entries=None,
        state_assessments=None,
        state_program_assignments=None,
        state_supervision_case_type_entries=None,
        supervision_period_supervision_type=None,
        custodial_authority=None,
        state_supervision_contacts=None,
    ):
        self.state_supervision_period_id: Optional[str] = state_supervision_period_id
        self.status: Optional[str] = status
        self.supervision_type: Optional[str] = supervision_type
        self.supervision_period_supervision_type: Optional[
            str
        ] = supervision_period_supervision_type
        self.start_date: Optional[str] = start_date
        self.termination_date: Optional[str] = termination_date
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.supervision_site: Optional[str] = supervision_site
        self.admission_reason: Optional[str] = admission_reason
        self.termination_reason: Optional[str] = termination_reason
        self.supervision_level: Optional[str] = supervision_level
        self.custodial_authority: Optional[str] = custodial_authority
        self.conditions: List[str] = conditions or []

        self.supervising_officer: Optional[StateAgent] = supervising_officer
        self.state_supervision_violation_entries: List[StateSupervisionViolation] = (
            state_supervision_violation_entries or []
        )
        self.state_assessments: List[StateAssessment] = state_assessments or []
        self.state_program_assignments: List[StateProgramAssignment] = (
            state_program_assignments or []
        )
        self.state_supervision_case_type_entries: List[
            StateSupervisionCaseTypeEntry
        ] = (state_supervision_case_type_entries or [])
        self.state_supervision_contacts: List[StateSupervisionContact] = (
            state_supervision_contacts or []
        )

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_case_type_entries", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.supervising_officer = StateAgent(**kwargs)
        return self.supervising_officer

    def create_state_supervision_violation(
        self, **kwargs
    ) -> "StateSupervisionViolation":
        supervision_violation = StateSupervisionViolation(**kwargs)
        self.state_supervision_violation_entries.append(supervision_violation)
        return supervision_violation

    def create_state_assessment(self, **kwargs) -> "StateAssessment":
        assessment = StateAssessment(**kwargs)
        self.state_assessments.append(assessment)
        return assessment

    def create_state_program_assignment(self, **kwargs) -> "StateProgramAssignment":
        program_assignment = StateProgramAssignment(**kwargs)
        self.state_program_assignments.append(program_assignment)
        return program_assignment

    def create_state_supervision_case_type_entry(
        self, **kwargs
    ) -> "StateSupervisionCaseTypeEntry":
        case_type = StateSupervisionCaseTypeEntry(**kwargs)
        self.state_supervision_case_type_entries.append(case_type)
        return case_type

    def create_state_supervision_contact(self, **kwargs) -> "StateSupervisionContact":
        contact = StateSupervisionContact(**kwargs)
        self.state_supervision_contacts.append(contact)
        return contact

    def get_state_supervision_violation_by_id(
        self, state_supervision_violation_id
    ) -> Optional["StateSupervisionViolation"]:
        return next(
            (
                sc
                for sc in self.state_supervision_violation_entries
                if sc.state_supervision_violation_id == state_supervision_violation_id
            ),
            None,
        )

    def get_state_supervision_case_type_entry_by_id(
        self, state_supervision_case_type_entry_id
    ) -> Optional["StateSupervisionCaseTypeEntry"]:
        return next(
            (
                cte
                for cte in self.state_supervision_case_type_entries
                if cte.state_supervision_case_type_entry_id
                == state_supervision_case_type_entry_id
            ),
            None,
        )

    def get_state_supervision_contact_by_id(
        self, state_supervision_contact_id
    ) -> Optional["StateSupervisionContact"]:
        return next(
            (
                sc
                for sc in self.state_supervision_contacts
                if sc.state_supervision_contact_id == state_supervision_contact_id
            ),
            None,
        )

    def prune(self) -> "StateSupervisionPeriod":
        if not self.supervising_officer:
            self.supervising_officer = None

        self.state_supervision_violation_entries = [
            sv for sv in self.state_supervision_violation_entries if sv
        ]
        self.state_assessments = [a for a in self.state_assessments if a]
        self.state_program_assignments = [
            p.prune() for p in self.state_program_assignments if p
        ]
        self.state_supervision_case_type_entries = [
            c for c in self.state_supervision_case_type_entries if c
        ]
        self.state_supervision_contacts = [
            c.prune() for c in self.state_supervision_contacts if c
        ]

        return self

    def sort(self):
        self.state_supervision_violation_entries.sort()
        self.state_assessments.sort()
        self.state_program_assignments.sort()
        self.state_supervision_case_type_entries.sort()
        self.state_supervision_contacts.sort()


class StateSupervisionContact(IngestObject):
    """Class for information about an contact between a person and their supervising agent. Referenced from
    SupervisionPeriod.
    """

    def __init__(
        self,
        state_supervision_contact_id=None,
        contact_date=None,
        contact_reason=None,
        state_code=None,
        contact_type=None,
        contact_method=None,
        location=None,
        resulted_in_arrest=None,
        status=None,
        verified_employment=None,
        contacted_agent=None,
    ):
        self.state_supervision_contact_id: Optional[str] = state_supervision_contact_id
        self.contact_date: Optional[str] = contact_date
        self.contact_reason: Optional[str] = contact_reason
        self.state_code: Optional[str] = state_code
        self.contact_type: Optional[str] = contact_type
        self.contact_method: Optional[str] = contact_method
        self.location: Optional[str] = location
        self.resulted_in_arrest: Optional[str] = resulted_in_arrest
        self.status: Optional[str] = status
        self.verified_employment: Optional[str] = verified_employment

        self.contacted_agent: Optional[StateAgent] = contacted_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, "contacted_agent", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.contacted_agent = StateAgent(**kwargs)
        return self.contacted_agent

    def prune(self) -> "StateSupervisionContact":
        if not self.contacted_agent:
            self.contacted_agent = None
        return self


class StateSupervisionCaseTypeEntry(IngestObject):
    """Class for information about a supervision case type. Referenced from SupervisionPeriod."""

    def __init__(
        self, state_supervision_case_type_entry_id=None, case_type=None, state_code=None
    ):
        self.state_supervision_case_type_entry_id = state_supervision_case_type_entry_id
        self.case_type = case_type
        self.state_code = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateIncarcerationIncident(IngestObject):
    """Class for information about an incident during incarceration. Referenced from IncarcerationPeriod."""

    def __init__(
        self,
        state_incarceration_incident_id=None,
        incident_type=None,
        incident_date=None,
        state_code=None,
        facility=None,
        location_within_facility=None,
        incident_details=None,
        responding_officer=None,
        state_incarceration_incident_outcomes=None,
    ):
        self.state_incarceration_incident_id: Optional[
            str
        ] = state_incarceration_incident_id
        self.incident_type: Optional[str] = incident_type
        self.incident_date: Optional[str] = incident_date
        self.state_code: Optional[str] = state_code
        self.facility: Optional[str] = facility
        self.location_within_facility: Optional[str] = location_within_facility
        self.incident_details: Optional[str] = incident_details

        self.responding_officer: Optional[StateAgent] = responding_officer
        self.state_incarceration_incident_outcomes: List[
            StateIncarcerationIncidentOutcome
        ] = (state_incarceration_incident_outcomes or [])

    def __setattr__(self, name, value):
        restricted_setattr(self, "incarceration_incident_outcomes", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.responding_officer = StateAgent(**kwargs)
        return self.responding_officer

    def create_state_incarceration_incident_outcome(
        self, **kwargs
    ) -> "StateIncarcerationIncidentOutcome":
        outcome = StateIncarcerationIncidentOutcome(**kwargs)
        self.state_incarceration_incident_outcomes.append(outcome)
        return outcome

    def get_state_incarceration_incident_outcome_by_id(
        self, state_incarceration_incident_outcome_id
    ) -> Optional["StateIncarcerationIncidentOutcome"]:
        return next(
            (
                iio
                for iio in self.state_incarceration_incident_outcomes
                if iio.state_incarceration_incident_outcome_id
                == state_incarceration_incident_outcome_id
            ),
            None,
        )

    def prune(self) -> "StateIncarcerationIncident":
        if not self.responding_officer:
            self.responding_officer = None
        self.state_incarceration_incident_outcomes = [
            iio for iio in self.state_incarceration_incident_outcomes if iio
        ]
        return self

    def sort(self):
        self.state_incarceration_incident_outcomes.sort()


class StateIncarcerationIncidentOutcome(IngestObject):
    """Class for information about a incarceration incident outcome. Referenced from StateIncarcerationIncident."""

    def __init__(
        self,
        state_incarceration_incident_outcome_id=None,
        outcome_type=None,
        date_effective=None,
        hearing_date=None,
        report_date=None,
        state_code=None,
        outcome_description=None,
        punishment_length_days=None,
    ):
        self.state_incarceration_incident_outcome_id: Optional[
            str
        ] = state_incarceration_incident_outcome_id
        self.outcome_type: Optional[str] = outcome_type
        self.date_effective: Optional[str] = date_effective
        self.hearing_date: Optional[str] = hearing_date
        self.report_date: Optional[str] = report_date
        self.state_code: Optional[str] = state_code
        self.outcome_description: Optional[str] = outcome_description
        self.punishment_length_days: Optional[str] = punishment_length_days

    def __setattr__(self, name, value):
        restricted_setattr(self, "punishment_length_days", name, value)


class StateParoleDecision(IngestObject):
    """Class for information about a parole decision. Referenced from IncarcerationPeriod."""

    def __init__(
        self,
        state_parole_decision_id=None,
        decision_date=None,
        corrective_action_deadline=None,
        state_code=None,
        county_code=None,
        decision_outcome=None,
        decision_reasoning=None,
        corrective_action=None,
        decision_agents=None,
    ):
        self.state_parole_decision_id: Optional[str] = state_parole_decision_id
        self.decision_date: Optional[str] = decision_date
        self.corrective_action_deadline: Optional[str] = corrective_action_deadline
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code
        self.decision_outcome: Optional[str] = decision_outcome
        self.decision_reasoning: Optional[str] = decision_reasoning
        self.corrective_action: Optional[str] = corrective_action

        self.decision_agents: List[StateAgent] = decision_agents or []

    def __setattr__(self, name, value):
        restricted_setattr(self, "decision_agents", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        decision_agent = StateAgent(**kwargs)
        self.decision_agents.append(decision_agent)
        return decision_agent

    def prune(self) -> "StateParoleDecision":
        self.decision_agents = [da for da in self.decision_agents if da]
        return self

    def sort(self):
        self.decision_agents.sort()


class StateSupervisionViolationTypeEntry(IngestObject):
    """Class for type information about a violation during supervision."""

    def __init__(
        self,
        state_supervision_violation_type_entry_id=None,
        violation_type=None,
        state_code=None,
    ):
        self.state_supervision_violation_type_entry_id: Optional[
            str
        ] = state_supervision_violation_type_entry_id
        self.violation_type: Optional[str] = violation_type
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateSupervisionViolatedConditionEntry(IngestObject):
    """Class for information about a condition on a person's supervision."""

    def __init__(
        self,
        state_supervision_violated_condition_entry_id=None,
        condition=None,
        state_code=None,
    ):
        self.state_supervision_violated_condition_entry_id: Optional[
            str
        ] = state_supervision_violated_condition_entry_id
        self.condition: Optional[str] = condition
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateSupervisionViolation(IngestObject):
    """Class for information about a supervision violation. Referenced from SupervisionViolation."""

    def __init__(
        self,
        state_supervision_violation_id=None,
        violation_date=None,
        state_code=None,
        is_violent=None,
        is_sex_offense=None,
        state_supervision_violation_types=None,
        state_supervision_violated_conditions=None,
        state_supervision_violation_responses=None,
    ):
        self.state_supervision_violation_id: Optional[
            str
        ] = state_supervision_violation_id

        self.violation_date: Optional[str] = violation_date
        self.state_code: Optional[str] = state_code
        self.is_violent: Optional[str] = is_violent
        self.is_sex_offense: Optional[str] = is_sex_offense

        self.state_supervision_violation_types: List[
            StateSupervisionViolationTypeEntry
        ] = (state_supervision_violation_types or [])
        self.state_supervision_violated_conditions: List[
            StateSupervisionViolatedConditionEntry
        ] = (state_supervision_violated_conditions or [])
        self.state_supervision_violation_responses: List[
            StateSupervisionViolationResponse
        ] = (state_supervision_violation_responses or [])

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_supervision_violation_responses", name, value)

    def create_state_supervision_violation_type_entry(
        self, **kwargs
    ) -> "StateSupervisionViolationTypeEntry":
        violation_type = StateSupervisionViolationTypeEntry(**kwargs)
        self.state_supervision_violation_types.append(violation_type)
        return violation_type

    def get_state_supervision_violation_type_entry_by_id(
        self, state_supervision_violation_type_entry_id
    ) -> Optional["StateSupervisionViolationTypeEntry"]:
        return next(
            (
                svt
                for svt in self.state_supervision_violation_types
                if svt.state_supervision_violation_type_entry_id
                == state_supervision_violation_type_entry_id
            ),
            None,
        )

    def create_state_supervision_violated_condition_entry(
        self, **kwargs
    ) -> "StateSupervisionViolatedConditionEntry":
        condition = StateSupervisionViolatedConditionEntry(**kwargs)
        self.state_supervision_violated_conditions.append(condition)
        return condition

    def get_state_supervision_violated_condition_entry_by_id(
        self, state_supervision_violated_condition_entry_id
    ) -> Optional["StateSupervisionViolatedConditionEntry"]:
        return next(
            (
                svc
                for svc in self.state_supervision_violated_conditions
                if svc.state_supervision_violated_condition_entry_id
                == state_supervision_violated_condition_entry_id
            ),
            None,
        )

    def create_state_supervision_violation_response(
        self, **kwargs
    ) -> "StateSupervisionViolationResponse":
        violation_response = StateSupervisionViolationResponse(**kwargs)
        self.state_supervision_violation_responses.append(violation_response)
        return violation_response

    def get_state_supervision_violation_response_by_id(
        self, state_supervision_violation_response_id
    ) -> Optional["StateSupervisionViolationResponse"]:
        return next(
            (
                vr
                for vr in self.state_supervision_violation_responses
                if vr.state_supervision_violation_response_id
                == state_supervision_violation_response_id
            ),
            None,
        )

    def prune(self) -> "StateSupervisionViolation":
        self.state_supervision_violation_responses = [
            vr for vr in self.state_supervision_violation_responses if vr
        ]

        return self

    def sort(self):
        self.state_supervision_violation_responses.sort()


class StateSupervisionViolationResponseDecisionEntry(IngestObject):
    """Class for information about a condition on a person's supervision."""

    def __init__(
        self,
        state_supervision_violation_response_decision_entry_id=None,
        decision=None,
        state_code=None,
    ):
        self.state_supervision_violation_response_decision_entry_id: Optional[
            str
        ] = state_supervision_violation_response_decision_entry_id
        self.decision: Optional[str] = decision
        self.state_code: Optional[str] = state_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "state_code", name, value)


class StateSupervisionViolationResponse(IngestObject):
    """Class for information about a supervision violation response. Referenced from
    StateSupervisionViolationResponse.
    """

    def __init__(
        self,
        state_supervision_violation_response_id=None,
        response_type=None,
        response_subtype=None,
        response_date=None,
        state_code=None,
        deciding_body_type=None,
        is_draft=None,
        supervision_violation_response_decisions=None,
        decision_agents=None,
    ):
        self.state_supervision_violation_response_id: Optional[
            str
        ] = state_supervision_violation_response_id
        self.response_type: Optional[str] = response_type
        self.response_subtype: Optional[str] = response_subtype
        self.response_date: Optional[str] = response_date
        self.state_code: Optional[str] = state_code
        self.deciding_body_type: Optional[str] = deciding_body_type
        self.is_draft: Optional[str] = is_draft
        self.state_supervision_violation_response_decisions: List[
            StateSupervisionViolationResponseDecisionEntry
        ] = (supervision_violation_response_decisions or [])
        self.decision_agents: List[StateAgent] = decision_agents or []

    def __setattr__(self, name, value):
        restricted_setattr(self, "decision_agents", name, value)

    def create_state_supervision_violation_response_decision_entry(
        self, **kwargs
    ) -> "StateSupervisionViolationResponseDecisionEntry":
        decision = StateSupervisionViolationResponseDecisionEntry(**kwargs)
        self.state_supervision_violation_response_decisions.append(decision)
        return decision

    def get_state_supervision_violation_response_decision_entry_by_id(
        self, state_supervision_violation_response_decision_entry_id
    ) -> Optional["StateSupervisionViolationResponseDecisionEntry"]:
        matching_svrdts = (
            svrdt
            for svrdt in self.state_supervision_violation_response_decisions
            if svrdt.state_supervision_violation_response_decision_entry_id
            == state_supervision_violation_response_decision_entry_id
        )

        return next(matching_svrdts, None)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        decision_agent = StateAgent(**kwargs)
        self.decision_agents.append(decision_agent)
        return decision_agent

    def prune(self) -> "StateSupervisionViolationResponse":
        self.decision_agents = [da for da in self.decision_agents if da]
        return self

    def sort(self):
        self.decision_agents.sort()


class StateAgent(IngestObject):
    """Class for information about some agent working within the justice system. Referenced from several state-level
    entities.
    """

    def __init__(
        self,
        state_agent_id=None,
        agent_type=None,
        state_code=None,
        full_name=None,
        surname=None,
        given_names=None,
        middle_names=None,
        name_suffix=None,
    ):
        self.state_agent_id: Optional[str] = state_agent_id
        self.agent_type: Optional[str] = agent_type
        self.state_code: Optional[str] = state_code
        self.full_name: Optional[str] = full_name
        self.surname: Optional[str] = surname
        self.given_names: Optional[str] = given_names
        self.middle_names: Optional[str] = middle_names
        self.name_suffix: Optional[str] = name_suffix

    def __setattr__(self, name, value):
        restricted_setattr(self, "name_suffix", name, value)


class StateProgramAssignment(IngestObject):
    """Class for information about a program assignment. Referenced from StatePerson, StateIncarcerationPeriod, and
    StateSupervisionPeriod.
    """

    def __init__(
        self,
        state_program_assignment_id=None,
        participation_status=None,
        referral_date=None,
        start_date=None,
        discharge_date=None,
        state_code=None,
        program_id=None,
        program_location_id=None,
        discharge_reason=None,
        referral_metadata=None,
        referring_agent=None,
    ):
        self.state_program_assignment_id: Optional[str] = state_program_assignment_id
        self.participation_status: Optional[str] = participation_status
        self.referral_date: Optional[str] = referral_date
        self.start_date: Optional[str] = start_date
        self.discharge_date: Optional[str] = discharge_date
        self.state_code: Optional[str] = state_code
        self.program_id: Optional[str] = program_id
        self.program_location_id: Optional[str] = program_location_id
        self.discharge_reason: Optional[str] = discharge_reason
        self.referral_metadata: Optional[str] = referral_metadata

        self.referring_agent: Optional[StateAgent] = referring_agent

    def __setattr__(self, name, value):
        restricted_setattr(self, "referring_agent", name, value)

    def create_state_agent(self, **kwargs) -> "StateAgent":
        self.referring_agent = StateAgent(**kwargs)
        return self.referring_agent

    def prune(self) -> "StateProgramAssignment":
        if not self.referring_agent:
            self.referring_agent = None
        return self


class StateEarlyDischarge(IngestObject):
    """Class for information about an Early Discharge. Referenced from StateIncarcerationSentence and
    StateSupervisionSentence.
    """

    def __init__(
        self,
        state_early_discharge_id=None,
        request_date=None,
        decision_date=None,
        decision=None,
        decision_status=None,
        deciding_body_type=None,
        requesting_body_type=None,
        state_code=None,
        county_code=None,
    ):
        self.state_early_discharge_id: Optional[str] = state_early_discharge_id
        self.request_date: Optional[str] = request_date
        self.decision_date: Optional[str] = decision_date
        self.decision: Optional[str] = decision
        self.decision_status: Optional[str] = decision_status
        self.deciding_body_type: Optional[str] = deciding_body_type
        self.requesting_body_type: Optional[str] = requesting_body_type
        self.state_code: Optional[str] = state_code
        self.county_code: Optional[str] = county_code

    def __setattr__(self, name, value):
        restricted_setattr(self, "county_code", name, value)


def eq(self, other, exclude=None):
    if other is None:
        return False
    if exclude is None:
        return self.__dict__ == other.__dict__

    return _without_exclusions(self, exclude) == _without_exclusions(other, exclude)


def _without_exclusions(obj, exclude=None):
    if exclude is None:
        exclude = []
    return {k: v for k, v in obj.__dict__.items() if k not in exclude}


def to_bool(obj, exclude=None):
    if exclude is None:
        exclude = []
    return any(
        any(v) if isinstance(v, list) else v
        for k, v in obj.__dict__.items()
        if k not in exclude
    )


def to_string(obj, exclude=None):
    if exclude is None:
        exclude = []
    out = [obj.__class__.__name__ + ":"]
    for key, val in vars(obj).items():
        if key in exclude:
            continue
        if isinstance(val, list):
            for index, elem in enumerate(val):
                out += "{}[{}]: {}".format(key, index, elem).split("\n")
        elif val is not None:
            out += "{}: {}".format(key, val).split("\n")
    return "\n   ".join(out)


def to_repr(obj, exclude=None):
    if exclude is None:
        exclude = []
    args = []
    for key, val in vars(obj).items():
        if key in exclude:
            continue
        if val is not None:
            args.append("{}={}".format(key, repr(val)))

    return "{}({})".format(obj.__class__.__name__, ", ".join(args))


def restricted_setattr(self, last_field, name, value):
    if isinstance(value, str) and (value == "" or value.isspace()):
        value = None
    if hasattr(self, last_field) and not hasattr(self, name):
        raise AttributeError("No field {} in object {}".format(name, type(self)))
    self.__dict__[name] = value
