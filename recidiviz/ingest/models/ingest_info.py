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
from typing import List, Optional, Dict, Sequence

HIERARCHY_MAP: Dict[str, Sequence[str]] = {
    'person': (),
    'booking': ('person',),
    'arrest': ('person', 'booking'),
    'charge': ('person', 'booking'),
    'hold': ('person', 'booking'),
    'bond': ('person', 'booking', 'charge'),
    'sentence': ('person', 'booking', 'charge')
}

PLURALS = {'person': 'people', 'booking': 'bookings', 'charge': 'charges',
           'hold': 'holds'}


class IngestObject:
    """Abstract base class for all the objects contained by IngestInfo"""

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

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

    def __init__(self, people=None):
        self.people: List[Person] = people or []

    def __setattr__(self, name, value):
        restricted_setattr(self, 'people', name, value)

    def create_person(self, **kwargs) -> 'Person':
        person = Person(**kwargs)
        self.people.append(person)
        return person

    def get_recent_person(self) -> Optional['Person']:
        if self.people:
            return self.people[-1]
        return None

    def prune(self) -> 'IngestInfo':
        self.people = [person.prune() for person in self.people if person]
        return self

    def sort(self):
        for person in self.people:
            person.sort()
        self.people.sort()

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
        self.bookings = [booking.prune() \
                         for booking in self.bookings if booking]
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


def to_bool(obj):
    return any(any(v) if isinstance(v, list) else v
               for v in obj.__dict__.values())


def to_string(obj):
    out = [obj.__class__.__name__ + ":"]
    for key, val in vars(obj).items():
        if isinstance(val, list):
            for index, elem in enumerate(val):
                out += '{}[{}]: {}'.format(key, index, elem).split('\n')
        elif val:
            out += '{}: {}'.format(key, val).split('\n')
    return '\n   '.join(out)


def to_repr(obj):
    args = []
    for key, val in vars(obj).items():
        if val:
            args.append('{}={}'.format(key, repr(val)))

    return '{}({})'.format(obj.__class__.__name__, ', '.join(args))


def restricted_setattr(self, last_field, name, value):
    if isinstance(value, str) and (value == '' or value.isspace()):
        value = None
    if hasattr(self, last_field) and not hasattr(self, name):
        raise AttributeError('No field {} in object {}'.format(name,
                                                               type(self)))
    self.__dict__[name] = value
