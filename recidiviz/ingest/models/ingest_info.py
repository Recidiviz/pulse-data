# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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


class IngestInfo(object):
    """Class for information about multiple people."""

    def __init__(self):
        self.person = []  # type: List[Person]

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def create_person(self, **kwargs):
        person = _Person(**kwargs)
        self.person.append(person)
        return person

    def get_recent_person(self):
        if self.person:
            return self.person[-1]
        return None


class _Person(object):
    """Class for information about a person.
    Referenced from IngestInfo.
    """
    def __init__(self, person_id=None, surname=None, given_names=None,
                 birthdate=None, status=None, sex=None, age=None,
                 race=None, ethnicity=None, place_of_residence=None,
                 bookings=None):
        self.person_id = person_id # type: str
        self.surname = surname  # type: str
        self.given_names = given_names  # type: str
        self.birthdate = birthdate # type: datetime
        self.status = status
        self.sex = sex
        self.age = age  # type: int
        self.race = race
        self.ethnicity = ethnicity
        self.place_of_residence = place_of_residence  # type: str

        self.booking = bookings or []  # type: List[Booking]

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def create_booking(self, **kwargs):
        booking = _Booking(**kwargs)
        self.booking.append(booking)
        return booking

    def get_recent_booking(self):
        if self.booking:
            return self.booking[-1]
        return None


class _Booking(object):
    """Class for information about a booking.
    Referenced from Person.
    """
    def __init__(self, booking_id=None, admission_date=None,
                 projected_release_date=None, release_date=None,
                 release_reason=None,
                 custody_status=None,
                 jurisdiction_status=None, hold=None,
                 facility=None, classification=None,
                 arrest=None, charges=None):
        self.booking_id = booking_id  # type: str
        self.admission_date = admission_date  # type: datetime
        self.projected_release_date = projected_release_date  # type: datetime
        self.release_date = release_date  # type: datetime
        self.release_reason = release_reason
        self.custody_status = custody_status
        self.jurisdiction_status = jurisdiction_status
        self.hold = hold  # type: str
        self.facility = facility  # type: str
        self.classification = classification

        self.arrest = arrest  # type: Arrest
        self.charge = charges or []  # type: List[Charge]

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def create_arrest(self, **kwargs):
        self.arrest = _Arrest(**kwargs)
        return self.arrest

    def create_charge(self, **kwargs):
        charge = _Charge(**kwargs)
        self.charge.append(charge)
        return charge

    def get_recent_charge(self):
        if self.charge:
            return self.charge[-1]
        return None

    def get_recent_arrest(self):
        return self.arrest


class _Arrest(object):
    """Class for information about an arrest.
    Referenced from Booking.
    """
    def __init__(self, date=None, location=None, officer_name=None,
                 officer_id=None):
        self.date = date  # type: datetime
        self.location = location  # type: str
        self.officer_name = officer_name  # type: str
        self.officer_id = officer_id  # type: str

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class _Charge(object):
    """Class for information about a charge.
    Referenced from Booking.
    """
    def __init__(self, offense_date=None, statute=None, name=None,
                 attempted=None, degree=None,
                 charge_class=None, level=None, fee=None,
                 charging_entity=None, charge_status=None,
                 number_of_counts=None, court_type=None,
                 case_number=None, next_court_date=None, judge_name=None,
                 bond=None, sentence=None):
        self.offense_date = offense_date  # type: datetime
        self.statute = statute  # type: str
        self.name = name  # type: str
        self.attempted = attempted  # type: bool
        self.degree = degree
        self.charge_class = charge_class
        self.level = level  # type: str
        self.fee = fee  # type: decimal
        self.charging_entity = charging_entity  # type: str
        self.charge_status = charge_status
        self.number_of_counts = number_of_counts  # type: int
        self.court_type = court_type
        self.case_number = case_number  # type: str
        self.next_court_date = next_court_date  # type: datetime
        self.judge_name = judge_name  # type: str

        self.bond = bond  # type: Bond
        self.sentence = sentence  # type: Sentence

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def create_bond(self, **kwargs):
        self.bond = _Bond(**kwargs)
        return self.bond

    def create_sentence(self, **kwargs):
        self.sentence = _Sentence(**kwargs)
        return self.sentence

    def get_recent_bond(self):
        return self.bond

    def get_recent_sentence(self):
        return self.sentence


class _Bond(object):
    """Class for information about a bond.
    Referenced from Charge.
    """
    def __init__(self, bond_id=None, amount=None, bond_type=None,
                 status=None):
        self.bond_id = bond_id  # type: str
        self.amount = amount  # type: decimal
        self.bond_type = bond_type
        self.status = status

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class _Sentence(object):
    """Class for information about a sentence.
    Referenced from Charge.
    """
    def __init__(self, date_imposed=None, min_length=None, max_length=None,
                 is_life=None, is_probation=None, is_suspended=None, fine=None,
                 parole_possible=None, post_release_supervision_length=None):
        self.date_imposed = date_imposed  # type: datetime
        self.min_length = min_length  # type: timedelta
        self.max_length = max_length  # type: timedelta
        self.is_life = is_life  # type: bool
        self.is_probation = is_probation  # type: bool
        self.is_suspended = is_suspended  # type: bool
        self.fine = fine  # type: decimal
        self.parole_possible = parole_possible  # type: bool

        # type: timedelta
        self.post_release_supervision_length = post_release_supervision_length
