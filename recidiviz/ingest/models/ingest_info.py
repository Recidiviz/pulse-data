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

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.booking import Classification
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.booking import JurisdictionStatus
from recidiviz.common.constants.booking import ReleaseReason
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.charge import CourtType
from recidiviz.common.constants.charge import Degree
from recidiviz.common.constants.person import Ethnicity
from recidiviz.common.constants.person import Race


class IngestInfo(object):
    """Class for information about multiple people."""

    def __init__(self):
        self.people = []  # type: List[Person]

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def create_person(self):
        person = _Person()
        self.people.append(person)
        return person


class _Person(object):
    """Class for information about a person.
    Referenced from IngestInfo.
    """
    def __init__(self, person_id=None, surname=None, given_names=None,
                 birthdate=None, sex=None, age=None, race=Race.unknown,
                 ethnicity=Ethnicity.unknown, place_of_residence=None,
                 bookings=None):
        self.person_id = person_id  # type: str
        self.surname = surname  # type: str
        self.given_names = given_names  # type: str
        self.birthdate = birthdate  # type: datetime
        self.sex = sex
        self.age = age  # type: int
        self.race = race
        self.ethnicity = ethnicity
        self.place_of_residence = place_of_residence  # type: str

        # type: List[Booking]
        self.bookings = [] if bookings is None else bookings

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def create_booking(self):
        booking = _Booking()
        self.bookings.append(booking)
        return booking


class _Booking(object):
    """Class for information about a booking.
    Referenced from Person.
    """
    def __init__(self, booking_id=None, admission_date=None,
                 projected_release_date=None, release_date=None,
                 release_reason=ReleaseReason.unknown,
                 custody_status=CustodyStatus.unknown,
                 jurisdiction_status=JurisdictionStatus.unknown, hold=None,
                 facility=None, classification=Classification.unknown,
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
        self.charges = [] if charges is None else charges  # type: List[Charge]

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def create_arrest(self):
        self.arrest = _Arrest()
        return self.arrest

    def create_charge(self):
        charge = _Charge()
        self.charges.append(charge)
        return charge


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
                 attempted=None, degree=Degree.unknown,
                 charge_class=ChargeClass.unknown, level=None, fee=None,
                 charging_entity=None, charge_status=ChargeStatus.unknown,
                 number_of_counts=None, court_type=CourtType.unknown,
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

    def create_bond(self):
        self.bond = _Bond()
        return self.bond

    def create_sentence(self):
        self.sentence = _Sentence()
        return self.sentence


class _Bond(object):
    """Class for information about a bond.
    Referenced from Charge.
    """
    def __init__(self, bond_id=None, amount=None, bond_type=BondType.unknown,
                 status=BondStatus.unknown):
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
