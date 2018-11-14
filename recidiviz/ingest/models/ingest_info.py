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
    def __init__(self):
        self.person_id = None # type: str
        self.surname = None  # type: str
        self.given_names = None  # type: str
        self.birthdate = None  # type: datetime
        self.sex = None
        self.age = None  # type: int
        self.race = Race.unknown
        self.ethnicity = Ethnicity.unknown
        self.place_of_residence = None  # type: str

        self.bookings = []  # type: List[Booking]

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
    def __init__(self):
        self.booking_id = None # type: str
        self.admission_date = None  # type: datetime
        self.projected_release_date = None  # type: datetime
        self.release_date = None  # type: datetime
        self.release_reason = ReleaseReason.unknown
        self.custody_status = CustodyStatus.unknown
        self.jurisdiction_status = JurisdictionStatus.unknown
        self.hold = None  # type: str
        self.facility = None  # type: str
        self.classification = Classification.unknown

        self.arrest = None  # type: Arrest
        self.charges = []  # type: List[Charge]

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
    def __init__(self):
        self.date = None  # type: datetime
        self.location = None  # type: str
        self.officer_name = None  # type: str
        self.officer_id = None  # type: str

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class _Charge(object):
    """Class for information about a charge.
    Referenced from Booking.
    """
    def __init__(self):
        self.offense_date = None  # type: datetime
        self.statute = None  # type: str
        self.name = None  # type: str
        self.attempted = None  # type: bool
        self.degree = Degree.unknown
        self.charge_class = ChargeClass.unknown
        self.level = None  # type: str
        self.fee = None  # type: decimal
        self.charging_entity = None  # type: str
        self.charge_status = ChargeStatus.unknown
        self.number_of_counts = None  # type: int
        self.court_type = CourtType.unknown
        self.case_number = None  # type: str
        self.next_court_date = None  # type: datetime
        self.judge_name = None  # type: str

        self.bond = None  # type: Bond
        self.sentence = None  # type: Sentence

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
    def __init__(self):
        self.bond_id = None  # type: str
        self.amount = None  # type: decimal
        self.type = BondType.unknown
        self.status = BondStatus.unknown

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class _Sentence(object):
    """Class for information about a sentence.
    Referenced from Charge.
    """
    def __init__(self):
        self.date_imposed = None  # type: datetime
        self.min_length = None  # type: timedelta
        self.max_length = None  # type: timedelta
        self.is_life = None  # type: bool
        self.is_probation = None  # type: bool
        self.is_suspended = None  # type: bool
        self.fine = None  # type: decimal
        self.parole_possible = None  # type: bool
        self.post_release_supervision_length = None  # type: timedelta

    def __eq__(self, other):
        return self.__dict__ == other.__dict__
