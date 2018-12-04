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
# ============================================================================
"""Converts scraped IngestInfo data to the database schema format."""

import datetime
import dateparser
from recidiviz.persistence.database import schema

def _normalize(s):
    if s is None:
        raise ValueError('function normalize should never be called with None')
    if s == '' or s.isspace():
        return ''
    return ' '.join(s.split()).upper()


def parse_date_or_error(date_string):
    if date_string == '' or date_string.isspace():
        return None
    parsed_date = dateparser.parse(date_string)
    if parsed_date is None:
        raise ValueError('cannot parse date: %s' % date_string)
    return parsed_date


def calculate_birthdate_from_age(age):
    """We estimate a person's birthdate by subtracting their age from the
    current year and setting their birthdate to the first day of that year.
    """
    if age == '' or age.isspace():
        return None
    try:
        birth_year = datetime.date.today().year - int(age)
        return datetime.date(year=birth_year, month=1, day=1)
    except:
        raise ValueError('cannot parse age: %s' % age)


def time_string_to_days(time_string):
    if time_string == '' or time_string.isspace():
        return 0
    try:
        return int(time_string)
    except:
        # dateparser.parse interprets the string '1 YEAR 2 DAYS' to mean the
        # datetime 1 year and 2 days ago.
        date_ago = dateparser.parse(time_string)
        if date_ago:
            return (datetime.datetime.now() - date_ago).days
        raise ValueError('cannot parse time duration: %s' % time_string)

def split_full_name(full_name):
    """Splits a full name into given and surnames.

    Args:
        full_name: (str)
    Returns:
        a pair of strings (surname, given_names)
    """
    if full_name == '' or full_name.isspace():
        return None
    full_name = _normalize(full_name)
    if ',' in full_name:
        names = full_name.split(',')
        if len(names) == 2 and all(names):
            return tuple(names)
    names = full_name.split()
    if len(names) >= 2:
        return names[-1], ' '.join(names[:-1])
    raise ValueError('cannot parse full name: %s' % full_name)


def parse_dollar_amount(dollar_string):
    if dollar_string == '' or dollar_string.isspace() or \
        'NO' in dollar_string.upper():
        return 0
    try:
        clean_string = ''.join(
            dollar_string.replace('$', '').replace(',', '').split())
        return int(float(clean_string))
    except:
        raise ValueError('cannot parse dollar value: %s' % dollar_string)


def verify_is_bool(possible_bool):
    if isinstance(possible_bool, bool):
        return possible_bool
    raise ValueError('expected bool but got %s: %s' %
                     (type(possible_bool), possible_bool))

def convert_person(ingest_person):
    """Converts an IngestInfo person into a Schema person.

    Args:
        ingest_person: (recidiviz.ingest.models.ingest_info._Person)

    Returns:
        (recidiviz.persistence.database.schema.Person)
    """
    person = schema.Person()

    if ingest_person.person_id is not None:
        person.scraped_person_id = _normalize(ingest_person.person_id)

    if ingest_person.surname or ingest_person.given_names:
        if ingest_person.surname is not None:
            person.surname = _normalize(ingest_person.surname)
        if ingest_person.given_names is not None:
            person.given_names = _normalize(ingest_person.given_names)
    elif ingest_person.full_name is not None:
        last, first = split_full_name(ingest_person.full_name)
        person.surname = _normalize(last)
        person.given_names = _normalize(first)

    if ingest_person.birthdate is not None:
        person.birthdate = parse_date_or_error(ingest_person.birthdate)
        person.birthdate_inferred_from_age = False
    elif ingest_person.age is not None:
        person.birthdate = calculate_birthdate_from_age(ingest_person.age)
        person.birthdate_inferred_from_age = True

    if ingest_person.gender is not None:
        # TODO(215): convert to enum
        person.gender = _normalize(ingest_person.gender)

    if ingest_person.race is not None:
        # TODO(215): convert to enum
        person.race = _normalize(ingest_person.race)

    if ingest_person.ethnicity is not None:
        # TODO(215): convert to enum
        person.ethnicity = _normalize(ingest_person.ethnicity)

    if ingest_person.place_of_residence is not None:
        person.place_of_residence = _normalize(ingest_person.place_of_residence)

    if ingest_person.booking is not None:
        person.bookings = [convert_booking(b) for b in ingest_person.booking]

    return person

def convert_booking(ingest_booking):
    """Converts an IngestInfo booking into a Schema booking.

    Args:
        ingest_person: (recidiviz.ingest.models.ingest_info._Booking)

    Returns:
        (recidiviz.persistence.database.schema.Booking)
    """
    booking = schema.Booking()

    if ingest_booking.booking_id is not None:
        booking.scraped_booking_id = _normalize(ingest_booking.booking_id)

    if ingest_booking.admission_date is not None:
        booking.admission_date = \
            parse_date_or_error(ingest_booking.admission_date)

    if ingest_booking.release_date is not None:
        booking.release_date = parse_date_or_error(ingest_booking.release_date)
        booking.release_date_inferred = False

    if ingest_booking.projected_release_date is not None:
        booking.projected_release_date = \
            parse_date_or_error(ingest_booking.projected_release_date)

    if ingest_booking.release_reason is not None:
        # TODO(215): convert to enum
        booking.release_reason = _normalize(ingest_booking.release_reason)

    if ingest_booking.custody_status is not None:
        # TODO(215): convert to enum
        booking.custody_status = _normalize(ingest_booking.custody_status)

    if ingest_booking.hold is not None:
        # TODO: decide if this should be a list (of objects? strings?) instead
        booking.hold = _normalize(ingest_booking.hold)
        # TODO: decide under what conditions we can set this to False
        booking.held_for_other_jurisdiction = True

    if ingest_booking.facility is not None:
        booking.facility = _normalize(ingest_booking.facility)

    if ingest_booking.classification is not None:
        # TODO(215): convert to enum
        booking.classification = _normalize(ingest_booking.classification)

    if ingest_booking.arrest:
        booking.arrest = convert_arrest(ingest_booking.arrest)

    if ingest_booking.charge:
        booking.charges = [convert_charge(c) for c in ingest_booking.charge]

    if ingest_booking.total_bond_amount is not None:
        # If there is a total bond amount listed, but the booking does not have
        # any bond amounts, update the bonds to that amount or add a new bond
        # with the total amount to each charge.
        bond_amount = parse_dollar_amount(ingest_booking.total_bond_amount)
        total_bond = schema.Bond(amount=bond_amount)
        if not booking.charges:
            booking.charges = [schema.Charge(bond=total_bond)]
        elif not any(charge.bond and charge.bond.amount
                     for charge in booking.charges):
            for charge in booking.charges:
                if charge.bond:
                    charge.bond.amount = bond_amount
                else:
                    charge.bond = total_bond

    return booking


def convert_arrest(ingest_arrest):
    """Converts an IngestInfo arrest into a Schema arrest.

    Args:
        ingest_person: (recidiviz.ingest.models.ingest_info._Arrest)

    Returns:
        (recidiviz.persistence.database.schema.Arrest)
    """
    arrest = schema.Arrest()

    if ingest_arrest.date is not None:
        arrest.date = parse_date_or_error(ingest_arrest.date)

    if ingest_arrest.location is not None:
        arrest.location = _normalize(ingest_arrest.location)

    if ingest_arrest.agency is not None:
        arrest.agency = _normalize(ingest_arrest.agency)

    if ingest_arrest.officer_name is not None:
        arrest.officer_name = _normalize(ingest_arrest.officer_name)

    if ingest_arrest.officer_id is not None:
        arrest.officer_id = _normalize(ingest_arrest.officer_id)

    if ingest_arrest.agency is not None:
        arrest.agency = _normalize(ingest_arrest.agency)

    return arrest


def convert_charge(ingest_charge):
    """Converts an IngestInfo charge into a Schema charge.

    Args:
        ingest_person: (recidiviz.ingest.models.ingest_info._Charge)

    Returns:
        (recidiviz.persistence.database.schema.Charge)
    """
    charge = schema.Charge()

    if ingest_charge.offense_date is not None:
        charge.offense_date = parse_date_or_error(ingest_charge.offense_date)

    if ingest_charge.statute is not None:
        charge.statute = _normalize(ingest_charge.statute)
        # TODO(215): charge.offense_code = code_to_BJS(ingest_charge.statute)

    if ingest_charge.name is not None:
        charge.name = _normalize(ingest_charge.name)

    if ingest_charge.attempted is not None:
        charge.attempted = verify_is_bool(ingest_charge.attempted)

    if ingest_charge.degree is not None:
        # TODO(215): convert to enum
        charge.degree = _normalize(ingest_charge.degree)

    if ingest_charge.charge_class is not None:
        # TODO(215): convert to enum
        charge.charge_class = _normalize(ingest_charge.charge_class)

    if ingest_charge.level is not None:
        charge.level = _normalize(ingest_charge.level)

    if ingest_charge.fee is not None:
        charge.fee = parse_dollar_amount(ingest_charge.fee)

    if ingest_charge.charging_entity is not None:
        charge.charging_entity = _normalize(ingest_charge.charging_entity)

    if ingest_charge.charge_status is not None:
        # TODO(215): convert to enum
        charge.status = _normalize(ingest_charge.charge_status)

    if ingest_charge.number_of_counts is not None:
        charge.number_of_counts = int(ingest_charge.number_of_counts)

    if ingest_charge.court_type is not None:
        # TODO(215): convert to enum
        charge.court_type = _normalize(ingest_charge.court_type)

    if ingest_charge.case_number is not None:
        charge.case_number = _normalize(ingest_charge.case_number)

    if ingest_charge.next_court_date is not None:
        charge.next_court_date = \
            parse_date_or_error(ingest_charge.next_court_date)

    if ingest_charge.judge_name is not None:
        charge.judge_name = _normalize(ingest_charge.judge_name)

    if ingest_charge.bond is not None:
        charge.bond = convert_bond(ingest_charge.bond)

    if ingest_charge.sentence is not None:
        charge.sentence = convert_sentence(ingest_charge.sentence)

    return charge


def convert_bond(ingest_bond):
    """Converts an IngestInfo bond into a Schema bond.

    Args:
        ingest_bond: (recidiviz.ingest.models.ingest_info._Bond)

    Returns:
        (recidiviz.persistence.database.schema.Bond)
    """

    bond = schema.Bond()

    if ingest_bond.bond_id is not None:
        bond.scraped_bond_id = _normalize(ingest_bond.bond_id)

    if ingest_bond.amount is not None:
        bond.amount = parse_dollar_amount(ingest_bond.amount)

    if ingest_bond.bond_type is not None:
        # TODO(215): convert to enum
        bond.type = _normalize(ingest_bond.bond_type)

    if ingest_bond.status is not None:
        # TODO(215): convert to enum
        bond.status = _normalize(ingest_bond.status)

    return bond


def convert_sentence(ingest_sentence):
    """Converts an IngestInfo sentence into a Schema sentence.

    Args:
        ingest_person: (recidiviz.ingest.models.ingest_info._Sentence)

    Returns:
        (recidiviz.persistence.database.schema.Sentence)
    """
    sentence = schema.Sentence()

    if ingest_sentence.date_imposed is not None:
        sentence.date_imposed = \
            parse_date_or_error(ingest_sentence.date_imposed)

    if ingest_sentence.min_length is not None:
        sentence.min_length_days = \
            time_string_to_days(ingest_sentence.min_length)

    if ingest_sentence.max_length is not None:
        sentence.max_length_days = \
            time_string_to_days(ingest_sentence.max_length)

    if ingest_sentence.is_life is not None:
        sentence.is_life = verify_is_bool(ingest_sentence.is_life)

    if ingest_sentence.is_probation is not None:
        sentence.is_probation = verify_is_bool(ingest_sentence.is_probation)

    if ingest_sentence.is_suspended is not None:
        sentence.is_suspended = verify_is_bool(ingest_sentence.is_suspended)

    if ingest_sentence.fine is not None:
        sentence.fine = parse_dollar_amount(ingest_sentence.fine)

    if ingest_sentence.parole_possible is not None:
        sentence.parole_possible = \
            verify_is_bool(ingest_sentence.parole_possible)

    if ingest_sentence.post_release_supervision_length is not None:
        sentence.post_release_supervision_length_days = \
            time_string_to_days(
                ingest_sentence.post_release_supervision_length)

    return sentence
