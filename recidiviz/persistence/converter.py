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

import more_itertools

from recidiviz.common.constants.bond import BondType, BondStatus
from recidiviz.common.constants.booking import ReleaseReason, CustodyStatus, \
    Classification
from recidiviz.common.constants.charge import ChargeDegree, ChargeClass, \
    ChargeStatus, CourtType
from recidiviz.common.constants.person import Gender, Race, Ethnicity
from recidiviz.persistence import converter_utils, entities


def convert(ingest_info):
    """Convert an IngestInfo proto into a persistence layer object.

    If a validation error happens, raises a ValueError and prints out all
    validation errors for this ingest_info object.

    Returns:
        A list of entities.Person based on the provided ingest_info proto
    """
    return _Converter(ingest_info).convert()


# TODO(302): Look into the feasibility of breaking up this converter and writing
#  more comprehensive tests
class _Converter(object):
    """Converts between ingest_info objects and persistence layer objects."""

    def __init__(self, ingest_info):
        self.ingest_info = ingest_info
        self.conversion_errors = set()

    def convert(self):
        people = [self._convert_person(p) for p in self.ingest_info.people]

        if self.conversion_errors:
            raise ValueError(_build_error_string(self.conversion_errors))

        return people

    def _convert_person(self, ingest_person):
        """Converts an ingest_info proto Person to a persistence object."""
        person = entities.Person()

        if ingest_person.HasField('person_id'):
            person.external_id = self._convert(converter_utils.normalize,
                                               ingest_person.person_id)

        if (ingest_person.HasField('surname') or ingest_person.HasField(
                'given_names')) and ingest_person.HasField('full_name'):
            self.conversion_errors.add(
                ValueError('Cannot have full_name and surname/given_names'))
        elif ingest_person.HasField('full_name'):
            last, first = self._convert(converter_utils.split_full_name,
                                        ingest_person.full_name)
            person.surname = self._convert(converter_utils.normalize, last)
            person.given_names = self._convert(converter_utils.normalize, first)
        else:
            if ingest_person.HasField('surname'):
                person.surname = self._convert(converter_utils.normalize,
                                               ingest_person.surname)
            if ingest_person.HasField('given_names'):
                person.given_names = self._convert(converter_utils.normalize,
                                                   ingest_person.given_names)

        if ingest_person.HasField('birthdate'):
            person.birthdate = self._convert(
                converter_utils.parse_date_or_error,
                ingest_person.birthdate)
            person.birthdate_inferred_from_age = False
        elif ingest_person.HasField('age'):
            person.birthdate = self._convert(
                converter_utils.calculate_birthdate_from_age,
                ingest_person.age)
            person.birthdate_inferred_from_age = True

        if ingest_person.HasField('gender'):
            person.gender = self._convert(Gender.from_str, ingest_person.gender)

        if ingest_person.HasField('race') \
                and not ingest_person.HasField('ethnicity') \
                and converter_utils.race_is_actually_ethnicity(ingest_person):
            person.ethnicity = self._convert(Ethnicity.from_str,
                                             ingest_person.race)
        elif ingest_person.HasField('race'):
            person.race = self._convert(Race.from_str, ingest_person.race)

        if ingest_person.HasField('ethnicity'):
            person.ethnicity = self._convert(Ethnicity.from_str,
                                             ingest_person.ethnicity)

        if ingest_person.HasField('place_of_residence'):
            person.place_of_residence = self._convert(
                converter_utils.normalize, ingest_person.place_of_residence)

        person.bookings = [self._convert_booking(booking_id) for booking_id in
                           ingest_person.booking_ids]

        return person

    def _convert_booking(self, booking_id):
        """Converts an ingest_info proto Booking to a persistence object."""
        ingest_booking = more_itertools.one(
            booking for booking in self.ingest_info.bookings if
            booking.booking_id == booking_id)

        booking = entities.Booking()

        if ingest_booking.HasField('booking_id'):
            booking.external_id = self._convert(
                converter_utils.normalize, ingest_booking.booking_id)

        if ingest_booking.HasField('admission_date'):
            booking.admission_date = self._convert(
                converter_utils.parse_date_or_error,
                ingest_booking.admission_date)

        if ingest_booking.HasField('release_date'):
            booking.release_date = self._convert(
                converter_utils.parse_date_or_error,
                ingest_booking.release_date)
            booking.release_date_inferred = False

        if ingest_booking.HasField('projected_release_date'):
            booking.projected_release_date = self._convert(
                converter_utils.parse_date_or_error,
                ingest_booking.projected_release_date)

        if ingest_booking.HasField('release_reason'):
            booking.release_reason = self._convert(
                ReleaseReason.from_str, ingest_booking.release_reason)

        if ingest_booking.HasField('custody_status'):
            booking.custody_status = self._convert(
                CustodyStatus.from_str, ingest_booking.custody_status)
        # TODO(338): Clean up the default logic here.
        else:
            booking.custody_status = self._convert(
                CustodyStatus.from_str, 'IN CUSTODY')

        # TODO: Populate hold when the proto is updated to contain a hold table

        if ingest_booking.HasField('facility'):
            booking.facility = self._convert(converter_utils.normalize,
                                             ingest_booking.facility)

        if ingest_booking.HasField('classification'):
            booking.classification = self._convert(
                Classification.from_str, ingest_booking.classification)

        if ingest_booking.HasField('arrest_id'):
            booking.arrest = self._convert_arrest(ingest_booking.arrest_id)

        booking.charges = [self._convert_charge(charge_id) for charge_id in
                           ingest_booking.charge_ids]

        if ingest_booking.HasField('total_bond_amount'):
            # If there is a total bond amount listed, but the booking does
            # not have any bond amounts, update the bonds to that amount or
            # add a new bond with the total amount to each charge.
            bond_amount = self._convert(converter_utils.parse_dollar_amount,
                                        ingest_booking.total_bond_amount)
            total_bond = entities.Bond(amount_dollars=bond_amount,
                                       status='POSTED')
            if not booking.charges:
                booking.charges = [entities.Charge(
                    bond=total_bond, status='PENDING')]
            elif not any(charge.bond and charge.bond.amount_dollars
                         for charge in booking.charges):
                for charge in booking.charges:
                    if charge.bond:
                        charge.bond.amount_dollars = bond_amount
                    else:
                        charge.bond = total_bond

        return booking

    def _convert_arrest(self, arrest_id):
        """Converts an ingest_info proto Arrest to a persistence object."""
        ingest_arrest = more_itertools.one(
            arrest for arrest in self.ingest_info.arrests if
            arrest.arrest_id == arrest_id)

        arrest = entities.Arrest()

        if ingest_arrest.HasField('arrest_id'):
            arrest.external_id = self._convert(
                converter_utils.normalize, ingest_arrest.arrest_id)

        if ingest_arrest.HasField('date'):
            arrest.date = self._convert(converter_utils.parse_date_or_error,
                                        ingest_arrest.date)

        if ingest_arrest.HasField('location'):
            arrest.location = self._convert(converter_utils.normalize,
                                            ingest_arrest.location)

        if ingest_arrest.HasField('agency'):
            arrest.agency = self._convert(converter_utils.normalize,
                                          ingest_arrest.agency)

        if ingest_arrest.HasField('officer_name'):
            arrest.officer_name = self._convert(converter_utils.normalize,
                                                ingest_arrest.officer_name)

        if ingest_arrest.HasField('officer_id'):
            arrest.officer_id = self._convert(converter_utils.normalize,
                                              ingest_arrest.officer_id)

        if ingest_arrest.HasField('agency'):
            arrest.agency = self._convert(converter_utils.normalize,
                                          ingest_arrest.agency)

        return arrest

    def _convert_charge(self, charge_id):
        """Converts an ingest_info proto Charge to a persistence object."""
        ingest_charge = more_itertools.one(
            charge for charge in self.ingest_info.charges if
            charge.charge_id == charge_id)

        charge = entities.Charge()

        if ingest_charge.HasField('charge_id'):
            charge.external_id = self._convert(
                converter_utils.normalize, ingest_charge.charge_id)

        if ingest_charge.HasField('offense_date'):
            charge.offense_date = self._convert(
                converter_utils.parse_date_or_error,
                ingest_charge.offense_date)

        if ingest_charge.HasField('statute'):
            charge.statute = self._convert(converter_utils.normalize,
                                           ingest_charge.statute)

        # TODO(215): charge.offense_code =
        # code_to_BJS(ingest_charge.statute)

        if ingest_charge.HasField('name'):
            charge.name = self._convert(converter_utils.normalize,
                                        ingest_charge.name)

        if ingest_charge.HasField('attempted'):
            charge.attempted = self._convert(
                converter_utils.parse_bool,
                ingest_charge.attempted)

        if ingest_charge.HasField('degree'):
            charge.degree = self._convert(ChargeDegree.from_str,
                                          ingest_charge.degree)

        if ingest_charge.HasField('charge_class'):
            charge.charge_class = self._convert(
                ChargeClass.from_str, ingest_charge.charge_class)

        if ingest_charge.HasField('level'):
            charge.level = self._convert(converter_utils.normalize,
                                         ingest_charge.level)

        if ingest_charge.HasField('fee_dollars'):
            charge.fee_dollars = self._convert(
                converter_utils.parse_dollar_amount,
                ingest_charge.fee_dollars)

        if ingest_charge.HasField('charging_entity'):
            charge.charging_entity = self._convert(
                converter_utils.normalize, ingest_charge.charging_entity)

        if ingest_charge.HasField('status'):
            charge.status = self._convert(
                ChargeStatus.from_str, ingest_charge.status)
        # TODO(338): Clean up the default logic here.
        else:
            charge.status = self._convert(
                ChargeStatus.from_str, 'PENDING')

        if ingest_charge.HasField('number_of_counts'):
            charge.number_of_counts = self._convert(
                int,
                ingest_charge.number_of_counts)

        if ingest_charge.HasField('court_type'):
            charge.court_type = self._convert(
                CourtType.from_str, ingest_charge.court_type)

        if ingest_charge.HasField('case_number'):
            charge.case_number = self._convert(converter_utils.normalize,
                                               ingest_charge.case_number)

        if ingest_charge.HasField('next_court_date'):
            charge.next_court_date = \
                self._convert(converter_utils.parse_date_or_error,
                              ingest_charge.next_court_date)

        if ingest_charge.HasField('judge_name'):
            charge.judge_name = self._convert(converter_utils.normalize,
                                              ingest_charge.judge_name)

        if ingest_charge.HasField('bond_id'):
            charge.bond = self._convert_bond(ingest_charge.bond_id)

        if ingest_charge.HasField('sentence_id'):
            charge.sentence = self._convert_sentence(
                ingest_charge.sentence_id)

        return charge

    def _convert_bond(self, bond_id):
        """Converts an ingest_info proto Bond to a persistence object."""
        ingest_bond = more_itertools.one(
            bond for bond in self.ingest_info.bonds if
            bond.bond_id == bond_id)

        bond = entities.Bond()

        if ingest_bond.HasField('bond_id'):
            bond.external_id = self._convert(
                converter_utils.normalize, ingest_bond.bond_id)

        if ingest_bond.HasField('amount'):
            bond.amount = self._convert(converter_utils.parse_dollar_amount,
                                        ingest_bond.amount)

        if ingest_bond.HasField('bond_type'):
            bond.bond_type = self._convert(
                BondType.from_str, ingest_bond.bond_type)

        if ingest_bond.HasField('status'):
            bond.status = self._convert(
                BondStatus.from_str, ingest_bond.status)
        # TODO(338): Clean up the default logic here.
        else:
            bond.status = self._convert(BondStatus.from_str, 'POSTED')

        return bond

    def _convert_sentence(self, sentence_id):
        """Converts an ingest_info proto Sentence to a persistence object."""
        ingest_sentence = more_itertools.one(
            sentence for sentence in self.ingest_info.sentences if
            sentence.sentence_id == sentence_id)

        sentence = entities.Sentence()

        if ingest_sentence.HasField('sentence_id'):
            sentence.external_id = self._convert(
                converter_utils.normalize, ingest_sentence.sentence_id)

        if ingest_sentence.HasField('date_imposed'):
            sentence.date_imposed = \
                self._convert(converter_utils.parse_date_or_error,
                              ingest_sentence.date_imposed)

        if ingest_sentence.HasField('county_of_commitment'):
            sentence.county_of_commitment = \
                self._convert(converter_utils.normalize,
                              ingest_sentence.county_of_commitment)

        if ingest_sentence.HasField('min_length'):
            sentence.min_length_days = \
                self._convert(converter_utils.time_string_to_days,
                              ingest_sentence.min_length)

        if ingest_sentence.HasField('max_length'):
            sentence.max_length_days = \
                self._convert(converter_utils.time_string_to_days,
                              ingest_sentence.max_length)

        if ingest_sentence.HasField('is_life'):
            sentence.is_life = self._convert(converter_utils.parse_bool,
                                             ingest_sentence.is_life)

        if ingest_sentence.HasField('is_probation'):
            sentence.is_probation = self._convert(
                converter_utils.parse_bool,
                ingest_sentence.is_probation)

        if ingest_sentence.HasField('is_suspended'):
            sentence.is_suspended = self._convert(
                converter_utils.parse_bool,
                ingest_sentence.is_suspended)

        if ingest_sentence.HasField('fine_dollars'):
            sentence.fine_dollars = self._convert(
                converter_utils.parse_dollar_amount,
                ingest_sentence.fine_dollars)

        if ingest_sentence.HasField('parole_possible'):
            sentence.parole_possible = \
                self._convert(converter_utils.parse_bool,
                              ingest_sentence.parole_possible)

        if ingest_sentence.HasField('post_release_supervision_length'):
            sentence.post_release_supervision_length_days = \
                self._convert(converter_utils.time_string_to_days,
                              ingest_sentence.post_release_supervision_length)

        return sentence

    def _convert(self, func, *args):
        """Given a function and its arguments, calls the function with the
        given arguments; however, instead in the case that the function
        raises an Exception, adds that exception to self.conversion_errors
        instead of raising it.

        Args:
            func: The method to be called
            *args: Arguments for the provided func

        Returns:
            the return value of func if no errors occur. If errors occur,
            returns None
        """
        # try:
        return func(*args)
        # except Exception as e:
        #     self.conversion_errors.add(e)
        #     return None


def _build_error_string(conversion_errors):
    error_str = '\n'
    for error in conversion_errors:
        error_str += '    ' + str(error) + '\n'
    return error_str
