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
from recidiviz.persistence import converter_utils
from recidiviz.persistence.database import schema


class Converter(object):
    """Class that converts between ingest_info objects and Schema objects"""

    def __init__(self):
        self.conversion_errors = set()

    def convert_ingest_info(self, ingest_info):
        """Converts an IngestInfo object into a list of Schema people. If a
        validation error happens, raises a ValueError and prints out all
        validation errors for this ingest_info object.

        Args:
            ingest_info: (recidiviz.ingest.models.ingest_info)

        Returns:
            List[(recidiviz.persistence.database.schema.Person)]
        """
        people = []
        self.conversion_errors = set()

        for ingest_person in ingest_info.person:
            people.append(self._convert_person(ingest_person))

        if self.conversion_errors:
            error_str = '\n'
            for error in self.conversion_errors:
                error_str += '    ' + str(error) + '\n'
            raise ValueError(error_str)
        return people

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
        if any(arg is None for arg in args):
            return None

        try:
            return func(*args)
        except Exception as e:
            self.conversion_errors.add(e)
            return None

    def _convert_person(self, ingest_person):
        """Converts an IngestInfo person into a Schema person.

        Args:
            ingest_person: (recidiviz.ingest.models.ingest_info._Person)

        Returns:
            (recidiviz.persistence.database.schema.Person)
        """
        person = schema.Person()
        person.scraped_person_id = self._convert(converter_utils.normalize,
                                                 ingest_person.person_id)

        if ingest_person.surname or ingest_person.given_names:
            person.surname = self._convert(converter_utils.normalize,
                                           ingest_person.surname)
            person.given_names = self._convert(converter_utils.normalize,
                                               ingest_person.given_names)
        elif ingest_person.full_name is not None:
            last, first = self._convert(converter_utils.split_full_name,
                                        ingest_person.full_name)
            person.surname = self._convert(converter_utils.normalize, last)
            person.given_names = self._convert(converter_utils.normalize, first)

        if ingest_person.birthdate is not None:
            person.birthdate = self._convert(
                converter_utils.parse_date_or_error,
                ingest_person.birthdate)
            person.birthdate_inferred_from_age = False
        elif ingest_person.age is not None:
            person.birthdate = self._convert(
                converter_utils.calculate_birthdate_from_age,
                ingest_person.age)
            person.birthdate_inferred_from_age = True

        person.gender = self._convert(converter_utils.string_to_enum,
                                      'gender',
                                      ingest_person.gender)

        if ingest_person.race is not None and ingest_person.ethnicity is None \
            and converter_utils.race_is_actually_ethnicity(ingest_person):
            # TODO(289): determine how to properly set race here once UNKNOWN
            # and NOT_PROVIDED are handled by enums.
            person.ethnicity = self._convert(converter_utils.string_to_enum,
                                             'ethnicity',
                                             ingest_person.race)
        else:
            person.race = self._convert(converter_utils.string_to_enum,
                                        'race', ingest_person.race)
            person.ethnicity = self._convert(converter_utils.string_to_enum,
                                             'ethnicity',
                                             ingest_person.ethnicity)

        person.place_of_residence = self._convert(
            converter_utils.normalize, ingest_person.place_of_residence)

        person.bookings = [self._convert_booking(b) for b in
                           ingest_person.booking]

        return person

    def _convert_booking(self, ingest_booking):
        """Converts an IngestInfo booking into a Schema booking.

        Args:
            ingest_person: (recidiviz.ingest.models.ingest_info._Booking)

        Returns:
            (recidiviz.persistence.database.schema.Booking)
        """
        booking = schema.Booking()

        booking.scraped_booking_id = self._convert(
            converter_utils.normalize, ingest_booking.booking_id)

        booking.admission_date = self._convert(
            converter_utils.parse_date_or_error,
            ingest_booking.admission_date)

        if ingest_booking.release_date is not None:
            booking.release_date = self._convert(
                converter_utils.parse_date_or_error,
                ingest_booking.release_date)
            booking.release_date_inferred = False

        booking.projected_release_date = self._convert(
            converter_utils.parse_date_or_error,
            ingest_booking.projected_release_date)

        booking.release_reason = self._convert(
            converter_utils.string_to_enum, 'release_reason',
            ingest_booking.release_reason)

        booking.custody_status = self._convert(
            converter_utils.string_to_enum, 'custody_status',
            ingest_booking.custody_status)

        if ingest_booking.hold is not None:
            # TODO: decide if this should be a list (of objects? strings?)
            # instead
            booking.hold = self._convert(converter_utils.normalize,
                                         ingest_booking.hold)
            # TODO: decide under what conditions we can set this to False
            booking.held_for_other_jurisdiction = True

        booking.facility = self._convert(converter_utils.normalize,
                                         ingest_booking.facility)

        booking.region = ingest_booking.region

        booking.classification = self._convert(
            converter_utils.string_to_enum, 'classification',
            ingest_booking.classification)

        if ingest_booking.arrest:
            booking.arrest = self._convert_arrest(ingest_booking.arrest)

        booking.charges = [self._convert_charge(c) for c in
                           ingest_booking.charge]

        if ingest_booking.total_bond_amount is not None:
            # If there is a total bond amount listed, but the booking does
            # not have any bond amounts, update the bonds to that amount or
            # add a new bond with the total amount to each charge.
            bond_amount = self._convert(converter_utils.parse_dollar_amount,
                                        ingest_booking.total_bond_amount)
            total_bond = schema.Bond(amount_dollars=bond_amount)
            if not booking.charges:
                booking.charges = [schema.Charge(bond=total_bond)]
            elif not any(charge.bond and charge.bond.amount_dollars
                         for charge in booking.charges):
                for charge in booking.charges:
                    if charge.bond:
                        charge.bond.amount_dollars = bond_amount
                    else:
                        charge.bond = total_bond

        return booking

    def _convert_arrest(self, ingest_arrest):
        """Converts an IngestInfo arrest into a Schema arrest.

        Args:
            ingest_person: (recidiviz.ingest.models.ingest_info._Arrest)

        Returns:
            (recidiviz.persistence.database.schema.Arrest)
        """
        arrest = schema.Arrest()

        arrest.date = self._convert(converter_utils.parse_date_or_error,
                                    ingest_arrest.date)

        arrest.location = self._convert(converter_utils.normalize,
                                        ingest_arrest.location)

        arrest.agency = self._convert(converter_utils.normalize,
                                      ingest_arrest.agency)

        arrest.officer_name = self._convert(converter_utils.normalize,
                                            ingest_arrest.officer_name)

        arrest.officer_id = self._convert(converter_utils.normalize,
                                          ingest_arrest.officer_id)

        arrest.agency = self._convert(converter_utils.normalize,
                                      ingest_arrest.agency)

        return arrest

    def _convert_charge(self, ingest_charge):
        """Converts an IngestInfo charge into a Schema charge.

        Args:
            ingest_person: (recidiviz.ingest.models.ingest_info._Charge)

        Returns:
            (recidiviz.persistence.database.schema.Charge)
        """
        charge = schema.Charge()

        charge.offense_date = self._convert(
            converter_utils.parse_date_or_error,
            ingest_charge.offense_date)

        charge.statute = self._convert(converter_utils.normalize,
                                       ingest_charge.statute)
        # TODO(215): charge.offense_code =
        # code_to_BJS(ingest_charge.statute)

        charge.name = self._convert(converter_utils.normalize,
                                    ingest_charge.name)

        charge.attempted = self._convert(converter_utils.verify_is_bool,
                                         ingest_charge.attempted)

        charge.degree = self._convert(converter_utils.string_to_enum,
                                      'charge_degree',
                                      ingest_charge.degree)

        charge.charge_class = self._convert(converter_utils.string_to_enum,
                                            'charge_class',
                                            ingest_charge.charge_class)

        charge.level = self._convert(converter_utils.normalize,
                                     ingest_charge.level)

        charge.fee_dollars = self._convert(
            converter_utils.parse_dollar_amount,
            ingest_charge.fee_dollars)

        charge.charging_entity = self._convert(
            converter_utils.normalize, ingest_charge.charging_entity)

        charge.status = self._convert(converter_utils.string_to_enum,
                                      'charge_status',
                                      ingest_charge.status)

        charge.number_of_counts = self._convert(int,
                                                ingest_charge.number_of_counts)

        charge.court_type = self._convert(converter_utils.string_to_enum,
                                          'court_type',
                                          ingest_charge.court_type)

        charge.case_number = self._convert(converter_utils.normalize,
                                           ingest_charge.case_number)

        charge.next_court_date = \
            self._convert(converter_utils.parse_date_or_error,
                          ingest_charge.next_court_date)

        charge.judge_name = self._convert(converter_utils.normalize,
                                          ingest_charge.judge_name)

        if ingest_charge.bond:
            charge.bond = self._convert_bond(ingest_charge.bond)

        if ingest_charge.sentence:
            charge.sentence = self._convert_sentence(ingest_charge.sentence)

        return charge

    def _convert_bond(self, ingest_bond):
        """Converts an IngestInfo bond into a Schema bond.

        Args:
            ingest_bond: (recidiviz.ingest.models.ingest_info._Bond)

        Returns:
            (recidiviz.persistence.database.schema.Bond)
        """

        bond = schema.Bond()

        bond.scraped_bond_id = self._convert(converter_utils.normalize,
                                             ingest_bond.bond_id)

        bond.amount = self._convert(converter_utils.parse_dollar_amount,
                                    ingest_bond.amount)

        bond.type = self._convert(converter_utils.string_to_enum,
                                  'bond_type',
                                  ingest_bond.bond_type)

        bond.status = self._convert(converter_utils.string_to_enum,
                                    'bond_status',
                                    ingest_bond.status)

        return bond

    def _convert_sentence(self, ingest_sentence):
        """Converts an IngestInfo sentence into a Schema sentence.

        Args:
            ingest_person: (recidiviz.ingest.models.ingest_info._Sentence)

        Returns:
            (recidiviz.persistence.database.schema.Sentence)
        """
        sentence = schema.Sentence()

        sentence.date_imposed = \
            self._convert(converter_utils.parse_date_or_error,
                          ingest_sentence.date_imposed)

        sentence.county_of_commitment = \
            self._convert(converter_utils.normalize,
                          ingest_sentence.county_of_commitment)

        sentence.min_length_days = \
            self._convert(converter_utils.time_string_to_days,
                          ingest_sentence.min_length)

        sentence.max_length_days = \
            self._convert(converter_utils.time_string_to_days,
                          ingest_sentence.max_length)

        sentence.is_life = self._convert(converter_utils.verify_is_bool,
                                         ingest_sentence.is_life)

        sentence.is_probation = self._convert(
            converter_utils.verify_is_bool,
            ingest_sentence.is_probation)

        sentence.is_suspended = self._convert(
            converter_utils.verify_is_bool,
            ingest_sentence.is_suspended)

        sentence.fine_dollars = self._convert(
            converter_utils.parse_dollar_amount,
            ingest_sentence.fine_dollars)

        sentence.parole_possible = \
            self._convert(converter_utils.verify_is_bool,
                          ingest_sentence.parole_possible)

        sentence.post_release_supervision_length_days = \
            self._convert(converter_utils.time_string_to_days,
                          ingest_sentence.post_release_supervision_length)

        return sentence
