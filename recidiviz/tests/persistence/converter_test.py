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
"""Tests for data converter."""

from datetime import date, datetime
import pytest

from recidiviz.ingest.models.ingest_info import _Person, _Booking, _Charge, \
    _Sentence
from recidiviz.persistence import converter

class TestConverter(object):
    """Test converting IngestInfo objects to Schema objects."""

    def test_parsePersonFields(self):
        ingest_person = _Person(full_name='LAST, FIRST',
                                birthdate='12-31-1999',
                                place_of_residence='NNN\n  STREET \t ZIP')

        schema_person = converter.convert_person(ingest_person)
        assert schema_person.given_names == 'FIRST'
        assert schema_person.surname == 'LAST'
        assert schema_person.birthdate == datetime(year=1999, month=12, day=31)
        assert schema_person.place_of_residence == 'NNN STREET ZIP'

    def test_convertAgeToBirthdate(self):
        ingest_person = _Person(age='1000')

        schema_person = converter.convert_person(ingest_person)
        expected_birthdate = date(year=date.today().year - 1000,
                                  month=1, day=1)
        assert expected_birthdate == schema_person.birthdate

    def test_convertTotalBondAmountToBond(self):
        ingest_booking = _Booking(total_bond_amount='$100.00')

        schema_booking = converter.convert_booking(ingest_booking)
        assert len(schema_booking.charges) == 1
        schema_charge = schema_booking.charges[0]
        assert schema_charge.bond is not None
        assert schema_charge.bond.amount == 100

    def test_updateChargeWithTotalBondAmount(self):
        ingest_booking = _Booking(total_bond_amount='$100.00')
        ingest_booking.create_charge().create_bond(bond_id='ID')
        ingest_booking.create_charge()

        schema_booking = converter.convert_booking(ingest_booking)
        assert len(schema_booking.charges) == 2
        schema_charge = schema_booking.charges[0]
        assert schema_charge.bond is not None
        assert schema_charge.bond.scraped_bond_id == 'ID'
        assert schema_charge.bond.amount == 100
        assert schema_booking.charges[1] is not None
        assert schema_booking.charges[1].bond is not None
        assert schema_booking.charges[1].bond.amount == 100

    def test_updateBookingFields(self):
        ingest_booking = _Booking(release_date='1/1/1111',
                                  projected_release_date='2/2/2222')

        schema_booking = converter.convert_booking(ingest_booking)
        assert schema_booking.release_date == \
            datetime(year=1111, month=1, day=1)
        assert schema_booking.release_date_inferred is False
        assert schema_booking.projected_release_date == \
            datetime(year=2222, month=2, day=2)

    def test_parseChargeFields(self):
        ingest_charge = _Charge(attempted=True, number_of_counts='3')

        schema_charge = converter.convert_charge(ingest_charge)
        assert schema_charge.attempted is True
        assert schema_charge.number_of_counts == 3

    def test_parseSentenceFields(self):
        ingest_sentence = _Sentence(min_length='1',
                                    post_release_supervision_length='')

        schema_sentence = converter.convert_sentence(ingest_sentence)
        assert schema_sentence.min_length_days == 1
        assert schema_sentence.post_release_supervision_length_days == 0

    def test_convertIngestInfo(self):
        ingest_person = _Person(person_id='PERSON_ID')
        ingest_booking = ingest_person.create_booking(booking_id='BOOKING_ID')
        ingest_booking.create_arrest(agency='PD')
        ingest_charge = ingest_booking.create_charge(name='CHARGE')
        ingest_charge.create_bond(bond_id='BOND_ID')
        ingest_charge.create_sentence(is_life=False)

        schema_person = converter.convert_person(ingest_person)
        assert schema_person.scraped_person_id == 'PERSON_ID'
        assert len(schema_person.bookings) == 1
        schema_booking = schema_person.bookings[0]
        assert schema_booking.scraped_booking_id == 'BOOKING_ID'
        assert schema_booking.arrest is not None
        assert schema_booking.arrest.agency == 'PD'
        assert len(schema_booking.charges) == 1
        schema_charge = schema_booking.charges[0]
        assert schema_charge.name == 'CHARGE'
        assert schema_charge.bond is not None
        assert schema_charge.bond.scraped_bond_id == 'BOND_ID'
        assert schema_charge.sentence is not None
        assert schema_charge.sentence.is_life is False

    def test_parseBadDate(self):
        ingest_person = _Person(birthdate='ABC')

        with pytest.raises(ValueError):
            converter.convert_person(ingest_person)

    def test_parseBadAge(self):
        ingest_person = _Person(age='ABC')

        with pytest.raises(ValueError):
            converter.convert_person(ingest_person)

    def test_parseBadTimeDuration(self):
        ingest_sentence = _Sentence(min_length='ABC')

        with pytest.raises(ValueError):
            converter.convert_sentence(ingest_sentence)

    def test_parseBadName(self):
        ingest_person = _Person(full_name='ABC')

        with pytest.raises(ValueError):
            converter.convert_person(ingest_person)

    def test_parseBadDollarAmount(self):
        ingest_charge = _Charge(fee='ABC')

        with pytest.raises(ValueError):
            converter.convert_charge(ingest_charge)

    def test_parseBadBoolField(self):
        ingest_charge = _Charge(attempted='ABC')

        with pytest.raises(ValueError):
            converter.convert_charge(ingest_charge)
