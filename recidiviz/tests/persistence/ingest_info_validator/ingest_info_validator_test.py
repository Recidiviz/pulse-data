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
"""Tests for validating ingest_info protos."""

import unittest

from recidiviz.ingest.models.ingest_info_pb2 import (
    Arrest,
    Bond,
    Booking,
    Charge,
    IngestInfo,
    Person,
    Sentence,
)
from recidiviz.persistence.ingest_info_validator import ingest_info_validator
from recidiviz.persistence.ingest_info_validator.ingest_info_validator import (
    ValidationError,
)

PERSON_1 = "PERSON_ID_1"
EXTRA_PERSON = "EXTRA_PERSON_ID"

BOOKING_1 = "BOOKING_ID_1"
BOOKING_2 = "BOOKING_ID_2"
MISSING_BOOKING = "MISSING_BOOKING_ID"
EXTRA_BOOKING = "EXTRA_BOOKING_ID"

ARREST_1 = "ARREST_ID_1"
ARREST_2 = "ARREST_ID_2"
MISSING_ARREST = "MISSING_ARREST_ID"
EXTRA_ARREST = "EXTRA_ARREST_ID"

CHARGE_1 = "CHARGE_ID_1"
CHARGE_2 = "CHARGE_ID_2"
MISSING_CHARGE = "MISSING_CHARGE_ID"
EXTRA_CHARGE = "EXTRA_CHARGE_ID"

SENTENCE_1 = "SENTENCE_ID_1"
MISSING_SENTENCE = "MISSING_SENTENCE_ID"
EXTRA_SENTENCE = "EXTRA_SENTENCE_ID"

BOND_1 = "BOND_ID_1"
MISSING_BOND = "MISSING_BOND_ID"
EXTRA_BOND = "EXTRA_BOND_ID"


class TestValidator(unittest.TestCase):
    """Test ingest_info validation."""

    def test_empty_ingest_info(self) -> None:
        ingest_info_validator.validate(IngestInfo())

    def test_duplicate_ids(self) -> None:
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.extend(
            [
                Person(person_id=PERSON_1),
                Person(person_id=PERSON_1),
                Person(person_id=PERSON_1, booking_ids=[BOOKING_1, BOOKING_2]),
            ]
        )
        ingest_info.bookings.extend(
            [
                Booking(booking_id=BOOKING_1),
                Booking(booking_id=BOOKING_1, charge_ids=[CHARGE_1, CHARGE_2]),
                Booking(booking_id=BOOKING_2, arrest_id=ARREST_1),
                Booking(booking_id=BOOKING_2, arrest_id=ARREST_2),
            ]
        )
        ingest_info.arrests.extend(
            [
                Arrest(arrest_id=ARREST_1),
                Arrest(arrest_id=ARREST_1),
                Arrest(arrest_id=ARREST_2),
            ]
        )
        ingest_info.charges.extend(
            [Charge(charge_id=CHARGE_1), Charge(charge_id=CHARGE_2)]
        )

        # Act
        with self.assertRaises(ValidationError) as e:
            ingest_info_validator.validate(ingest_info)
        result = e.exception.errors

        # Assert
        expected_result = {
            "people": {
                "duplicate_ids": {PERSON_1},
            },
            "bookings": {"duplicate_ids": {BOOKING_1, BOOKING_2}},
            "arrests": {"duplicate_ids": {ARREST_1}},
        }

        self.assertEqual(result, expected_result)

    def test_non_existing_id(self) -> None:
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.add(person_id="1", booking_ids=["2"])

        # Act
        with self.assertRaises(ValidationError) as e:
            ingest_info_validator.validate(ingest_info)
        result = e.exception.errors

        # Assert
        expected_result = {
            "bookings": {
                "ids_referenced_that_do_not_exist": {"2"},
            }
        }

        self.assertEqual(result, expected_result)

    def test_extra_id(self) -> None:
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.bookings.add(booking_id="1")

        # Act
        with self.assertRaises(ValidationError) as e:
            ingest_info_validator.validate(ingest_info)
        result = e.exception.errors

        # Assert
        expected_result = {
            "bookings": {
                "ids_never_referenced": {"1"},
            }
        }

        self.assertEqual(result, expected_result)

    def test_reports_all_errors_together(self) -> None:
        # Arrange
        ingest_info = IngestInfo()
        ingest_info.people.extend(
            [
                Person(person_id=PERSON_1, booking_ids=[MISSING_BOOKING]),
                Person(person_id=PERSON_1, booking_ids=[BOOKING_1]),
                Person(person_id=PERSON_1, booking_ids=[BOOKING_1, BOOKING_2]),
                Person(person_id=EXTRA_PERSON),
            ]
        )
        ingest_info.bookings.extend(
            [
                Booking(booking_id=BOOKING_1),
                Booking(booking_id=BOOKING_1, arrest_id=MISSING_ARREST),
                Booking(
                    booking_id=BOOKING_2,
                    arrest_id=ARREST_1,
                    charge_ids=[CHARGE_1, CHARGE_2, MISSING_CHARGE],
                ),
                Booking(booking_id=EXTRA_BOOKING),
            ]
        )
        ingest_info.arrests.extend(
            [
                Arrest(arrest_id=ARREST_1),
                Arrest(arrest_id=ARREST_1),
                Arrest(arrest_id=EXTRA_ARREST),
            ]
        )
        ingest_info.charges.extend(
            [
                Charge(charge_id=CHARGE_1),
                Charge(charge_id=CHARGE_1, sentence_id=SENTENCE_1, bond_id=BOND_1),
                Charge(
                    charge_id=CHARGE_2,
                    sentence_id=MISSING_SENTENCE,
                    bond_id=MISSING_BOND,
                ),
                Charge(charge_id=EXTRA_CHARGE),
            ]
        )
        ingest_info.bonds.extend(
            [Bond(bond_id=BOND_1), Bond(bond_id=BOND_1), Bond(bond_id=EXTRA_BOND)]
        )
        ingest_info.sentences.extend(
            [
                Sentence(sentence_id=SENTENCE_1),
                Sentence(sentence_id=SENTENCE_1),
                Sentence(sentence_id=EXTRA_SENTENCE),
            ]
        )

        # Act
        with self.assertRaises(ValidationError) as e:
            ingest_info_validator.validate(ingest_info)
        result = e.exception.errors

        # Assert
        expected_result = {
            "people": {"duplicate_ids": {PERSON_1}},
            "bookings": {
                "duplicate_ids": {BOOKING_1},
                "ids_referenced_that_do_not_exist": {MISSING_BOOKING},
                "ids_never_referenced": {EXTRA_BOOKING},
            },
            "arrests": {
                "duplicate_ids": {ARREST_1},
                "ids_referenced_that_do_not_exist": {MISSING_ARREST},
                "ids_never_referenced": {EXTRA_ARREST},
            },
            "charges": {
                "duplicate_ids": {CHARGE_1},
                "ids_referenced_that_do_not_exist": {MISSING_CHARGE},
                "ids_never_referenced": {EXTRA_CHARGE},
            },
            "sentences": {
                "duplicate_ids": {SENTENCE_1},
                "ids_referenced_that_do_not_exist": {MISSING_SENTENCE},
                "ids_never_referenced": {EXTRA_SENTENCE},
            },
            "bonds": {
                "duplicate_ids": {BOND_1},
                "ids_referenced_that_do_not_exist": {MISSING_BOND},
                "ids_never_referenced": {EXTRA_BOND},
            },
        }

        self.assertEqual(result, expected_result)
