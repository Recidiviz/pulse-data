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
"""Tests for ingest/extractor/json_data_extractor.py"""
import os
import unittest

from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import Booking, Charge, IngestInfo
from recidiviz.tests.ingest import fixtures


class DataExtractorJsonTest(unittest.TestCase):
    """Tests for extracting data from JSON."""

    def test_jailtracker_person(self):
        key_mapping_file = "fixtures/jailtracker_person.yaml"
        key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
        extractor = JsonDataExtractor(key_mapping_file)

        expected_result = IngestInfo()
        expected_result.create_person(
            person_id="012345", birthdate="12/12/0001", age="2018", race="WHITE"
        )
        result = extractor.extract_and_populate_data(
            fixtures.as_dict("extractor", "jailtracker_person.json")
        )

        self.assertEqual(result, expected_result)

    def test_jailtracker_booking(self):
        key_mapping_file = "fixtures/jailtracker_booking.yaml"
        key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)

        extractor = JsonDataExtractor(key_mapping_file)

        expected_result = IngestInfo()
        expected_person = expected_result.create_person()
        expected_person.create_booking(
            booking_id="123098", admission_date="1/1/2001", release_date="1/1/2001"
        )
        expected_person.create_booking(
            booking_id="123099", admission_date="1/1/2002", release_date="1/1/2002"
        )

        result = extractor.extract_and_populate_data(
            fixtures.as_dict("extractor", "jailtracker_booking.json")
        )

        self.assertEqual(result, expected_result)

    def test_person_with_charges(self):
        key_mapping_file = "fixtures/person_with_charges.yaml"
        key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
        extractor = JsonDataExtractor(key_mapping_file)

        expected_result = IngestInfo()
        expected_person = expected_result.create_person(
            person_id="3245", full_name="AAA AAAB", race="BLACK"
        )
        booking_1 = expected_person.create_booking(
            booking_id="324567", admission_date="1/1/1111"
        )
        booking_1.create_charge(charge_id="345309", name="charge name 1")
        booking_1.create_charge(charge_id="894303", name="charge name 2")
        booking_2 = expected_person.create_booking(
            booking_id="3245", admission_date="2/2/2222"
        )
        booking_2.create_charge(charge_id="42309", name="charge name 3")

        result = extractor.extract_and_populate_data(
            fixtures.as_dict("extractor", "person_with_charges.json")
        )
        self.assertEqual(result, expected_result)

    def test_person_with_holds(self):
        key_mapping_file = "fixtures/person_with_holds.yaml"
        key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
        extractor = JsonDataExtractor(key_mapping_file)

        expected_result = IngestInfo()
        expected_person = expected_result.create_person(
            person_id="3245", full_name="AAA AAAB", race="BLACK"
        )
        booking_1 = expected_person.create_booking(
            booking_id="324567", admission_date="1/1/1111"
        )
        booking_1.create_hold(hold_id="345309", jurisdiction_name="jurisdiction name 1")
        booking_1.create_hold(hold_id="894303", jurisdiction_name="jurisdiction name 2")
        booking_2 = expected_person.create_booking(
            booking_id="3245", admission_date="2/2/2222"
        )
        booking_2.create_hold(hold_id="42309", jurisdiction_name="jurisdiction name 3")

        result = extractor.extract_and_populate_data(
            fixtures.as_dict("extractor", "person_with_holds.json")
        )
        self.assertEqual(result, expected_result)

    def test_skip_empty(self):
        key_mapping_file = os.path.join(
            os.path.dirname(__file__), "fixtures/skip_empty.yaml"
        )
        extractor = JsonDataExtractor(key_mapping_file)

        expected = IngestInfo()
        expected.create_person(
            full_name="skip empty",
            bookings=[
                Booking(
                    custody_status="in custody",
                    booking_id="1",
                    charges=[
                        Charge(
                            name="battery",
                        ),
                        Charge(
                            name="assault",
                            charge_class="misdemeanor",
                        ),
                    ],
                ),
                Booking(
                    booking_id="2",
                    charges=[
                        Charge(
                            name="robbery",
                            charge_class="felony",
                        ),
                    ],
                ),
            ],
        )

        result = extractor.extract_and_populate_data(
            fixtures.as_dict("extractor", "skip_empty.json")
        )
        self.assertEqual(result, expected)
