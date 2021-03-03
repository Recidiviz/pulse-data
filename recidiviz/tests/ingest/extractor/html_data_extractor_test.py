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

"""Tests for ingest/extractor/html_data_extractor.py"""
import os
import unittest

from lxml import html

from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.tests.ingest import fixtures


class HtmlDataExtractorTest(unittest.TestCase):
    """Tests for extracting data from HTML."""

    def extract(self, html_filename, yaml_filename):
        yaml_path = os.path.join(
            os.path.dirname(__file__), "../testdata/data_extractor/yaml", yaml_filename
        )
        extractor = HtmlDataExtractor(yaml_path)
        contents = html.fromstring(
            fixtures.as_string("testdata/data_extractor/html", html_filename)
        )
        return extractor.extract_and_populate_data(contents)

    def test_good_table(self):
        """Tests a well modelled table."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.birthdate = "1/15/2048"

        info = self.extract("good_table.html", "good_table.yaml")
        self.assertEqual(expected_info, info)

    def test_good_table_with_link(self):
        """Tests a well modelled table with a link."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.birthdate = "1/15/2048"

        info = self.extract("good_table_links.html", "good_table.yaml")
        self.assertEqual(expected_info, info)

    def test_nested_good_table(self):
        """Tests a well modelled nested table."""
        expected_info = IngestInfo()

        # Add person information
        person = expected_info.create_person()
        person.surname = "LAST NAME"
        person.birthdate = "06/03/2999"
        person.gender = "Male"
        person.age = "100000000"
        person.race = "White/Eurp/ N.Afr/Mid Eas"
        person.person_id = "18-00187"

        # Add booking information
        booking = person.create_booking()
        booking.booking_id = "18-00000"
        booking.admission_date = "1/05/2000 09:39"
        booking.create_hold(jurisdiction_name="District Court 13-3-01")

        # Add charge information
        charge = booking.create_charge()
        charge.statute = "901"
        charge.name = "Criminal Attempt [INCHOATE]"
        charge.case_number = "CR-000-2000"

        # Add bond information
        charge.create_bond().amount = "$1.00"

        info = self.extract("nested_good_table.html", "nested_good_table.yaml")
        self.assertEqual(expected_info, info)

    def test_bad_table(self):
        """Tests a table with an unusual cell layout."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.race = "W"
        booking = person.create_booking()

        charge1 = booking.create_charge()
        charge1.charging_entity = "ROCKWALL CO SO ROCKWALL"
        charge1.name = "FIRST CHARGE"
        charge1.offense_date = "1-01-2042"
        bond1 = charge1.create_bond()
        bond1.amount = "$75,000.00"
        bond1.bond_type = "Surety Bond"

        charge2 = booking.create_charge()
        charge2.charging_entity = "Rockwall"
        charge2.name = "SECOND CHARGE"
        charge2.offense_date = "1-01-2040"
        bond2 = charge2.create_bond()
        bond2.amount = "$1,500.00"
        bond2.bond_type = "Surety Bond"

        charge3 = booking.create_charge()
        charge3.charging_entity = "Rockwall"
        charge3.name = "THIRD CHARGE"
        charge3.offense_date = "1-01-2040"
        bond3 = charge3.create_bond()
        bond3.amount = "$2,000.00"
        bond3.bond_type = "Surety Bond"

        info = self.extract("bad_table.html", "bad_table.yaml")
        # The scraper scrapes a charge row that just says 'Count=3'; this row
        # has to be manually removed
        info.people[0].bookings[0].charges.pop()
        self.assertEqual(expected_info, info)

    def test_multiple_people_with_maybe_charges(self):
        """Tests for a page with many people, each with possibly a set of
        charges"""
        expected_info = IngestInfo()

        # Create the info for the first person
        person1 = expected_info.create_person()
        person1.person_id = "person1"
        person1.place_of_residence = "address1"
        person1.age = "62"

        booking1 = person1.create_booking()
        booking1.admission_date = "03/14/2009 02:09:04"
        booking1.release_date = "3/14/2009 3:49:00 PM"
        booking1.booking_id = "booking1"
        booking1.custody_status = "Release"

        b1_charge1 = booking1.create_charge()
        b1_charge1.statute = "13A-6-24"
        b1_charge1.case_number = "000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)"
        b1_charge1.name = "RECKLESS ENDANGERMENT"
        b1_charge1.level = "M"
        b1_charge1.create_bond().amount = "$1000.00"
        b1_charge2 = booking1.create_charge()
        b1_charge2.statute = "13A-6-24"
        b1_charge2.case_number = "000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)"
        b1_charge2.name = "RECKLESS ENDANGERMENT"
        b1_charge2.level = "M"
        b1_charge2.create_bond().amount = "$1000.00"

        # Create info for the second person
        person2 = expected_info.create_person()
        person2.person_id = "person2"
        person2.place_of_residence = "address2"
        person2.age = "44"

        booking2 = person2.create_booking()
        booking2.admission_date = "11/19/2001 07:03:00"
        booking2.release_date = "11/19/2001 6:40:00 PM"
        booking2.booking_id = "booking2"
        booking2.custody_status = "Release"

        # Create info for the 3rd person
        person3 = expected_info.create_person()
        person3.person_id = "person3"
        person3.place_of_residence = "address3"
        person3.age = "22"

        booking3 = person3.create_booking()
        booking3.admission_date = "05/12/2014 11:04:59"
        booking3.release_date = "5/13/2014 9:08:08 AM"
        booking3.booking_id = "booking3"
        booking3.custody_status = "Release"

        b3_charge1 = booking3.create_charge()
        b3_charge1.statute = "13A-12-214"
        b3_charge1.case_number = "000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)"
        b3_charge1.name = "POSSESSION OF MARIJUANA SECOND DEGREE"
        b3_charge1.degree = "S"
        b3_charge1.level = "M"
        b3_charge1.create_bond().amount = "$1000.00"
        b3_charge2 = booking3.create_charge()
        b3_charge2.statute = "13A-12-260"
        b3_charge2.case_number = "000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)"
        b3_charge2.name = "POSSESSION OF DRUG PARAPHERNALIA"
        b3_charge2.level = "M"
        b3_charge2.create_bond().amount = "$1000.00"

        info = self.extract(
            "multiple_people_sometimes_charges.html",
            "multiple_people_sometimes_charges.yaml",
        )
        self.assertEqual(expected_info, info)

    def test_bad_object(self):
        """Tests a yaml file with a db object that doesn't exist."""
        with self.assertRaises(KeyError):
            self.extract("good_table.html", "bad_object.yaml")

    def test_bad_attr(self):
        """Tests a yaml file with a db attribute that doesn't exist."""
        with self.assertRaises(AttributeError):
            self.extract("good_table.html", "bad_attr.yaml")

    def test_partial_table(self):
        """Tests a page with a table as well as unstructured data."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.age = "38"
        person.place_of_residence = "WICHITA FALLS"
        person.race = "HISPANIC"
        booking = person.create_booking()
        booking.admission_date = "08/18/2017"
        charge = booking.create_charge()
        charge.name = "FIRST CHARGE"
        charge.charging_entity = "WICHITA FALLS PD"
        bond = charge.create_bond()
        bond.amount = "25,000.00"

        info = self.extract("partial_table.html", "partial_table.yaml")
        self.assertEqual(expected_info, info)

    def test_labeled_fields(self):
        """Tests a page with field values in <span>s labeled by <label>s."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.person_id = "11111"
        person.race = "White"
        person.gender = "Male"
        booking = person.create_booking()
        booking.admission_date = "11/12/2018 5:04 PM"
        booking.facility = "Walla Walla County Corrections Department"
        charge = booking.create_charge()
        charge.name = "DUI"
        charge.offense_date = "9/21/2018 5:34 PM"
        charge.charge_class = "Gross Misdemeanor"
        charge.status = "Time Served"
        booking.charges.append(charge)
        info = self.extract("labeled_fields.html", "labeled_fields.yaml")
        self.assertEqual(expected_info, info)

    def test_bad_labels(self):
        """Tests a page with field values in <span>s labeled by nested
        <label>s."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.person_id = "046573"
        person.gender = "Male"
        booking = person.create_booking()
        booking.booking_id = "00119283"
        booking.admission_date = "07/19/2018 14:24"
        booking.facility = "MCSO"
        charge = booking.create_charge()
        bond = charge.create_bond()
        bond.amount = "$100,000.00"
        bond.bond_type = "10 %"
        charge.statute = "2911.12"
        charge.name = "BURGLARY"

        info = self.extract("bad_labels.html", "bad_labels.yaml")
        self.assertEqual(expected_info, info)

    def test_text_label(self):
        """Tests a page with a key/value pair in plain text."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.birthdate = "12/25/0"
        person.race = "W"
        person.gender = "M"
        booking = person.create_booking()
        booking.booking_id = "202200000"
        booking.admission_date = "01/01/2001 19:44"
        booking.release_date = "11/01/2014"
        booking.total_bond_amount = "00000000"
        booking.facility = "Southwest Detention Center"
        arrest = booking.create_arrest()
        arrest.arrest_date = "01/01/2001 09:01"
        arrest.agency = "Hemet PD"
        charge1 = booking.create_charge()
        charge1.statute = "245(A)(1)"
        charge1.status = "DISM"
        charge1.name = "CHARGE 1"
        charge1.degree = "FEL"
        charge2 = booking.create_charge()
        charge2.statute = "245(A)(4)"
        charge2.status = "SENT"
        charge2.name = "CHARGE 2"
        charge2.degree = "FEL"
        bond2 = charge2.create_bond()
        bond2.amount = "$100"

        info = self.extract("text_label.html", "text_label.yaml")
        self.assertEqual(expected_info, info)

    def test_th_rows(self):
        """Tests a yaml file with <th> keys in rows."""
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.race = "WHITE"
        person.gender = "M"

        info = self.extract("th_rows.html", "th_rows.yaml")
        self.assertEqual(expected_info, info)

    def test_content_is_not_modified(self):
        """Tests that the HtmlDataExtractor does not mutate |content|."""
        key_mapping_file = "../testdata/data_extractor/yaml/text_label.yaml"
        key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
        extractor = HtmlDataExtractor(key_mapping_file)

        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.birthdate = "1/1/1111"

        html_contents = html.fromstring("<html><div>DOB: 1/1/1111</div></html>")
        info = extractor.extract_and_populate_data(html_contents)

        self.assertEqual(expected_info, info)
        self.assertFalse(html_contents.cssselect("td"))

    def test_cell_ordering(self):
        """Tests that the HtmlDataExtractor handles 'th' and 'td' cells in the
        correct order."""
        expected_info = IngestInfo()
        expected_info.create_person(birthdate="A")
        expected_info.create_person(birthdate="B")
        expected_info.create_person(birthdate="C")

        info = self.extract("mixed_cells.html", "good_table.yaml")
        self.assertEqual(expected_info.people[0], info.people[0])

    def test_no_multi_key_parent(self):
        """Tests that parent classes are created properly when a field is
        scraped whose parent is a multi-key class that has not been scraped. In
        this example, charges are multi-key classes, but a bond is scraped from
        a booking with no charge information."""
        expected_info = IngestInfo()
        charge = expected_info.create_person().create_booking().create_charge()
        charge.create_bond(bond_id="1111")

        # The extractor will warn that 'Charge Description' cannot be found.
        # This is necessary because we need a field under multi_key_mappings
        # so that charge is treated as a multi_key class.
        info = self.extract("no_charges.html", "charge_multi_key.yaml")
        self.assertEqual(expected_info, info)

    def test_one_to_many(self):
        key_mapping_file = "../testdata/data_extractor/yaml/one_to_many.yaml"
        key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
        extractor = HtmlDataExtractor(key_mapping_file)

        expected_info = IngestInfo()
        charge = expected_info.create_person().create_booking().create_charge()
        charge.create_sentence(min_length="1 day", max_length="1 day")

        html_contents = html.fromstring("<td>Sentence Length</td><td>1 day</td>")
        info = extractor.extract_and_populate_data(html_contents)
        self.assertEqual(expected_info, info)

    def test_child_first(self):
        """Tests that in multi_key mappings (columns in a table), parent
        objects are created where needed."""
        expected_info = IngestInfo()
        p = expected_info.create_person()
        p.create_booking(admission_date="111").create_charge(name="AAA")
        p.create_booking(admission_date="222").create_charge(name="BBB")
        info = self.extract("child_first.html", "child_first.yaml")
        self.assertEqual(expected_info, info)

    def test_single_page_roster(self):
        """Tests that bookings are not treated as multi-key classes,
        i.e. we assume that a person has at most one booking if they are
        listed in columns."""
        expected_info = IngestInfo()
        p1 = expected_info.create_person(full_name="PERSON ONE", birthdate="1/1/1111")
        p1.create_booking(booking_id="NUMBER ONE")
        p2 = expected_info.create_person(full_name="PERSON TWO", birthdate="2/2/2222")
        p2.create_booking(booking_id="NUMBER TWO")
        p3 = expected_info.create_person(full_name="PERSON THREE", birthdate="3/3/3333")
        p3.create_booking(booking_id="NUMBER THREE")
        info = self.extract("single_page_roster.html", "single_page_roster.yaml")
        self.assertEqual(expected_info, info)

    def test_bond_multi_key(self):
        expected_info = IngestInfo()
        booking = expected_info.create_person().create_booking()
        booking.create_charge().create_bond(bond_id="1", amount="10")
        booking.create_charge().create_bond(bond_id="2", amount="20")
        booking.create_charge().create_bond(bond_id="3", amount="30")

        info = self.extract("bonds.html", "bonds.yaml")
        self.assertEqual(expected_info, info)

    def test_three_levels_multi_key(self):
        expected_info = IngestInfo()
        p = expected_info.create_person()
        b1 = p.create_booking(admission_date="01/01/2011", release_date="02/02/2012")
        b1.create_charge(name="Charge1").create_bond(
            amount="$1.00", bond_agent="AGENT 1"
        )
        b2 = p.create_booking(admission_date="03/03/2013")
        b2.create_charge(name="Charge2").create_bond(amount="$2.00")
        b3 = p.create_booking(admission_date="03/03/2013")
        b3.create_charge(name="Charge3").create_bond(amount="$3.00")
        b4 = p.create_booking(admission_date="03/03/2013")
        b4.create_charge(name="Charge4").create_bond(amount="$4.00")

        info = self.extract(
            "three_levels_multi_key.html", "three_levels_multi_key.yaml"
        )
        self.assertEqual(expected_info, info)
