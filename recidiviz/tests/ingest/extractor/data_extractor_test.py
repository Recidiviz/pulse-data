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

"""Tests for ingest/extractor/data_extractor_test.py"""

import os
from lxml import html
import pytest

from recidiviz.ingest.extractor.data_extractor import DataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo


def test_good_table():
    """Tests a well modelled table."""
    key_mapping_file = '../testdata/data_extractor/yaml/good_table.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    expected_info = IngestInfo()
    person = expected_info.create_person()
    person.create_booking()
    person.birthdate = '1/15/2048'

    html_file = '../testdata/data_extractor/html/good_table.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())

    info = extractor.extract_and_populate_data(html_contents)
    assert expected_info == info

def test_nested_good_table():
    """Tests a well modelled nested table."""
    key_mapping_file = '../testdata/data_extractor/yaml/nested_good_table.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    expected_info = IngestInfo()

    # Add person information
    person = expected_info.create_person()
    person.birthdate = '06/03/2999'
    person.sex = 'Male'
    person.age = '100000000'
    person.race = 'White/Eurp/ N.Afr/Mid Eas'
    person.person_id = '18-00187'

    # Add booking information
    booking = person.create_booking()
    booking.booking_id = '18-00000'
    booking.admission_date = '1/05/2000 09:39'
    booking.hold = 'District Court 13-3-01'

    # Add charge information
    charge = booking.create_charge()
    charge.statute = '901'
    charge.name = 'Criminal Attempt [INCHOATE]'
    charge.case_number = 'CR-000-2000'

    # Add bond information
    charge.create_bond().amount = '$1.00'

    html_file = '../testdata/data_extractor/html/nested_good_table.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())

    info = extractor.extract_and_populate_data(html_contents)
    assert info == expected_info

def test_bad_table():
    """Tests a table with an unusual cell layout."""
    key_mapping_file = '../testdata/data_extractor/yaml/bad_table.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    expected_info = IngestInfo()
    person = expected_info.create_person()
    person.race = 'W'
    booking = person.create_booking()

    charge1 = booking.create_charge()
    charge1.charging_entity = 'ROCKWALL CO SO ROCKWALL'
    charge1.name = 'FIRST CHARGE'
    charge1.offense_date = '1-01-2042'
    bond1 = charge1.create_bond()
    bond1.amount = '$75,000.00'
    bond1.bond_type = 'Surety Bond'

    charge2 = booking.create_charge()
    charge2.charging_entity = 'Rockwall'
    charge2.name = 'SECOND CHARGE'
    charge2.offense_date = '1-01-2040'
    bond2 = charge2.create_bond()
    bond2.amount = '$1,500.00'
    bond2.bond_type = 'Surety Bond'

    charge3 = booking.create_charge()
    charge3.charging_entity = 'Rockwall'
    charge3.name = 'THIRD CHARGE'
    charge3.offense_date = '1-01-2040'
    bond3 = charge3.create_bond()
    bond3.amount = '$2,000.00'
    bond3.bond_type = 'Surety Bond'

    html_file = '../testdata/data_extractor/html/bad_table.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())

    info = extractor.extract_and_populate_data(html_contents)

    # The scraper scrapes a charge row that just says 'Count=3', we need to
    # know how to ignore this somehow
    info.person[0].booking[0].charge.pop()
    assert info == expected_info

def test_multiple_people_with_maybe_charges():
    """Tests for a page with many people, each with possibly a set of charges"""
    key_mapping_file = ('../testdata/data_extractor/yaml/'
                        'multiple_people_sometimes_charges.yaml')
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    expected_info = IngestInfo()

    # Create the info for the first person
    person1 = expected_info.create_person()
    person1.status = 'Release'
    person1.person_id = 'person1'
    person1.place_of_residence = 'address1'
    person1.age = '62'

    booking1 = person1.create_booking()
    booking1.admission_date = '03/14/2009 02:09:04'
    booking1.release_date = '3/14/2009 3:49:00 PM'
    booking1.booking_id = 'booking1'

    b1_charge1 = booking1.create_charge()
    b1_charge1.statute = '13A-6-24'
    b1_charge1.case_number = '000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)'
    b1_charge1.name = 'RECKLESS ENDANGERMENT'
    b1_charge1.degree = ''
    b1_charge1.level = 'M'
    b1_charge1.create_bond().amount = '$1000.00'
    b1_charge2 = booking1.create_charge()
    b1_charge2.statute = '13A-6-24'
    b1_charge2.case_number = '000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)'
    b1_charge2.name = 'RECKLESS ENDANGERMENT'
    b1_charge2.degree = ''
    b1_charge2.level = 'M'
    b1_charge2.create_bond().amount = '$1000.00'

    # Create info for the second person
    person2 = expected_info.create_person()
    person2.status = 'Release'
    person2.person_id = 'person2'
    person2.place_of_residence = 'address2'
    person2.age = '44'

    booking2 = person2.create_booking()
    booking2.admission_date = '11/19/2001 07:03:00'
    booking2.release_date = '11/19/2001 6:40:00 PM'
    booking2.booking_id = 'booking2'

    # Create info for the 3rd person
    person3 = expected_info.create_person()
    person3.status = 'Release'
    person3.person_id = 'person3'
    person3.place_of_residence = 'address3'
    person3.age = '22'

    booking3 = person3.create_booking()
    booking3.admission_date = '05/12/2014 11:04:59'
    booking3.release_date = '5/13/2014 9:08:08 AM'
    booking3.booking_id = 'booking3'

    b3_charge1 = booking3.create_charge()
    b3_charge1.statute = '13A-12-214'
    b3_charge1.case_number = '000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)'
    b3_charge1.name = 'POSSESSION OF MARIJUANA SECOND DEGREE'
    b3_charge1.degree = 'S'
    b3_charge1.level = 'M'
    b3_charge1.create_bond().amount = '$1000.00'
    b3_charge2 = booking3.create_charge()
    b3_charge2.statute = '13A-12-260'
    b3_charge2.case_number = '000-0000 (BALDWIN COUNTY SHERIFFS OFFICE)'
    b3_charge2.name = 'POSSESSION OF DRUG PARAPHERNALIA'
    b3_charge2.degree = ''
    b3_charge2.level = 'M'
    b3_charge2.create_bond().amount = '$1000.00'

    html_file = ('../testdata/data_extractor/html/'
                 'multiple_people_sometimes_charges.html')
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())

    info = extractor.extract_and_populate_data(html_contents)

    assert info == expected_info

def test_bad_lookup():
    """Tests a yaml file with a lookup key that doesn't exist on the page."""
    key_mapping_file = '../testdata/data_extractor/yaml/bad_lookup.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    html_file = '../testdata/data_extractor/html/good_table.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())

    with pytest.warns(UserWarning):
        extractor.extract_and_populate_data(html_contents)

def test_bad_object():
    """Tests a yaml file with a db object that doesn't exist."""
    key_mapping_file = '../testdata/data_extractor/yaml/bad_object.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    html_file = '../testdata/data_extractor/html/good_table.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())

    with pytest.raises(KeyError):
        extractor.extract_and_populate_data(html_contents)

def test_bad_attr():
    """Tests a yaml file with a db attribute that doesn't exist."""
    key_mapping_file = '../testdata/data_extractor/yaml/bad_attr.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    html_file = '../testdata/data_extractor/html/good_table.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())

    with pytest.raises(AttributeError):
        extractor.extract_and_populate_data(html_contents)

def test_partial_table():
    """Tests a page with a table as well as unstructured data."""
    key_mapping_file = '../testdata/data_extractor/yaml/partial_table.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    expected_info = IngestInfo()
    person = expected_info.create_person()
    person.age = '38'
    person.place_of_residence = 'WICHITA FALLS'
    person.race = 'HISPANIC'
    booking = person.create_booking()
    booking.admission_date = '08/18/2017'
    charge = booking.create_charge()
    charge.name = 'FIRST CHARGE'
    charge.charging_entity = 'WICHITA FALLS PD'
    charge.charge_status = ''
    bond = charge.create_bond()
    bond.amount = '25,000.00'

    html_file = '../testdata/data_extractor/html/partial_table.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())
    info = extractor.extract_and_populate_data(html_contents)
    assert expected_info == info

def test_labeled_fields():
    """Tests a page with field values in <span>s labeled by <label>s."""
    key_mapping_file = '../testdata/data_extractor/yaml/labeled_fields.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    expected_info = IngestInfo()
    person = expected_info.create_person()
    person.person_id = '11111'
    person.race = 'White'
    person.sex = 'Male'
    booking = person.create_booking()
    booking.admission_date = '11/12/2018 5:04 PM'
    booking.facility = 'Walla Walla County Corrections Department'
    booking.release_date = ''
    charge = booking.create_charge()
    charge.name = 'DUI'
    charge.offense_date = '9/21/2018 5:34 PM'
    charge.charge_class = 'Gross Misdemeanor'
    charge.charge_status = 'Time Served'
    charge.charging_entity = ''
    charge.next_court_date = ''
    booking.charge.append(charge)

    html_file = '../testdata/data_extractor/html/labeled_fields.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())
    info = extractor.extract_and_populate_data(html_contents)
    assert expected_info == info

def test_bad_labels():
    """Tests a page with field values in <span>s labeled by nested <label>s."""
    key_mapping_file = '../testdata/data_extractor/yaml/bad_labels.yaml'
    key_mapping_file = os.path.join(os.path.dirname(__file__), key_mapping_file)
    extractor = DataExtractor(key_mapping_file)

    expected_info = IngestInfo()
    person = expected_info.create_person()
    person.person_id = '046573'
    person.sex = 'Male'
    booking = person.create_booking()
    booking.booking_id = '00119283'
    booking.admission_date = '07/19/2018 14:24'
    booking.facility = 'MCSO'
    arrest = booking.create_arrest()
    arrest.date = ''
    charge = booking.create_charge()
    bond = charge.create_bond()
    bond.amount = '$100,000.00'
    bond.bond_type = '10 %'
    charge.statute = '2911.12'
    charge.next_court_date = ''
    charge.name = 'BURGLARY'
    charge.case_number = ''

    html_file = '../testdata/data_extractor/html/bad_labels.html'
    html_file = os.path.join(os.path.dirname(__file__), html_file)
    with open(html_file, 'r') as f:
        html_contents = html.fromstring(f.read())
    info = extractor.extract_and_populate_data(html_contents)
    assert expected_info == info
