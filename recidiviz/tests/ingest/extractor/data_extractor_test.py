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

    # The scraper scrapes a charge row that just says 'Count=3'. This would
    # have to be removed separately by a child scraper.
    info.people[0].bookings[0].charges.pop()
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

    with pytest.raises(ValueError):
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

    with pytest.raises(ValueError):
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
