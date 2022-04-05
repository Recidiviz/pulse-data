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

"""Tests for ingest/extractor/csv_data_extractor.py"""
import csv
import os
import unittest
from typing import Callable

from recidiviz.ingest.extractor.csv_data_extractor import CsvDataExtractor, \
    IngestFieldCoordinates
from recidiviz.tests.ingest import fixtures


# pylint:disable=protected-access
class CsvDataExtractorTest(unittest.TestCase):
    """Tests for extracting data from CSV."""

    def test_ancestor_chain(self):
        extractor = _instantiate_extractor('standard_child_file_csv.yaml')
        rows = _get_content_as_csv('standard_child_file.csv')
        first_row = next(iter(rows))

        ancestor_chain = extractor._ancestor_chain(first_row, None)
        expected_chain = {'state_person': '52163'}
        self.assertEqual(expected_chain, ancestor_chain)

    def test_ancestor_chain_multiple_keys(self):
        extractor = _instantiate_extractor('multiple_ancestors.yaml')
        rows = _get_content_as_csv('multiple_ancestors.csv')
        first_row = next(iter(rows))

        ancestor_chain = extractor._ancestor_chain(first_row, None)
        expected_chain = {'state_person': '52163',
                          'state_sentence_group': '12345'}
        self.assertEqual(expected_chain, ancestor_chain)

    def test_ancestor_chain_no_ancestor_key(self):
        extractor = _instantiate_extractor('no_ancestor_key_csv.yaml')
        rows = _get_content_as_csv('standard_child_file.csv')
        first_row = next(iter(rows))

        coordinates = extractor._ancestor_chain(first_row, None)
        self.assertFalse(coordinates)

    def test_get_creation_args_no_override(self):
        extractor = _instantiate_extractor('standard_child_file_csv.yaml')
        rows = _get_content_as_csv('standard_child_file.csv')
        first_row = next(iter(rows))

        args = extractor._get_creation_args(first_row, 'IN_OUT_STATUS')
        expected_args = {'sentence_group_id': '113377'}
        self.assertEqual(expected_args, args)

    def test_get_creation_args_with_override(self):
        def _override(_row):
            return IngestFieldCoordinates('sentence_group',
                                          'sentence_group_id',
                                          'abcdef')

        extractor = _instantiate_extractor('standard_child_file_csv.yaml',
                                           primary_key_override=_override)
        rows = _get_content_as_csv('standard_child_file.csv')
        first_row = next(iter(rows))

        args = extractor._get_creation_args(first_row, 'IN_OUT_STATUS')
        expected_args = {'sentence_group_id': 'abcdef'}
        self.assertEqual(expected_args, args)

    def test_get_creation_args_not_for_current_field(self):
        extractor = _instantiate_extractor(
            'multiple_entity_types_in_row_csv.yaml')
        rows = _get_content_as_csv('multiple_entity_types_in_row.csv')
        first_row = next(iter(rows))

        args = extractor._get_creation_args(first_row, 'PAROLE_DATE')
        self.assertFalse(args)

    def test_get_creation_args_override_but_not_for_current_field(self):
        def _override(_row):
            return IngestFieldCoordinates('sentence_group',
                                          'sentence_group_id',
                                          'abcdef')

        extractor = _instantiate_extractor(
            'multiple_entity_types_in_row_csv.yaml',
            primary_key_override=_override)
        rows = _get_content_as_csv('multiple_entity_types_in_row.csv')
        first_row = next(iter(rows))

        args = extractor._get_creation_args(first_row, 'PAROLE_DATE')
        self.assertFalse(args)

    def test_get_creation_args_no_override_or_mapping(self):
        extractor = _instantiate_extractor('no_primary_keys_csv.yaml')
        rows = _get_content_as_csv('no_primary_keys.csv')
        first_row = next(iter(rows))

        args = extractor._get_creation_args(first_row, 'LAST_NAME')
        self.assertFalse(args)

    def test_get_creation_args_not_a_valid_lookup_key(self):
        extractor = _instantiate_extractor('standard_child_file_csv.yaml')
        rows = _get_content_as_csv('standard_child_file.csv')
        first_row = next(iter(rows))

        args = extractor._get_creation_args(first_row, 'AGY_LOC_ID')
        self.assertFalse(args)

    def test_primary_coordinates(self):
        extractor = _instantiate_extractor('standard_child_file_csv.yaml')
        rows = _get_content_as_csv('standard_child_file.csv')
        first_row = next(iter(rows))

        key = extractor._primary_coordinates(first_row)
        expected_key = IngestFieldCoordinates('sentence_group',
                                              'sentence_group_id',
                                              '113377')
        self.assertEqual(expected_key, key)

    def test_primary_coordinates_override(self):
        coordinates = IngestFieldCoordinates('sentence_group',
                                             'sentence_group_id',
                                             'abcdef')

        def _override(_row):
            return coordinates

        extractor = _instantiate_extractor('standard_child_file_csv.yaml',
                                           primary_key_override=_override)
        rows = _get_content_as_csv('standard_child_file.csv')
        first_row = next(iter(rows))

        key = extractor._primary_coordinates(first_row)
        expected_key = coordinates
        self.assertEqual(expected_key, key)

    def test_primary_coordinates_no_override_or_mapping(self):
        extractor = _instantiate_extractor('no_primary_keys_csv.yaml')
        rows = _get_content_as_csv('no_primary_keys.csv')
        first_row = next(iter(rows))

        key = extractor._primary_coordinates(first_row)
        self.assertIsNone(key)


def _instantiate_extractor(yaml_filename: str,
                           primary_key_override: Callable = None) \
        -> CsvDataExtractor:
    yaml_path = os.path.join(os.path.dirname(__file__),
                             '../testdata/data_extractor/yaml',
                             yaml_filename)
    return CsvDataExtractor(yaml_path,
                            primary_key_override_callback=primary_key_override)


def _get_content_as_csv(content_filename: str) -> csv.DictReader:
    content = fixtures.as_string('testdata/data_extractor/csv',
                                 content_filename)
    return csv.DictReader(content.splitlines())
