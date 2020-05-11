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
"""Tests the functions in the metric_utils file."""
import unittest
import pytest

from recidiviz.calculator.pipeline.utils.metric_utils import MetricMethodologyType, json_serializable_metric_key
from recidiviz.common.constants.person_characteristics import Gender, Race, Ethnicity


class TestJsonSerializableMetricKey(unittest.TestCase):
    """Tests the json_serializable_metric_key function."""
    def test_json_serializable_metric_key(self):
        metric_key = {'gender': Gender.MALE,
                      'race': [Race.BLACK, Race.WHITE],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'race': 'BLACK,WHITE',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_OneRace(self):
        metric_key = {'gender': Gender.MALE,
                      'race': [Race.BLACK],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'race': 'BLACK',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_RaceEthnicity(self):
        metric_key = {'gender': Gender.MALE,
                      'race': [Race.BLACK],
                      'ethnicity': [Ethnicity.HISPANIC, Ethnicity.EXTERNAL_UNKNOWN],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'race': 'BLACK',
                           'ethnicity': 'HISPANIC,EXTERNAL_UNKNOWN',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_RaceEthnicityNone(self):
        # This should never happen due to the way this dictionary is constructed.
        metric_key = {'gender': Gender.MALE,
                      'race': [None],
                      'ethnicity': [None],
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'state_code': 'CA'}

        expected_output = {'gender': 'MALE',
                           'methodology': 'PERSON',
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        updated_metric_key = json_serializable_metric_key(metric_key)

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_metric_key_InvalidList(self):
        metric_key = {'invalid_list_key': ['list', 'values']}

        with pytest.raises(ValueError) as e:
            json_serializable_metric_key(metric_key)

            self.assertEqual(e, "Unexpected list in metric_key for key: invalid_list_key")
