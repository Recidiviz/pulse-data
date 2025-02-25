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

"""Tests for utils/params.py."""
import unittest
from typing import Dict

from werkzeug.datastructures import MultiDict
from recidiviz.ingest.scrape import constants
from recidiviz.utils import params


PARAMS: Dict = MultiDict([('region', 'us_mo'),
                          ('scrape_type', constants.ScrapeType.BACKGROUND),
                          ('region', 'us_wa'),
                          ('false_bool_param', 'False'),
                          ('true_bool_param', 'true'),
                          ('malformed_bool_param', 'asdf'),
                          ('empty_bool_param', '')])


class TestParams(unittest.TestCase):
    """Tests for params.py"""

    def test_get_str_param_value(self):
        self.assertEqual(params.get_str_param_value('region', PARAMS), 'us_mo')

    def test_get_str_param_values(self):
        self.assertEqual(
            params.get_str_param_values('region', PARAMS), ['us_mo', 'us_wa'])

    def test_get_str_param_value_default(self):
        self.assertEqual(
            params.get_str_param_value('foo', PARAMS, default='bar'), 'bar')

    def test_get_str_param_value_no_default(self):
        self.assertIsNone(params.get_str_param_value('foo', PARAMS))

    def test_get_str_param_value_explicitly_none_default(self):
        self.assertIsNone(
            params.get_str_param_value('foo', PARAMS, default=None))

    def test_get_bool_param_value(self):
        self.assertEqual(
            params.get_bool_param_value('false_bool_param', PARAMS), False)
        self.assertEqual(
            params.get_bool_param_value('true_bool_param', PARAMS), True)

    def test_get_bool_param_value_default(self):
        self.assertEqual(
            params.get_bool_param_value('foo', PARAMS, default=True), True)
        self.assertEqual(
            params.get_bool_param_value('foo', PARAMS, default=None), None)

    def test_get_bool_param_value_no_default(self):
        self.assertEqual(
            params.get_bool_param_value('foo', PARAMS), None)

    def test_get_bool_param_value_malformed(self):
        with self.assertRaises(ValueError):
            params.get_bool_param_value('malformed_bool_param', PARAMS)

        with self.assertRaises(ValueError):
            params.get_bool_param_value('empty_bool_param', PARAMS)
