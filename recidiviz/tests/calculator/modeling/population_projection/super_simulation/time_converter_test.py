# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Test the TimeConverter object"""

import unittest
from datetime import datetime
import pandas as pd
from pandas.testing import assert_series_equal

from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)


class TestTimeConverter(unittest.TestCase):
    """Test the TimeConverter object runs correctly"""

    def setUp(self) -> None:
        self.test_converter = TimeConverter(2015.5, 0.1)

    def test_convert_year_to_time_step_matches_example_by_hand(self) -> None:
        """Make sure the arithmetic works out for converting year to time step"""
        year = 2022.0
        expected_time_step = 65

        self.assertEqual(
            expected_time_step, self.test_converter.convert_year_to_time_step(year)
        )

    def test_convert_year_to_time_step_breaks_when_year_not_integer_time_steps_from_reference_year(
        self,
    ) -> None:
        """Make sure TimeConverter raises if given a faulty year"""
        with self.assertRaises(ValueError):
            self.test_converter.convert_year_to_time_step(2020.05)

    def test_convert_time_steps_to_year_matches_example_by_hand(self) -> None:
        """Make sure the arithmetic works out for converting time steps to years"""
        time_steps = pd.Series([4, 5, 10])
        expected_years = pd.Series([2015.9, 2016.0, 2016.5])

        assert_series_equal(
            expected_years, self.test_converter.convert_time_steps_to_year(time_steps)
        )

    def test_convert_timestamp_to_time_step_breaks_with_non_monthly_time_step(
        self,
    ) -> None:
        """Make sure TimeConverter raises if trying to convert a timestamp having self.time_step != 1/12"""
        with self.assertRaises(ValueError):
            self.test_converter.convert_timestamp_to_time_step(datetime(2020, 1, 1))

    def test_convert_timestamp_to_time_step_matches_example_by_hand(self) -> None:
        """Make sure the arithmetic works out for converting timestamp to time step"""
        monthly_converter = TimeConverter(2015.5, 1 / 12)
        date = datetime(2020, 1, 1)
        expected_time_step = 54

        self.assertEqual(
            monthly_converter.convert_timestamp_to_time_step(date), expected_time_step
        )
