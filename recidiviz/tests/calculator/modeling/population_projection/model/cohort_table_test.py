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
"""Test the CohortTable object"""

import unittest
import pandas as pd

from recidiviz.calculator.modeling.population_projection.cohort_table import CohortTable


class TestCohortTable(unittest.TestCase):
    """Test the CohortTable object runs correctly"""

    def test_monotonic_decreasing_size(self):
        """Tests that cohort size can only decrease over time"""
        cohort = CohortTable(starting_ts=2000, transition_table_max_length=100)
        with self.assertRaises(ValueError):
            cohort.append_ts_end_count(
                cohort_sizes=pd.Series({i: 1 for i in range(1900, 2000)}),
                projection_ts=2001,
            )

    def test_duplicate_year_data_rejected(self):
        """Tests that yearly data added to cohort must be in a new year"""
        cohort = CohortTable(starting_ts=2000, transition_table_max_length=100)
        cohort.append_ts_end_count(
            cohort_sizes=pd.Series({i: 0 for i in range(1900, 2000)}),
            projection_ts=2000,
        )
        with self.assertRaises(ValueError):
            cohort.append_ts_end_count(
                cohort_sizes=pd.Series({i: 0 for i in range(1900, 2000)}),
                projection_ts=2000,
            )

    def test_cohort_happy_path(self):
        """Tests the Cohort can maintain the timeline data"""

        start_time = 2000
        cohort_size_list = [-10, -20, -30, -40, -50]
        cohort = CohortTable(starting_ts=2000, transition_table_max_length=1)
        for time_index, cohort_size in enumerate(cohort_size_list):
            cohort.append_ts_end_count(
                cohort_sizes=pd.Series({1999: cohort_size}),
                projection_ts=start_time + time_index,
            )

        for index, cohort_size in enumerate(cohort_size_list):
            self.assertEqual(cohort_size, cohort.get_cohort_timeline(1999).iloc[index])
