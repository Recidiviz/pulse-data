# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests functionality of MetricPopulationType functions"""

import re
import unittest

from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulation,
    MetricPopulationType,
)


class MetricPopulationsByTypeTest(unittest.TestCase):
    # check that population_type = key value
    def test_population_type_matches_key(self) -> None:
        for key, value in METRIC_POPULATIONS_BY_TYPE.items():
            self.assertEqual(
                value.population_type,
                key,
                "MetricPopulation `population_type` does not match key value.",
            )

    # check that population_name_short only has valid character types
    def test_population_name_short_char_types(self) -> None:
        for _, value in METRIC_POPULATIONS_BY_TYPE.items():
            if not re.match(r"^\w+$", value.population_name_short):
                raise ValueError(
                    "All characters in MetricPopulationType value must be alphanumeric or underscores."
                )


class MetricPopulationTest(unittest.TestCase):
    def test_get_conditions_query_string(self) -> None:
        my_population = MetricPopulation(
            population_type=MetricPopulationType.INCARCERATION,
            conditions_dict={
                "gender": ["TRANS_FEMALE", "TRANS_MALE"],
                "start_reason": ["NEW_ADMISSION"],
            },
        )
        query_string = my_population.get_conditions_query_string()
        expected_query_string = """gender IN ("TRANS_FEMALE", "TRANS_MALE")
 AND start_reason IN ("NEW_ADMISSION")"""
        self.assertEqual(query_string, expected_query_string)
