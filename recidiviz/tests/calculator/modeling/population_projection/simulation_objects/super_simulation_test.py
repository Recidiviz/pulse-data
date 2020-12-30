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
"""Test the SuperSimulation object"""

import unittest
# import pandas as pd
# from pandas.testing import assert_frame_equal
from recidiviz.calculator.modeling.population_projection.super_simulation import SuperSimulation
# pylint: disable=line-too-long


class TestSuperSimulation(unittest.TestCase):
    """Test the SuperSimulation object runs correctly"""

    def test_reference_year_must_be_integer_time_steps_from_start_year(self):
        test_configuration_start_year = \
            open('recidiviz/tests/calculator/modeling/population_projection/simulation_objects/test_configurations/super_simulation_broken_start_year_model_inputs.yaml')
        test_configuration_time_step = \
            open('recidiviz/tests/calculator/modeling/population_projection/simulation_objects/test_configurations/super_simulation_broken_time_step_model_inputs.yaml')
        with self.assertRaises(ValueError):
            SuperSimulation(test_configuration_start_year)

        with self.assertRaises(ValueError):
            SuperSimulation(test_configuration_time_step)
