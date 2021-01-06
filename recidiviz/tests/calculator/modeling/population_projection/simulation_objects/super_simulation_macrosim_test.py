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
"""Test the MacroSuperSimulation object"""

import unittest
from recidiviz.calculator.modeling.population_projection.super_simulation_macrosim import MacroSuperSimulation
from recidiviz.tests.calculator.modeling.population_projection.simulation_objects.super_simulation_test \
    import get_inputs_path


class TestMacroSuperSimulation(unittest.TestCase):
    """Test the SuperSimulation object runs correctly"""

    def test_reference_year_must_be_integer_time_steps_from_start_year(self):
        with open(get_inputs_path(
                'super_simulation_broken_start_year_model_inputs.yaml')) as test_configuration_start_year:
            with self.assertRaises(ValueError):
                MacroSuperSimulation(test_configuration_start_year)
        with open(get_inputs_path(
                'super_simulation_broken_time_step_model_inputs.yaml')) as test_configuration_time_step:
            with self.assertRaises(ValueError):
                MacroSuperSimulation(test_configuration_time_step)
