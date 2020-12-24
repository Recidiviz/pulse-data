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
import pandas as pd
from pandas.testing import assert_frame_equal
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

    def test_compartments_must_match(self):
        test_configuration = \
            open('recidiviz/tests/calculator/modeling/population_projection/simulation_objects/test_configurations/super_simulation_mismatched_compartments.yaml')
        with self.assertRaises(ValueError):
            SuperSimulation(test_configuration)

    def test_microsim_correct_data_gets_passed_down(self):
        """
        tests that microsim input data is passed through the simulation objects appropriately and ends up in
            the correct compartments
        """
        test_configuration = open(
            'recidiviz/tests/calculator/modeling/population_projection/simulation_objects/test_configurations/super_simulation_microsim_model_inputs.yaml'
        )
        test_microsim = SuperSimulation(test_configuration)
        test_microsim.simulate_baseline('pretrial', 'prison')

        expected_transition_data = pd.DataFrame(columns=['compartment', 'compartment_duration', 'offense_group',
                                                         'outflow_to', 'total_population', 'time_step',
                                                         'remaining_duration'])
        expected_total_population_data = expected_transition_data.copy()

        expected_transition_data['compartment_duration'] = [1.5, 1.75, 2]
        expected_transition_data['compartment'] = 'prison'
        expected_transition_data['outflow_to'] = 'release'
        expected_transition_data['offense_group'] = 'DRUG'
        expected_transition_data['total_population'] = [1881, 1484, 5930]

        expected_remaining_sentences_data = expected_transition_data.copy()
        expected_remaining_sentences_data['compartment_duration'] -= 0.5
        expected_remaining_sentences_data['remaining_duration'] = True

        expected_total_population_data['time_step'] = range(-5, 1)
        expected_total_population_data['compartment'] = 'prison'
        expected_total_population_data['offense_group'] = 'DRUG'
        expected_total_population_data['total_population'] = [2000, 2100, 2050, 2011, 2110, 2112]

        simulation_population_data = \
            test_microsim.pop_simulations['baseline'].sub_simulations['DRUG'].total_population_data
        simulation_transition_data = test_microsim.pop_simulations['baseline'].sub_simulations['DRUG'].transitions_data

        assert_frame_equal(expected_total_population_data.reset_index(drop=True).astype('object'),
                           simulation_population_data[simulation_population_data.compartment == 'prison'].reset_index(
                             drop=True).astype('object'))
        assert_frame_equal(expected_remaining_sentences_data.reset_index(drop=True).astype('object'),
                           simulation_transition_data[simulation_transition_data.compartment == 'prison'].reset_index(
                               drop=True).astype('object'))

        self.assertTrue(test_microsim.pop_simulations['baseline'].sub_simulations['DRUG'].simulation_compartments['prison']\
            .outflows_data.empty)
