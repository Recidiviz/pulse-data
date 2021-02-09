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
from functools import partial
from mock import patch
import pandas as pd
from pandas.util.testing import assert_frame_equal

from recidiviz.tests.calculator.modeling.population_projection.simulation_objects.super_simulation_test \
    import get_inputs_path
from recidiviz.calculator.modeling.population_projection.simulations.super_simulation_factory import \
    SuperSimulationFactory
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.simulations.compartment_transitions import \
    CompartmentTransitions

outflows_data = pd.DataFrame({
    'compartment': ['PRETRIAL'] * 12,
    'outflow_to': ['PRISON'] * 12,
    'time_step': list(range(5, 11)) * 2,
    'simulation_tag': ['test_data'] * 12,
    'crime_type': ['NONVIOLENT'] * 6 + ['VIOLENT'] * 6,
    'total_population': [100] + [100 + 2 * i for i in range(5)] + [10] + [10 + i for i in range(5)]
})

transitions_data = pd.DataFrame({
    'compartment': ['PRISON', 'PRISON', 'RELEASE', 'RELEASE'] * 2,
    'outflow_to': ['RELEASE', 'RELEASE', 'PRISON', 'RELEASE'] * 2,
    'compartment_duration': [3, 5, 3, 50] * 2,
    'simulation_tag': ['test_data'] * 8,
    'crime_type': ['NONVIOLENT'] * 4 + ['VIOLENT'] * 4,
    'total_population': [0.6, 0.4, 0.3, 0.7] * 2
})

total_population_data = pd.DataFrame({
    'compartment': ['PRISON', 'RELEASE'] * 2,
    'time_step': [9] * 4,
    'simulation_tag': ['test_data'] * 4,
    'crime_type': ['NONVIOLENT'] * 2 + ['VIOLENT'] * 2,
    'total_population': [300, 500, 30, 50]
})

data_dict = {
    'outflows_data_raw': outflows_data,
    'transitions_data_raw': transitions_data,
    'total_population_data_raw': total_population_data
}


def mock_load_table_from_big_query(table_name: str, simulation_tag: str) -> pd.DataFrame:
    return data_dict[table_name][data_dict[table_name].simulation_tag == simulation_tag]


class TestMacroSuperSimulation(unittest.TestCase):
    """Test the SuperSimulation object runs correctly"""

    @patch('recidiviz.calculator.modeling.population_projection.spark_bq_utils.load_spark_table_from_big_query',
           mock_load_table_from_big_query)
    def setUp(self):
        self.macrosim = SuperSimulationFactory.build_super_simulation(get_inputs_path(
                'super_simulation_data_ingest.yaml'))

    @patch('recidiviz.calculator.modeling.population_projection.spark_bq_utils.load_spark_table_from_big_query',
           mock_load_table_from_big_query)
    def test_reference_year_must_be_integer_time_steps_from_start_year(self):
        """Tests macrosimulation enforces compatibility of start year and time step"""
        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(get_inputs_path(
                'super_simulation_broken_start_year_model_inputs.yaml'))
        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(get_inputs_path(
                'super_simulation_broken_time_step_model_inputs.yaml'))

    @patch('recidiviz.calculator.modeling.population_projection.spark_bq_utils.load_spark_table_from_big_query',
           mock_load_table_from_big_query)
    def test_macrosim_data_hydrated(self):
        """Tests macrosimulation are properly ingesting data from BQ"""
        self.assertFalse(self.macrosim.data_dict['outflows_data'].empty)
        self.assertFalse(self.macrosim.data_dict['transitions_data'].empty)
        self.assertFalse(self.macrosim.data_dict['total_population_data'].empty)

    def test_cost_multipliers_multiplicative(self):

        # test doubling multiplier doubles costs
        policy_function = partial(CompartmentTransitions.apply_reduction,
                                  reduction_df=pd.DataFrame({'outflow': ['RELEASE'], 'reduction_size': [0.5],
                                                             'affected_fraction': [0.75]}),
                                  reduction_type='*',
                                  retroactive=True)
        cost_multipliers = pd.DataFrame({'crime_type': ['NONVIOLENT', 'VIOLENT'], 'multiplier': [2, 2]})

        policy_list = [SparkPolicy(policy_fn=policy_function,
                                   spark_compartment='PRISON',
                                   sub_population={'crime_type': crime_type},
                                   apply_retroactive=True) for crime_type in ['NONVIOLENT', 'VIOLENT']]
        spending_diff_scaled, _, spending_diff_non_cumulative_scaled = \
            self.macrosim.simulate_policy(policy_list, 'PRISON', cost_multipliers)
        spending_diff, _, spending_diff_non_cumulative = \
            self.macrosim.simulate_policy(policy_list, 'PRISON')
        assert_frame_equal(spending_diff * 2, spending_diff_scaled)
        assert_frame_equal(spending_diff_non_cumulative * 2, spending_diff_non_cumulative_scaled)

        # same test but for only one subgroup
        partial_cost_multipliers_double = pd.DataFrame({'crime_type': ['NONVIOLENT'], 'multiplier': [2]})
        partial_cost_multipliers_triple = pd.DataFrame({'crime_type': ['NONVIOLENT'], 'multiplier': [3]})
        spending_diff_partial_double, _, spending_diff_non_cumulative_partial_double = \
            self.macrosim.simulate_policy(policy_list, 'PRISON', partial_cost_multipliers_double)
        spending_diff_partial_triple, _, spending_diff_non_cumulative_partial_triple = \
            self.macrosim.simulate_policy(policy_list, 'PRISON', partial_cost_multipliers_triple)

        assert_frame_equal((spending_diff_partial_triple - spending_diff),
                           (spending_diff_partial_double - spending_diff) * 2)
        assert_frame_equal((spending_diff_non_cumulative_partial_triple - spending_diff_non_cumulative),
                           (spending_diff_non_cumulative_partial_double - spending_diff_non_cumulative) * 2)
