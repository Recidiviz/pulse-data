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
"""Test the PopulationSimulation object"""
import unittest
from copy import deepcopy
import pandas as pd
from pandas.testing import assert_index_equal

from recidiviz.calculator.modeling.population_projection.population_simulation import PopulationSimulation
from recidiviz.calculator.modeling.population_projection.incarceration_transitions import IncarceratedTransitions
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class TestPopulationSimulation(unittest.TestCase):
    """Test the PopulationSimulation class runs correctly"""

    def setUp(self):
        self.test_outflows_data = pd.DataFrame({
            'total_population': [4, 2, 2, 4, 3],
            'crime': ['NAR'] * 5,
            'outflow_to': ['supervision', 'prison', 'supervision', 'prison', 'prison'],
            'compartment': ['prison', 'supervision', 'prison', 'pretrial', 'pretrial'],
            'time_step': [0] * 5
        })

        self.test_transitions_data = pd.DataFrame({
            'compartment_duration': [1, 1, 2],
            'total_population': [4, 2, 2],
            'crime': ['NAR'] * 3,
            'outflow_to': ['supervision', 'prison', 'supervision'],
            'compartment': ['prison', 'supervision', 'prison'],
            'time_step': [0] * 3
        })

        self.test_total_population_data = pd.DataFrame({
            'total_population': [10] * 5,
            'compartment': ['prison'] * 5,
            'crime': ['NAR'] * 5,
            'time_step': range(-4, 1)
        })

        self.user_inputs = {
            'projection_time_steps': 10,
            'policy_time_step': 2,
            'start_time_step': 0,
            'policy_list': [],
            'constant_admissions': True,
            'speed_run': False
        }
        self.simulation_architecture = {
            'pretrial': None, 'prison': 'incarcerated', 'supervision': 'released'
        }

    def test_disaggregation_axes_must_be_in_data_dfs(self):
        test_outflows_data = self.test_outflows_data.drop('crime', axis=1)

        test_transitions_data = self.test_transitions_data.drop('crime', axis=1)

        test_total_population_data = self.test_total_population_data.drop('crime', axis=1)

        with self.assertRaises(ValueError):
            population_simulation = PopulationSimulation()
            population_simulation.simulate_policies(test_outflows_data,
                                                    self.test_transitions_data,
                                                    test_total_population_data,
                                                    self.simulation_architecture,
                                                    ['crime'],
                                                    self.user_inputs)

        with self.assertRaises(ValueError):
            population_simulation = PopulationSimulation()
            population_simulation.simulate_policies(self.test_outflows_data,
                                                    self.test_transitions_data,
                                                    test_total_population_data,
                                                    self.simulation_architecture,
                                                    ['crime'],
                                                    self.user_inputs)

        with self.assertRaises(ValueError):
            population_simulation = PopulationSimulation()
            population_simulation.simulate_policies(test_outflows_data,
                                                    self.test_transitions_data,
                                                    self.test_total_population_data,
                                                    self.simulation_architecture,
                                                    ['crime'],
                                                    self.user_inputs)

        with self.assertRaises(ValueError):
            population_simulation = PopulationSimulation()
            population_simulation.simulate_policies(self.test_outflows_data,
                                                    test_transitions_data,
                                                    self.test_total_population_data,
                                                    self.simulation_architecture,
                                                    ['crime'],
                                                    self.user_inputs)

    def test_simulation_forces_complete_user_inputs_dict(self):

        for i in self.user_inputs:
            test_user_inputs = deepcopy(self.user_inputs)
            test_user_inputs.pop(i)
            with self.assertRaises(ValueError):
                population_simulation = PopulationSimulation()
                population_simulation.simulate_policies(self.test_outflows_data, self.test_transitions_data,
                                                        self.test_total_population_data, self.simulation_architecture,
                                                        ['crime'], test_user_inputs)

    def test_microsim_requires_empty_policy_list(self):
        population_simulation = PopulationSimulation()
        with self.assertRaises(ValueError):
            user_inputs = deepcopy(self.user_inputs)
            user_inputs['policy_list'] = [SparkPolicy(IncarceratedTransitions.test_non_retroactive_policy,
                                                      'supervision', {'crime': 'NAR'})]
            population_simulation.simulate_policies(
                outflows_data=self.test_outflows_data,
                transitions_data=self.test_transitions_data,
                total_population_data=self.test_total_population_data,
                simulation_compartments=self.simulation_architecture,
                disaggregation_axes=['crime'],
                user_inputs=user_inputs,
                microsim=True,
                microsim_data=self.test_outflows_data
            )

    def test_baseline_with_backcast_projection_on(self):
        """Assert that the simulation results has negative time steps when the back-cast is enabled"""
        population_simulation = PopulationSimulation()
        projection = population_simulation.simulate_policies(
            outflows_data=self.test_outflows_data,
            transitions_data=self.test_transitions_data,
            total_population_data=self.test_total_population_data,
            simulation_compartments=self.simulation_architecture,
            disaggregation_axes=['crime'],
            user_inputs=self.user_inputs,
        )

        assert_index_equal(projection.index.unique().sort_values(), pd.Int64Index(range(-4, 10)))

    def test_baseline_with_backcast_projection_off(self):
        """Assert that microsim simulation results only have positive time steps"""
        population_simulation = PopulationSimulation()
        projection = population_simulation.simulate_policies(
            outflows_data=self.test_outflows_data,
            transitions_data=self.test_transitions_data,
            total_population_data=self.test_total_population_data,
            simulation_compartments=self.simulation_architecture,
            disaggregation_axes=['crime'],
            user_inputs=self.user_inputs,
            microsim=True,
            microsim_data=self.test_transitions_data
        )

        assert_index_equal(projection.index.unique().sort_values(), pd.Int64Index(range(11)))
