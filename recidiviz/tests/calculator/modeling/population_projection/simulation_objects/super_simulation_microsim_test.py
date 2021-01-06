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
"""Test the MicroSuperSimulation object"""

import unittest
from datetime import datetime
from mock import patch
import pandas as pd

from recidiviz.tests.calculator.modeling.population_projection.simulation_objects.super_simulation_test \
    import get_inputs_path
from recidiviz.calculator.modeling.population_projection.simulations.super_simulation_factory import \
    SuperSimulationFactory
# pylint: disable=unused-argument

outflows_data = pd.DataFrame({
    'compartment': ['PRETRIAL'] * 12,
    'outflow_to': ['PRISON'] * 12,
    'time_step': [datetime(2020, i, 1) for i in range(7, 13)] * 2,
    'state_code': ['test_state'] * 12,
    'run_date': [datetime(2021, 1, 1)] * 12,
    'gender': ['MALE'] * 6 + ['FEMALE'] * 6,
    'total_population': [100] + [100 + 2 * i for i in range(5)] + [10] + [10 + i for i in range(5)]
})

transitions_data = pd.DataFrame({
    'compartment': ['PRISON', 'PRISON', 'RELEASE', 'RELEASE'] * 2,
    'outflow_to': ['RELEASE', 'RELEASE', 'PRISON', 'RELEASE'] * 2,
    'compartment_duration': [3, 5, 3, 50] * 2,
    'state_code': ['test_state'] * 8,
    'run_date': [datetime(2021, 1, 1)] * 8,
    'gender': ['MALE'] * 4 + ['FEMALE'] * 4,
    'total_population': [0.6, 0.4, 0.3, 0.7] * 2
})

remaining_sentence_data = pd.DataFrame({
    'compartment': ['PRISON', 'PRISON', 'RELEASE', 'RELEASE'] * 2,
    'outflow_to': ['RELEASE', 'RELEASE', 'PRISON', 'RELEASE'] * 2,
    'compartment_duration': [1, 2, 1, 50] * 2,
    'state_code': ['test_state'] * 8,
    'run_date': [datetime(2021, 1, 1)] * 8,
    'gender': ['MALE'] * 4 + ['FEMALE'] * 4,
    'total_population': [60, 40, 10, 90] * 2
})

total_population_data = pd.DataFrame({
    'compartment': ['PRISON', 'RELEASE'] * 2,
    'time_step': [datetime(2021, 1, 1)] * 4,
    'state_code': ['test_state'] * 4,
    'run_date': [datetime(2021, 1, 1)] * 4,
    'gender': ['MALE'] * 2 + ['FEMALE'] * 2,
    'total_population': [300, 500, 30, 50]
})

data_dict = {
    'test_outflows': outflows_data,
    'test_transitions': transitions_data,
    'test_total_population': total_population_data,
    'test_remaining_sentences': remaining_sentence_data,
    'test_total_jail_population': pd.DataFrame(columns=['state_code']),
    'test_total_out_of_state_supervision_population': pd.DataFrame(columns=['state_code'])
}


def mock_load_table_from_big_query(project_id: str, dataset: str, table_name: str, state_code: str) -> pd.DataFrame:
    return data_dict[table_name][data_dict[table_name]['state_code'] == state_code]


def mock_load_table_from_big_query_no_remaining_data(project_id: str, dataset: str, table_name: str,
                                                     state_code: str) -> pd.DataFrame:
    if table_name == 'test_remaining_sentences':
        table_name = 'test_transitions'
    return data_dict[table_name][data_dict[table_name]['state_code'] == state_code]


class TestMicroSuperSimulation(unittest.TestCase):
    """Test the SuperSimulation object runs correctly"""

    @patch('recidiviz.calculator.modeling.population_projection.ignite_bq_utils.load_ignite_table_from_big_query',
           mock_load_table_from_big_query)
    def setUp(self):
        self.microsim = SuperSimulationFactory.build_super_simulation(get_inputs_path(
                'super_simulation_microsim_model_inputs.yaml'))
        self.microsim.simulate_baseline(['PRISON'])

    def test_microsim_data_hydrated(self):
        """Tests microsimulation are properly ingesting data from BQ"""
        self.assertFalse(self.microsim.data_dict['outflows_data'].empty)
        self.assertFalse(self.microsim.data_dict['transitions_data'].empty)
        self.assertFalse(self.microsim.data_dict['total_population_data'].empty)
        self.assertFalse(self.microsim.data_dict['remaining_sentence_data'].empty)

    @patch('recidiviz.calculator.modeling.population_projection.ignite_bq_utils.load_ignite_table_from_big_query',
           mock_load_table_from_big_query_no_remaining_data)
    def test_using_remaining_sentences_reduces_prison_population(self):
        """Tests microsim is using remaining sentence data in the right way"""
        microsim = SuperSimulationFactory.build_super_simulation(get_inputs_path(
            'super_simulation_microsim_model_inputs.yaml'))
        microsim.simulate_baseline(['PRISON'])

        # get time before starting cohort filters out of prison
        affected_time_frame = self.microsim.data_dict['transitions_data'][
            self.microsim.data_dict['transitions_data'].compartment == 'PRISON'].compartment_duration.max()

        # get projected prison population from simulation substituting transitions data for remaining sentences
        substitute_outputs = microsim.output_data['baseline']
        substitute_prison_population = substitute_outputs[
            (substitute_outputs.compartment == 'PRISON')
            & (substitute_outputs.time_step > microsim.user_inputs['start_time_step'])
            & (substitute_outputs.time_step - microsim.user_inputs['start_time_step'] < affected_time_frame)
        ].groupby('time_step').sum().total_population

        # get projected prison population from regular simulation
        regular_outputs = self.microsim.output_data['baseline']
        regular_prison_population = regular_outputs[
            (regular_outputs.compartment == 'PRISON')
            & (regular_outputs.time_step > self.microsim.user_inputs['start_time_step'])
            & (regular_outputs.time_step - self.microsim.user_inputs['start_time_step'] < affected_time_frame)
        ].groupby('time_step').sum().total_population

        self.assertTrue((substitute_prison_population > regular_prison_population).all())
