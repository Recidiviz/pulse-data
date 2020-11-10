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
"""Test the SubSimulation class"""

import unittest
import pandas as pd
from recidiviz.calculator.modeling.population_projection.sub_simulation import SubSimulation


class TestSubSimulation(unittest.TestCase):
    """Test the SubSimulation runs correctly"""

    def setUp(self):
        self.test_outflow_data = pd.DataFrame({
            'total_population': [4, 2, 2, 4, 3],
            'outflow_to': ['supervision', 'prison', 'supervision', 'prison', 'prison'],
            'compartment': ['prison', 'supervision', 'prison', 'pretrial', 'pretrial'],
            'time_step': [0, 0, 0, 0, 0]
        })

        self.test_transitions_data = pd.DataFrame({
            'compartment_duration': [1, 1, 5],
            'total_population': [4, 2, 2],
            'outflow_to': ['supervision', 'prison', 'supervision'],
            'compartment': ['prison', 'supervision', 'prison'],
        })

        self.test_total_population_data = pd.DataFrame({
            'total_population': [10] * 5,
            'compartment': ['prison'] * 5,
            'time_step': [-4, -3, -2, -1, 0],
        })

        self.test_architecture = {'pretrial': None, 'supervision': 'released',
                                  'prison': 'incarcerated'}

        self.compartment_policies = []

        self.test_user_inputs = {'start_time_step': 0, 'policy_time_step': 5, 'projection_time_steps': 10,
                                 'constant_admissions': False, 'speed_run': False}

    def test_total_population_data_must_include_start_ts(self):
        sparse_total_population_data = \
            self.test_total_population_data[self.test_total_population_data.time_step != 0]
        sim = SubSimulation(self.test_outflow_data,
                            self.test_transitions_data,
                            sparse_total_population_data,
                            self.test_architecture,
                            self.test_user_inputs,
                            self.compartment_policies,
                            0)
        with self.assertRaises(ValueError):
            sim.initialize()
