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
"""Test the ShellCompartment object"""
import unittest
import pandas as pd

from recidiviz.calculator.modeling.population_projection.full_compartment import FullCompartment
from recidiviz.calculator.modeling.population_projection.incarceration_transitions import IncarceratedTransitions
from recidiviz.calculator.modeling.population_projection.shell_compartment import ShellCompartment


class TestShellCompartment(unittest.TestCase):
    """Test the ShellCompartment class runs correctly"""

    def setUp(self):
        self.test_outflow_data = pd.DataFrame({
            'compartment_duration': [1, 1, 2, 2.5, 10],
            'total_population': [4, 2, 2, 4, 3],
            'outflow_to': ['jail', 'prison', 'jail', 'prison', 'prison'],
            'compartment': ['test'] * 5
        })

        self.historical_data = pd.DataFrame({
            2015: {'jail': 2, 'prison': 2},
            2016: {'jail': 1, 'prison': 0},
            2017: {'jail': 1, 'prison': 1}
        })

        self.compartment_policies = []

        self.test_transition_table = IncarceratedTransitions(self.test_outflow_data)
        self.test_transition_table.initialize_transition_table()
        self.test_transition_table.initialize(self.compartment_policies)

    def test_all_edges_fed_to(self):
        """ShellCompartments require edges to the compartments defined in the outflows_data"""
        starting_ts = 2015
        policy_ts = 2018
        test_shell_compartment = ShellCompartment(self.test_outflow_data, starting_ts=starting_ts, policy_ts=policy_ts,
                                                  tag='test_shell', constant_admissions=True, policy_list=[])
        test_full_compartment = FullCompartment(self.historical_data, self.test_transition_table,
                                                starting_ts=starting_ts, policy_ts=policy_ts, tag='test_compartment')
        with self.assertRaises(ValueError):
            test_shell_compartment.initialize_edges([test_shell_compartment, test_full_compartment])
