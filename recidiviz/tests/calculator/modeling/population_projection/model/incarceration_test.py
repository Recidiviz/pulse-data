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
"""Test the IncarceratedTransitions object"""
import unittest
import pandas as pd
from recidiviz.calculator.modeling.population_projection.incarceration_transitions import IncarceratedTransitions


class TestIncarceratedPopulation(unittest.TestCase):

    def setUp(self):
        self.test_incarceration_data = pd.DataFrame({
            'compartment_duration': [1, 1, 2, 2.5, 10],
            'total_population': [4, 2, 2, 4, 3],
            'inflow_to': ['supervision', 'release', 'supervision', 'release', 'release']
        })

        self.compartment_policies = pd.DataFrame(columns=[
            'compartment', 'sub_population', 'retroactive_policies', 'non_retroactive_policies'])

        self.incarceration_transition_table = IncarceratedTransitions(['release'])
        self.incarceration_transition_table.initialize_transition_table(self.test_incarceration_data)
        self.incarceration_transition_table.initialize(self.compartment_policies)

        self.historical_data = pd.DataFrame({
            2015: {'jail': 2, 'prison': 2},
            2016: {'jail': 1, 'prison': 0},
            2017: {'jail': 1, 'prison': 1}
        })
