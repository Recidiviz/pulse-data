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
"""Test the ReleaseTransitions object"""

import unittest
import pandas as pd
from recidiviz.calculator.modeling.population_projection.release_transitions import ReleasedTransitions


class TestReleasedTransitionTable(unittest.TestCase):
    """Test the ReleaseTransitions class runs correctly"""

    def setUp(self):
        self.test_data = pd.DataFrame({
            'compartment_duration': [1, 1, 2, 2.5, 10],
            'total_population': [4, 2, 2, 4, 3],
            'outflow_to': ['jail', 'prison', 'jail', 'prison', 'prison'],
            'compartment': ['test_compartment'] * 5
        })

        self.compartment_policies = []

    def test_transition_table_rejects_impossible_large_probabilities(self):
        compartment_transitions = ReleasedTransitions(self.test_data)
        compartment_transitions.initialize_transition_table()
        compartment_transitions.initialize(self.compartment_policies)
