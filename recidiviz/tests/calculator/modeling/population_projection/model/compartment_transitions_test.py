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
"""Test the CompartmentTransitions object"""

import unittest
from copy import deepcopy
import pandas as pd
import numpy as np

from recidiviz.calculator.modeling.population_projection.compartment_transitions import CompartmentTransitions
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class CompartmentTransitionsStub(CompartmentTransitions):
    """Create a child class from the abstract CompartmentTransitions class"""
    def normalize_long_sentences(self):
        return super().normalize_long_sentences()

    def set_max_sentence_from_threshold(self, threshold_percentile: float):
        super()._set_max_sentence_from_threshold(threshold_percentile)


class TestTransitionTable(unittest.TestCase):
    """A base class for other transition test classes"""
    def setUp(self):
        self.test_data = pd.DataFrame({
            'compartment_duration': [1, 1, 2, 2.5, 10],
            'total_population': [4, 2, 2, 4, 3],
            'outflow_to': ['jail', 'prison', 'jail', 'prison', 'prison'],
            'compartment': ['test_compartment'] * 5
        })


class TestInitialization(TestTransitionTable):
    """Test the CompartmentTransitions initialization method"""

    def test_rejects_remaining_as_outflow(self):
        """Tests that compartment transitions won't accept 'remaining' as an outflow"""
        broken_test_data = self.test_data.copy()
        broken_test_data.loc[broken_test_data['outflow_to'] == 'jail', 'outflow_to'] = 'remaining'
        with self.assertRaises(ValueError):
            CompartmentTransitionsStub(broken_test_data)

    def test_normalize_transitions_requires_non_normalized_before_table(self):
        """Tests that transitory transitions table rejects a pre-normalized 'before' table"""
        transitions_table = CompartmentTransitionsStub(self.test_data)
        transitions_table.initialize_transition_table()
        transitions_table.transition_dfs['after'] = deepcopy(transitions_table.transition_dfs['before'])
        transitions_table.normalize_transitions(state='before')

        with self.assertRaises(ValueError):
            transitions_table.normalize_transitions(state='after',
                                                    before_table=transitions_table.transition_dfs['before'])

    def test_normalize_transitions_requires_generated_transition_table(self):
        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        # manually initializing without the self._generate_transition_tables()
        compartment_transitions.transition_dfs = {
            'before': {outflow: np.zeros(10) for outflow in compartment_transitions.outflows},
            'transitory': 0,
            'after_retroactive': 0,
            'after_non_retroactive': 0
        }
        with self.assertRaises(ValueError):
            compartment_transitions.normalize_transitions(state='after_retroactive')

    def test_normalize_transitions_requires_initialized_transition_table(self):
        with self.assertRaises(ValueError):
            compartment_transitions = CompartmentTransitionsStub(self.test_data)
            compartment_transitions.normalize_transitions(state='after_retroactive')

    def test_max_sentence_is_clipped(self):
        transitions_table = CompartmentTransitionsStub(self.test_data)

        transitions_table.set_max_sentence_from_threshold(0.98)
        self.assertEqual(10, transitions_table.max_sentence)

        transitions_table.set_max_sentence_from_threshold(0.8)
        self.assertEqual(3, transitions_table.max_sentence)


class TestTablePopulation(TestTransitionTable):
    """Test the population assumptions in the CompartmentTransitions object"""

    def test_rejects_data_with_negative_populations_or_durations(self):
        negative_duration_data = pd.DataFrame({
            'compartment_duration': [1, -1, 2, 2.5, 10],
            'total_population': [4, 2, 2, 4, 3],
            'outflow_to': ['jail', 'prison', 'jail', 'prison', 'prison'],
            'compartment': ['test_compartment'] * 5
        })

        negative_population_data = pd.DataFrame({
            'compartment_duration': [1, 1, 2, 2.5, 10],
            'total_population': [4, 2, 2, -4, 3],
            'outflow_to': ['jail', 'prison', 'jail', 'prison', 'prison'],
            'compartment': ['test_compartment'] * 5
        })
        compartment_transitions = CompartmentTransitionsStub(negative_duration_data)
        with self.assertRaises(ValueError):
            compartment_transitions.initialize_transition_table()

        compartment_transitions = CompartmentTransitionsStub(negative_population_data)
        with self.assertRaises(ValueError):
            compartment_transitions.initialize_transition_table()

    def test_results_independent_of_data_order(self):

        compartment_policies = [
            SparkPolicy(policy_fn=CompartmentTransitionsStub.test_retroactive_policy,
                        sub_population={'compartment': 'test_compartment'},
                        spark_compartment='jail',
                        apply_retroactive=True),
            SparkPolicy(policy_fn=CompartmentTransitionsStub.test_non_retroactive_policy,
                        sub_population={'compartment': 'test_compartment'},
                        spark_compartment='jail',
                        apply_retroactive=False),
        ]
        compartment_transitions_default = CompartmentTransitionsStub(self.test_data)
        compartment_transitions_shuffled = CompartmentTransitionsStub(self.test_data.sample(frac=1))

        compartment_transitions_default.initialize_transition_table()
        compartment_transitions_default.initialize(compartment_policies)

        compartment_transitions_shuffled.initialize_transition_table()
        compartment_transitions_shuffled.initialize(compartment_policies)

        self.assertEqual(compartment_transitions_default, compartment_transitions_shuffled)

    def test_non_retroactive_policy_cannot_affect_retroactive_table(self):
        compartment_policies = [
            SparkPolicy(policy_fn=CompartmentTransitionsStub.test_retroactive_policy,
                        sub_population={'compartment': 'test_compartment'},
                        spark_compartment='jail',
                        apply_retroactive=False)
        ]

        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_transitions.initialize_transition_table()
        with self.assertRaises(ValueError):
            compartment_transitions.initialize(compartment_policies)
