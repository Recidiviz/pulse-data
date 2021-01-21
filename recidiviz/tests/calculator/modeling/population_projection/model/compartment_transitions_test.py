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
from functools import partial
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np

from recidiviz.calculator.modeling.population_projection.compartment_transitions import CompartmentTransitions
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class CompartmentTransitionsStub(CompartmentTransitions):
    """Create a child class from the abstract CompartmentTransitions class"""
    def normalize_long_sentences(self):
        super().normalize_long_sentences()

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
        # manually initializing without the self._generate_transition_table()
        compartment_transitions.transition_dfs = {
            'before': pd.DataFrame({outflow: np.zeros(10) for outflow in compartment_transitions.outflows},
                                   index=range(1, 11)),
            'transitory': pd.DataFrame(),
            'after_retroactive': pd.DataFrame(),
            'after_non_retroactive': pd.DataFrame()
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


class TestTableHydration(TestTransitionTable):
    """Test the hydration assumptions in the CompartmentTransitions object"""

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
                        spark_compartment='test_compartment',
                        apply_retroactive=True),
            SparkPolicy(policy_fn=CompartmentTransitionsStub.test_non_retroactive_policy,
                        sub_population={'compartment': 'test_compartment'},
                        spark_compartment='test_compartment',
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
                        spark_compartment='test_compartment',
                        apply_retroactive=False)
        ]

        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_transitions.initialize_transition_table()
        with self.assertRaises(ValueError):
            compartment_transitions.initialize(compartment_policies)


class TestPolicyFunctions(TestTransitionTable):
    """Test the policy functions used for Spark modeling"""

    def test_unnormalized_table_inverse_of_normalize_table(self):
        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_transitions.initialize_transition_table()
        original_before_table = compartment_transitions.transition_dfs['before'].copy()
        # 'normalize' table (in the classical mathematical sense) to match scale of unnormalized table
        original_before_table /= original_before_table.sum().sum()

        compartment_transitions.normalize_transitions('before')
        compartment_transitions.unnormalize_table('before')
        assert_frame_equal(pd.DataFrame(original_before_table),
                           pd.DataFrame(compartment_transitions.transition_dfs['before']))

    def test_alternate_transitions_data_equal_to_differently_instantiated_transition_table(self):
        alternate_data = self.test_data.copy()
        alternate_data.compartment_duration *= 2
        alternate_data.total_population = 10 - alternate_data.total_population

        policy_function = SparkPolicy(
            policy_fn=partial(CompartmentTransitions.use_alternate_transitions_data,
                              alternate_historical_transitions=alternate_data,
                              retroactive=False),
            spark_compartment='test_compartment',
            sub_population={'sub_group': 'test_population'},
            apply_retroactive=False
        )

        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_transitions.initialize_transition_table()
        compartment_transitions.initialize([policy_function])

        alternate_data_transitions = CompartmentTransitionsStub(alternate_data)
        alternate_data_transitions.initialize_transition_table()
        alternate_data_transitions.initialize([])

        assert_frame_equal(compartment_transitions.transition_dfs['after_non_retroactive'],
                           alternate_data_transitions.transition_dfs['after_non_retroactive'])

    def test_preserve_normalized_outflow_behavior_preserves_normalized_outflow_behavior(self):
        compartment_policies = [
            SparkPolicy(policy_fn=CompartmentTransitionsStub.test_retroactive_policy,
                        sub_population={'compartment': 'test_compartment'},
                        spark_compartment='test_compartment',
                        apply_retroactive=True),
            SparkPolicy(policy_fn=partial(CompartmentTransitionsStub.preserve_normalized_outflow_behavior,
                                          outflows=['prison'], state='after_retroactive', before_state='before'),
                        sub_population={'compartment': 'test_compartment'},
                        spark_compartment='test_compartment',
                        apply_retroactive=True)
        ]

        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_transitions.initialize_transition_table()
        compartment_transitions.initialize(compartment_policies)

        baseline_transitions = CompartmentTransitionsStub(self.test_data)
        baseline_transitions.initialize_transition_table()
        baseline_transitions.initialize([])

        self.assertTrue((baseline_transitions.transition_dfs['after_retroactive']['prison'] ==
                         compartment_transitions.transition_dfs['after_retroactive']['prison']).all())

    def test_apply_reduction_with_trivial_reductions_doesnt_change_transition_table(self):

        policy_mul = partial(CompartmentTransitionsStub.apply_reduction,
                             reduction_df=pd.DataFrame({'outflow': ['prison'] * 2, 'affected_fraction': [0, 0.5],
                                                        'reduction_size': [0.5, 0]}),
                             reduction_type='*',
                             retroactive=False)

        policy_add = partial(CompartmentTransitionsStub.apply_reduction,
                             reduction_df=pd.DataFrame({'outflow': ['prison'] * 2, 'affected_fraction': [0, 0.5],
                                                        'reduction_size': [0.5, 0]}),
                             reduction_type='+',
                             retroactive=False)

        compartment_policies = [
            SparkPolicy(policy_mul, 'test_compartment', {'sub_group': 'test_population'}, False),
            SparkPolicy(policy_add, 'test_compartment', {'sub_group': 'test_population'}, False)
        ]

        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_transitions.initialize_transition_table()
        compartment_transitions.initialize(compartment_policies)

        assert_frame_equal(compartment_transitions.transition_dfs['before'],
                           compartment_transitions.transition_dfs['after_non_retroactive'])

    def test_apply_reduction_matches_example_by_hand(self):
        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_policy = [
            SparkPolicy(policy_fn=partial(CompartmentTransitionsStub.apply_reduction,
                                          reduction_df=pd.DataFrame({'outflow': ['prison'],
                                                                     'affected_fraction': [0.25],
                                                                     'reduction_size': [0.5]}),
                                          reduction_type='+',
                                          retroactive=True),
                        sub_population={'sub_group': 'test_population'},
                        spark_compartment='test_compartment',
                        apply_retroactive=True)
        ]

        expected_result = pd.DataFrame({
            'jail': [4, 2, 0, 0, 0, 0, 0, 0, 0, 0],
            'prison': [2, 0.5, 3.5, 0, 0, 0, 0, 0, 0.375, 2.625]
        }, index=range(1, 11), dtype=float)
        expected_result.index.name = 'compartment_duration'
        expected_result.columns.name = 'outflow_to'
        expected_result /= expected_result.sum().sum()

        compartment_transitions.initialize_transition_table()
        compartment_transitions.initialize(compartment_policy)
        compartment_transitions.unnormalize_table('after_retroactive')
        assert_frame_equal(round(compartment_transitions.transition_dfs['after_retroactive'], 8),
                           round(expected_result, 8))

    def test_reallocate_outflow_preserves_total_population(self):
        compartment_policies = [
            SparkPolicy(policy_fn=partial(CompartmentTransitionsStub.reallocate_outflow,
                                          reallocation_df=pd.DataFrame({'outflow': ['jail', 'jail'],
                                                                        'affected_fraction': [0.25, 0.25],
                                                                        'new_outflow': ['prison', 'treatment']}),
                                          reallocation_type='+',
                                          retroactive=True),
                        sub_population={'sub_group': 'test_population'},
                        spark_compartment='test_compartment',
                        apply_retroactive=True)
        ]

        compartment_transitions = CompartmentTransitionsStub(self.test_data)
        compartment_transitions.initialize_transition_table()
        compartment_transitions.initialize(compartment_policies)

        self.assertTrue((compartment_transitions.transition_dfs['before'].sum(axis=1) ==
                         compartment_transitions.transition_dfs['after_retroactive'].sum(axis=1)).all())
