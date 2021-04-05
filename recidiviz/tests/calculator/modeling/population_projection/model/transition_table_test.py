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
"""Test the TransitionTable object"""

import unittest
from functools import partial
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal, assert_index_equal

from recidiviz.calculator.modeling.population_projection.simulations.transition_table import (
    TransitionTable,
)

from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    SIG_FIGS,
    TransitionTableType,
)

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class TestTransitionTable(unittest.TestCase):
    """A base class for other transition test classes"""

    def setUp(self):
        self.test_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test_compartment"] * 5,
            }
        )
        self.prev_table = TransitionTable(-9999, [])
        self.prev_table.generate_transition_table(
            TransitionTableType.AFTER, self.test_data
        )


class TestInitialization(TestTransitionTable):
    """Test the TransitionTable initialization methods"""

    def test_normalize_transitions_requires_non_normalized_before_table(self):
        """Tests that transitory transitions table rejects a pre-normalized 'previous' table"""
        # uses its own prev_table because we don't want to normalize the general-use one
        prev_table = TransitionTable(-9999, [])
        prev_table.generate_transition_table(TransitionTableType.AFTER, self.test_data)
        prev_table.normalize_transitions()

        with self.assertRaises(ValueError):
            TransitionTable(
                0,
                [],
                prev_table.get_table(TransitionTableType.AFTER),
            )

    def test_results_independent_of_data_order(self):

        compartment_policies = [
            SparkPolicy(
                policy_fn=TransitionTable.test_retroactive_policy,
                sub_population={"compartment": "test_compartment"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=True,
            ),
            SparkPolicy(
                policy_fn=TransitionTable.test_non_retroactive_policy,
                sub_population={"compartment": "test_compartment"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=False,
            ),
        ]
        transition_table_default = TransitionTable(
            5,
            compartment_policies,
            self.prev_table.get_table(TransitionTableType.AFTER),
        )
        transition_table_shuffled = TransitionTable(
            5,
            compartment_policies,
            self.prev_table.get_table(TransitionTableType.AFTER).sample(frac=1, axis=1),
        )

        self.assertEqual(transition_table_default, transition_table_shuffled)


class TestPolicyFunctions(TestTransitionTable):
    """Test the policy functions used for Spark modeling"""

    def test_non_retroactive_policy_cannot_affect_retroactive_table(self):
        compartment_policies = [
            SparkPolicy(
                policy_fn=TransitionTable.test_retroactive_policy,
                sub_population={"compartment": "test_compartment"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=False,
            )
        ]

        with self.assertRaises(ValueError):
            TransitionTable(
                5,
                compartment_policies,
                self.prev_table.get_table(TransitionTableType.AFTER),
            )

    def test_unnormalized_table_inverse_of_normalize_table(self):
        transition_table = TransitionTable(
            5,
            [],
            self.prev_table.get_table(TransitionTableType.AFTER),
        )
        original_before_table = transition_table.transition_dfs[
            TransitionTableType.BEFORE
        ].copy()
        # 'normalize' table (in the classical, mathematical sense) to match scale of unnormalized table
        original_before_table /= original_before_table.sum().sum()

        transition_table._normalize_table(  # pylint: disable=protected-access
            TransitionTableType.BEFORE
        )
        transition_table.unnormalize_table(TransitionTableType.BEFORE)
        assert_frame_equal(
            pd.DataFrame(original_before_table),
            pd.DataFrame(transition_table.transition_dfs[TransitionTableType.BEFORE]),
        )

    def test_alternate_transitions_data_equal_to_differently_instantiated_transition_table(
        self,
    ):
        alternate_data = self.test_data.copy()
        alternate_data.compartment_duration *= 2
        alternate_data.total_population = 10 - alternate_data.total_population

        policy_function = SparkPolicy(
            policy_fn=partial(
                TransitionTable.use_alternate_transitions_data,
                alternate_historical_transitions=alternate_data,
                retroactive=False,
            ),
            spark_compartment="test_compartment",
            sub_population={"sub_group": "test_population"},
            policy_ts=5,
            apply_retroactive=False,
        )

        transition_table = TransitionTable(
            5,
            [policy_function],
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        alternate_prev_table = TransitionTable(-9999, [])
        alternate_prev_table.generate_transition_table(
            TransitionTableType.AFTER, alternate_data
        )

        alternate_data_table = TransitionTable(
            5,
            [],
            alternate_prev_table.get_table(TransitionTableType.AFTER),
        )

        assert_frame_equal(
            transition_table.transition_dfs[TransitionTableType.AFTER],
            alternate_data_table.transition_dfs[TransitionTableType.AFTER],
        )

    def test_preserve_normalized_outflow_behavior_preserves_normalized_outflow_behavior(
        self,
    ):
        compartment_policies = [
            SparkPolicy(
                policy_fn=TransitionTable.test_retroactive_policy,
                sub_population={"compartment": "test_compartment"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=True,
            ),
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.preserve_normalized_outflow_behavior,
                    outflows=["prison"],
                    state=TransitionTableType.BEFORE,
                ),
                sub_population={"compartment": "test_compartment"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=True,
            ),
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        baseline_transitions = TransitionTable(
            5,
            [],
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        transition_table.normalize_transitions()
        baseline_transitions.normalize_transitions()

        assert_series_equal(
            baseline_transitions.transition_dfs[TransitionTableType.BEFORE]["prison"],
            transition_table.transition_dfs[TransitionTableType.BEFORE]["prison"],
        )

    def test_apply_reduction_with_trivial_reductions_doesnt_change_transition_table(
        self,
    ):

        policy_mul = partial(
            TransitionTable.apply_reduction,
            reduction_df=pd.DataFrame(
                {
                    "outflow": ["prison"] * 2,
                    "affected_fraction": [0, 0.5],
                    "reduction_size": [0.5, 0],
                }
            ),
            reduction_type="*",
            retroactive=False,
        )

        policy_add = partial(
            TransitionTable.apply_reduction,
            reduction_df=pd.DataFrame(
                {
                    "outflow": ["prison"] * 2,
                    "affected_fraction": [0, 0.5],
                    "reduction_size": [0.5, 0],
                }
            ),
            reduction_type="+",
            retroactive=False,
        )

        compartment_policies = [
            SparkPolicy(
                policy_mul,
                "test_compartment",
                {"sub_group": "test_population"},
                5,
                False,
            ),
            SparkPolicy(
                policy_add,
                "test_compartment",
                {"sub_group": "test_population"},
                5,
                False,
            ),
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        assert_frame_equal(
            transition_table.previous_table,
            transition_table.transition_dfs[TransitionTableType.AFTER],
        )

    def test_apply_reduction_matches_example_by_hand(self):
        compartment_policy = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.apply_reduction,
                    reduction_df=pd.DataFrame(
                        {
                            "outflow": ["prison"],
                            "affected_fraction": [0.25],
                            "reduction_size": [0.5],
                        }
                    ),
                    reduction_type="+",
                    retroactive=True,
                ),
                sub_population={"sub_group": "test_population"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=True,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policy,
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        expected_result = pd.DataFrame(
            {
                "jail": [4, 2, 0, 0, 0, 0, 0, 0, 0, 0],
                "prison": [2, 0.5, 3.5, 0, 0, 0, 0, 0, 0.375, 2.625],
            },
            index=range(1, 11),
            dtype=float,
        )
        expected_result.index.name = "compartment_duration"
        expected_result.columns.name = "outflow_to"

        assert_frame_equal(
            round(
                transition_table.transition_dfs[TransitionTableType.BEFORE], SIG_FIGS
            ),
            round(expected_result, SIG_FIGS),
        )

    def test_reallocate_outflow_preserves_total_population(self):
        compartment_policies = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.reallocate_outflow,
                    reallocation_df=pd.DataFrame(
                        {
                            "outflow": ["jail", "jail"],
                            "affected_fraction": [0.25, 0.25],
                            "new_outflow": ["prison", "treatment"],
                        }
                    ),
                    reallocation_type="+",
                    retroactive=True,
                ),
                sub_population={"sub_group": "test_population"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=True,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        assert_series_equal(
            transition_table.transition_dfs[TransitionTableType.BEFORE].sum(axis=1),
            transition_table.previous_table.sum(axis=1),
        )

    def test_extend_table_extends_table(self):
        """make sure CompartmentTransitions.extend_table is actually adding empty rows"""
        state = TransitionTableType.BEFORE
        transition_table = TransitionTable(
            5,
            [],
            self.prev_table.get_table(TransitionTableType.AFTER),
        )
        expected_df_columns = transition_table.transition_dfs[state].columns
        expected_df_index_name = transition_table.transition_dfs[state].index.name
        transition_table.extend_tables(15)
        self.assertEqual(
            set(transition_table.transition_dfs[state].index),
            set(range(1, 16)),
        )
        # Test the DataFrame multi-index was not changed during the extend
        assert_index_equal(
            transition_table.transition_dfs[state].columns,
            expected_df_columns,
        )
        self.assertEqual(
            transition_table.transition_dfs[state].index.name,
            expected_df_index_name,
        )

    def test_chop_technicals_chops_correctly(self):
        """
        Make sure CompartmentTransitions.chop_technical_revocations zeros technicals after the correct duration and
            that table sums to the same amount (i.e. total population shifted but not removed)
        """
        compartment_policies = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.chop_technical_revocations,
                    technical_outflow="prison",
                    release_outflow="jail",
                    retroactive=False,
                ),
                sub_population={"sub_group": "test_population"},
                spark_compartment="test_compartment",
                policy_ts=5,
                apply_retroactive=False,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        baseline_transitions = TransitionTable(
            5,
            [],
            self.prev_table.get_table(TransitionTableType.AFTER),
        )

        transition_table.normalize_transitions()
        baseline_transitions.normalize_transitions()

        # check total population was preserved
        assert_series_equal(
            transition_table.transition_dfs[TransitionTableType.AFTER].iloc[0],
            baseline_transitions.transition_dfs[TransitionTableType.AFTER].iloc[0],
        )

        # check technicals chopped
        transition_table.unnormalize_table(TransitionTableType.AFTER)
        self.assertTrue(
            (
                transition_table.transition_dfs[TransitionTableType.AFTER].loc[
                    3:, "prison"
                ]
                == 0
            ).all()
        )
        self.assertTrue(
            transition_table.transition_dfs[TransitionTableType.AFTER].loc[1, "prison"]
            != 0
        )
