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

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal, assert_series_equal

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)
from recidiviz.calculator.modeling.population_projection.utils.transitions_utils import (
    MIN_POSSIBLE_POLICY_TS,
    SIG_FIGS,
)


class TestTransitionTable(unittest.TestCase):
    """A base class for other transition test classes"""

    def setUp(self) -> None:
        self.test_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test_compartment"] * 5,
            }
        )
        self.prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TS, [])
        self.prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TS], self.test_data
        )


class TestInitialization(TestTransitionTable):
    """Test the TransitionTable initialization methods"""

    def test_normalize_transitions_requires_non_normalized_before_table(self) -> None:
        """Tests that transitory transitions table rejects a pre-normalized 'previous' table"""
        # uses its own prev_table because we don't want to normalize the general-use one
        prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TS, [])
        prev_table.generate_transition_tables([MIN_POSSIBLE_POLICY_TS], self.test_data)
        prev_table.normalize_transitions()

        with self.assertRaises(ValueError):
            TransitionTable(
                0,
                [],
                {MIN_POSSIBLE_POLICY_TS: prev_table.get_after_table()},
            )

    def test_results_independent_of_data_order(self) -> None:

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
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )
        transition_table_shuffled = TransitionTable(
            5,
            compartment_policies,
            {
                MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table().sample(
                    frac=1, axis=1
                )
            },
        )

        self.assertEqual(transition_table_default, transition_table_shuffled)


class TestPolicyFunctions(TestTransitionTable):
    """Test the policy functions used for Spark modeling"""

    def test_non_retroactive_policy_cannot_affect_retroactive_table(self) -> None:
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
                {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
            )

    def test_unnormalized_table_inverse_of_normalize_table(self) -> None:
        transition_table = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )
        original_before_table = transition_table.tables[MIN_POSSIBLE_POLICY_TS].copy()
        # 'normalize' table (in the classical, mathematical sense) to match scale of unnormalized table
        original_before_table /= original_before_table.sum().sum()

        transition_table.normalize_table(MIN_POSSIBLE_POLICY_TS)
        transition_table.unnormalize_table(MIN_POSSIBLE_POLICY_TS)
        assert_frame_equal(
            pd.DataFrame(original_before_table),
            pd.DataFrame(transition_table.tables[MIN_POSSIBLE_POLICY_TS]),
        )

    def test_alternate_transitions_data_equal_to_differently_instantiated_transition_table(
        self,
    ) -> None:
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
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        alternate_prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TS, [])
        alternate_prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TS], alternate_data
        )

        alternate_data_table = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TS: alternate_prev_table.get_after_table()},
        )

        assert_frame_equal(
            transition_table.tables[5],
            alternate_data_table.tables[5],
        )

    def test_preserve_normalized_outflow_behavior_preserves_normalized_outflow_behavior(
        self,
    ) -> None:
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
                    ts=MIN_POSSIBLE_POLICY_TS,
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
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        baseline_transitions = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        transition_table.normalize_transitions()
        baseline_transitions.normalize_transitions()

        assert_series_equal(
            baseline_transitions.tables[MIN_POSSIBLE_POLICY_TS]["prison"],
            transition_table.tables[MIN_POSSIBLE_POLICY_TS]["prison"],
        )

    def test_apply_reduction_with_trivial_reductions_doesnt_change_transition_table(
        self,
    ) -> None:

        policy_mul = partial(
            TransitionTable.apply_reductions,
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
            TransitionTable.apply_reductions,
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
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        assert_frame_equal(
            transition_table.previous_tables[MIN_POSSIBLE_POLICY_TS],
            transition_table.tables[5],
        )

    def test_apply_reductions_matches_example_by_hand(self) -> None:
        compartment_policy = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.apply_reductions,
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
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
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
            round(transition_table.tables[MIN_POSSIBLE_POLICY_TS], SIG_FIGS),
            round(expected_result, SIG_FIGS),
        )

    def test_constrained_apply_reductions_doesnt_affect_above_affected_LOS(
        self,
    ) -> None:
        compartment_policy = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.apply_reductions,
                    reduction_df=pd.DataFrame(
                        {
                            "outflow": ["prison"],
                            "affected_fraction": [0.25],
                            "reduction_size": [0.5],
                        }
                    ),
                    reduction_type="+",
                    affected_LOS=[None, 5],
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
            compartment_policy,
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        assert_frame_equal(
            round(transition_table.tables[5].loc[6:], SIG_FIGS),
            round(self.prev_table.tables[MIN_POSSIBLE_POLICY_TS].loc[6:], SIG_FIGS),
        )

    def test_reallocate_outflow_preserves_total_population(self) -> None:
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
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        if not transition_table.previous_tables:
            raise ValueError("previous tables are not populated")

        assert_series_equal(
            transition_table.tables[MIN_POSSIBLE_POLICY_TS].sum(axis=1),
            transition_table.previous_tables[MIN_POSSIBLE_POLICY_TS].sum(axis=1),
        )

    def test_extend_table_extends_table(self) -> None:
        """make sure CompartmentTransitions.extend_table is actually adding empty rows"""
        transition_table = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )
        expected_df_columns = transition_table.tables[MIN_POSSIBLE_POLICY_TS].columns
        expected_df_index_name = transition_table.tables[
            MIN_POSSIBLE_POLICY_TS
        ].index.name
        transition_table.extend_tables(15)
        self.assertEqual(
            set(transition_table.tables[MIN_POSSIBLE_POLICY_TS].index),
            set(range(1, 16)),
        )
        # Test the DataFrame multi-index was not changed during the extend
        assert_index_equal(
            transition_table.tables[MIN_POSSIBLE_POLICY_TS].columns,
            expected_df_columns,
        )
        self.assertEqual(
            transition_table.tables[MIN_POSSIBLE_POLICY_TS].index.name,
            expected_df_index_name,
        )

    def test_chop_technicals_chops_correctly(self) -> None:
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
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        baseline_transitions = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TS: self.prev_table.get_after_table()},
        )

        transition_table.normalize_transitions()
        baseline_transitions.normalize_transitions()

        # check total population was preserved
        assert_series_equal(
            transition_table.tables[5].iloc[0],
            baseline_transitions.tables[5].iloc[0],
        )

        # check technicals chopped
        transition_table.unnormalize_table(5)
        self.assertTrue((transition_table.tables[5].loc[3:, "prison"] == 0).all())
        self.assertTrue(transition_table.tables[5].loc[1, "prison"] != 0)

    def test_abolish_mandatory_minimum_matches_example_by_hand(self) -> None:
        normal_data = pd.DataFrame(
            {
                "compartment_duration": range(4, 17),
                "total_population": [1, 4, 9, 16, 25, 36, 100, 36, 25, 16, 9, 4, 1],
            }
        )
        normal_data["outflow_to"] = "release"

        normal_prev_table = TransitionTable(-9999, [])
        normal_prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TS], normal_data
        )

        compartment_policies = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.abolish_mandatory_minimum,
                    historical_outflows=normal_data,
                    outflow="release",
                    affected_fraction=0.5,
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
            {MIN_POSSIBLE_POLICY_TS: normal_prev_table.get_after_table()},
        )

        expected_mean = 10 - 2 * 0.5 / 2
        calculated_mean = np.average(
            transition_table.tables[5].release.index,
            weights=transition_table.tables[5].release.values,
        )
        self.assertEqual(np.round(expected_mean, 1), np.round(calculated_mean, 1))
