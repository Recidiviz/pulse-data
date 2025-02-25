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
    MIN_POSSIBLE_POLICY_TIME_STEP,
    SIG_FIGS,
)


class TestTransitionTable(unittest.TestCase):
    """A base class for other transition test classes"""

    def setUp(self) -> None:
        self.test_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "cohort_portion": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test_compartment"] * 5,
            }
        )
        self.prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TIME_STEP, [])
        self.prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TIME_STEP], self.test_data
        )


class TestInitialization(TestTransitionTable):
    """Test the TransitionTable initialization methods"""

    def test_normalize_transitions_requires_non_normalized_before_table(self) -> None:
        """Tests that transitory transitions table rejects a pre-normalized 'previous' table"""
        # uses its own prev_table because we don't want to normalize the general-use one
        prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TIME_STEP, [])
        prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TIME_STEP], self.test_data
        )
        prev_table.normalize_transitions()

        with self.assertRaises(ValueError):
            TransitionTable(
                0,
                [],
                {MIN_POSSIBLE_POLICY_TIME_STEP: prev_table.get_after_table()},
            )

    def test_results_independent_of_data_order(self) -> None:
        compartment_policies = [
            SparkPolicy(
                policy_fn=TransitionTable.test_retroactive_policy,
                simulation_group="test_compartment",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=True,
            ),
            SparkPolicy(
                policy_fn=TransitionTable.test_non_retroactive_policy,
                simulation_group="test_compartment",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=False,
            ),
        ]
        transition_table_default = TransitionTable(
            5,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )
        transition_table_shuffled = TransitionTable(
            5,
            compartment_policies,
            {
                MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table().sample(
                    frac=1, axis=1
                )
            },
        )

        self.assertEqual(transition_table_default, transition_table_shuffled)

    def test_unnormalize_table(self) -> None:
        test_data = pd.DataFrame(
            {
                "compartment_duration": [1, 3, 1, 2],
                "cohort_portion": [1, 4, 2, 3],
                "outflow_to": [
                    "mars",
                    "mars",
                    "moon",
                    "moon",
                ],
            }
        )
        transition_table = TransitionTable(MIN_POSSIBLE_POLICY_TIME_STEP, [])
        transition_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TIME_STEP], test_data
        )
        transition_table.normalize_transitions()
        unnormalized_table = TransitionTable.unnormalized_table(
            transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP]
        )
        expected_cdf = pd.DataFrame(
            {
                "mars": [0.1, 0, 0.4],
                "moon": [0.2, 0.3, 0],
            },
            columns=pd.Index(["mars", "moon"], name="outflow_to"),
            index=pd.Index(range(1, 4), name="compartment_duration"),
        )
        assert_frame_equal(unnormalized_table, expected_cdf)


class TestPolicyFunctions(TestTransitionTable):
    """Test the policy functions used for Spark modeling"""

    def test_non_retroactive_policy_cannot_affect_retroactive_table(self) -> None:
        compartment_policies = [
            SparkPolicy(
                policy_fn=TransitionTable.test_retroactive_policy,
                simulation_group="test_compartment",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=False,
            )
        ]

        with self.assertRaises(ValueError):
            TransitionTable(
                5,
                compartment_policies,
                {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
            )

    def test_unnormalized_table_inverse_of_normalize_table(self) -> None:
        transition_table = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )
        original_before_table = transition_table.tables[
            MIN_POSSIBLE_POLICY_TIME_STEP
        ].copy()
        # 'normalize' table (in the classical, mathematical sense) to match scale of unnormalized table
        original_before_table /= original_before_table.sum().sum()

        transition_table.normalize_table(MIN_POSSIBLE_POLICY_TIME_STEP)
        transition_table.unnormalize_table(MIN_POSSIBLE_POLICY_TIME_STEP)
        assert_frame_equal(
            pd.DataFrame(original_before_table),
            pd.DataFrame(transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP]),
        )

    def test_alternate_transitions_data_equal_to_differently_instantiated_transition_table(
        self,
    ) -> None:
        alternate_data = self.test_data.copy()
        alternate_data.compartment_duration *= 2
        alternate_data.cohort_portion = 10 - alternate_data.cohort_portion

        policy_function = SparkPolicy(
            spark_compartment="test_compartment",
            simulation_group="test_population",
            policy_time_step=5,
            apply_retroactive=False,
            policy_fn=TransitionTable.use_alternate_transitions_data,
            alternate_transitions_data=alternate_data,
        )

        transition_table = TransitionTable(
            5,
            [policy_function],
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )

        alternate_prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TIME_STEP, [])
        alternate_prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TIME_STEP], alternate_data
        )

        alternate_data_table = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TIME_STEP: alternate_prev_table.get_after_table()},
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
                simulation_group="test_compartment",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=True,
            ),
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.preserve_normalized_outflow_behavior,
                    outflows=["prison"],
                    time_step=MIN_POSSIBLE_POLICY_TIME_STEP,
                ),
                simulation_group="test_compartment",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=True,
            ),
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )

        baseline_transitions = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )

        transition_table.normalize_transitions()
        baseline_transitions.normalize_transitions()

        assert_series_equal(
            baseline_transitions.tables[MIN_POSSIBLE_POLICY_TIME_STEP]["prison"],
            transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP]["prison"],
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
                "test_compartment",
                "test_population",
                5,
                False,
                policy_mul,
            ),
            SparkPolicy(
                "test_compartment",
                "test_population",
                5,
                False,
                policy_add,
            ),
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )

        assert_frame_equal(
            transition_table.previous_tables[MIN_POSSIBLE_POLICY_TIME_STEP],
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
                simulation_group="test_population",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=True,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policy,
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
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
            round(transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP], SIG_FIGS),
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
                simulation_group="test_population",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=False,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policy,
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )

        assert_frame_equal(
            round(transition_table.tables[5].loc[6:], SIG_FIGS),
            round(
                self.prev_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP].loc[6:], SIG_FIGS
            ),
        )

    def test_reallocate_outflow_preserves_cohort_portion(self) -> None:
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
                simulation_group="test_population",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=True,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )

        if not transition_table.previous_tables:
            raise ValueError("previous tables are not populated")

        assert_series_equal(
            transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP].sum(axis=1),
            transition_table.previous_tables[MIN_POSSIBLE_POLICY_TIME_STEP].sum(axis=1),
        )

    def test_extend_table_extends_table(self) -> None:
        """make sure CompartmentTransitions.extend_table is actually adding empty rows"""
        transition_table = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )
        expected_df_columns = transition_table.tables[
            MIN_POSSIBLE_POLICY_TIME_STEP
        ].columns
        expected_df_index_name = transition_table.tables[
            MIN_POSSIBLE_POLICY_TIME_STEP
        ].index.name
        transition_table.extend_tables(15)
        self.assertEqual(
            set(transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP].index),
            set(range(1, 16)),
        )
        # Test the DataFrame multi-index was not changed during the extend
        assert_index_equal(
            transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP].columns,
            expected_df_columns,
        )
        self.assertEqual(
            transition_table.tables[MIN_POSSIBLE_POLICY_TIME_STEP].index.name,
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
                simulation_group="test_population",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=False,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
        )

        baseline_transitions = TransitionTable(
            5,
            [],
            {MIN_POSSIBLE_POLICY_TIME_STEP: self.prev_table.get_after_table()},
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
                "cohort_portion": [1, 4, 9, 16, 25, 36, 100, 36, 25, 16, 9, 4, 1],
            }
        )
        normal_data["outflow_to"] = "release"

        normal_prev_table = TransitionTable(-9999, [])
        normal_prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TIME_STEP], normal_data
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
                simulation_group="test_population",
                spark_compartment="test_compartment",
                policy_time_step=5,
                apply_retroactive=False,
            )
        ]

        transition_table = TransitionTable(
            5,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: normal_prev_table.get_after_table()},
        )

        expected_mean = 10 - 2 * 0.5 / 2
        calculated_mean = np.average(
            transition_table.tables[5].release.index,
            weights=transition_table.tables[5].release.values,
        )
        self.assertEqual(np.round(expected_mean, 1), np.round(calculated_mean, 1))

    def test_apply_reductions_one_outflow(self) -> None:
        one_outflow_df = pd.DataFrame(
            {
                "compartment_duration": range(1, 11),
                "cohort_portion": [1] * 10,
                "outflow_to": "outflow",
            }
        )

        normal_prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TIME_STEP, [])
        normal_prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TIME_STEP], one_outflow_df
        )
        policy_time_step = 5
        compartment_policies = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.apply_reductions,
                    reduction_df=pd.DataFrame(
                        {
                            "outflow": ["outflow"],
                            "affected_fraction": [0.5],
                            "reduction_size": [1],
                        }
                    ),
                    reduction_type="+",
                    retroactive=False,
                ),
                simulation_group="test_population",
                spark_compartment="test_compartment",
                policy_time_step=policy_time_step,
                apply_retroactive=False,
            )
        ]

        transition_table = TransitionTable(
            policy_time_step,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: normal_prev_table.get_after_table()},
        )

        expected_policy_table = pd.DataFrame(
            {"outflow": [1.5] + [1] * 8 + [0.5]},
            index=pd.RangeIndex(range(1, 11), name="compartment_duration"),
        )
        expected_policy_table.columns = pd.Index(
            expected_policy_table.columns, name="outflow_to"
        )
        assert_frame_equal(
            transition_table.tables[policy_time_step], expected_policy_table
        )

    def test_apply_reductions_two_outflows(self) -> None:
        """
        Ensure that the `apply_reductions` method only modifies the one policy-impacted outflow even if two arepresent
        """
        two_outflows_df = pd.concat(
            [
                pd.DataFrame(
                    {
                        "compartment_duration": range(1, 11),
                        "cohort_portion": [1] * 10,
                        "outflow_to": "outflow",
                    }
                ),
                pd.DataFrame(
                    {
                        "compartment_duration": range(1, 11),
                        "cohort_portion": [0.5] * 10,
                        "outflow_to": "other_outflow",
                    }
                ),
            ]
        )

        normal_prev_table = TransitionTable(MIN_POSSIBLE_POLICY_TIME_STEP, [])
        normal_prev_table.generate_transition_tables(
            [MIN_POSSIBLE_POLICY_TIME_STEP], two_outflows_df
        )
        policy_time_step = 5
        compartment_policies = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.apply_reductions,
                    reduction_df=pd.DataFrame(
                        {
                            "outflow": ["outflow"],
                            "affected_fraction": [0.5],
                            "reduction_size": [1],
                        }
                    ),
                    reduction_type="+",
                    retroactive=False,
                ),
                simulation_group="test_population",
                spark_compartment="test_compartment",
                policy_time_step=policy_time_step,
                apply_retroactive=False,
            )
        ]

        transition_table = TransitionTable(
            policy_time_step,
            compartment_policies,
            {MIN_POSSIBLE_POLICY_TIME_STEP: normal_prev_table.get_after_table()},
        )

        expected_policy_table = pd.DataFrame(
            {
                "other_outflow": [0.5] * 10,
                "outflow": [1.5] + [1] * 8 + [0.5],
            },
            index=pd.RangeIndex(range(1, 11), name="compartment_duration"),
        )
        expected_policy_table.columns = pd.Index(
            expected_policy_table.columns, name="outflow_to"
        )
        assert_frame_equal(
            transition_table.tables[policy_time_step], expected_policy_table
        )
