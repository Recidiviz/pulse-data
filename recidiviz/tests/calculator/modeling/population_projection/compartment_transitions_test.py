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
from functools import partial
import logging

import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.calculator.modeling.population_projection.compartment_transitions import (
    CompartmentTransitions,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)


class TestCompartmentTransitions(unittest.TestCase):
    """Test the CompartmentTransitions class"""

    def setUp(self) -> None:
        self.test_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test_compartment"] * 5,
            }
        )

    def test_rejects_remaining_as_outflow(self) -> None:
        """Tests that compartment transitions won't accept 'remaining' as an outflow"""
        broken_test_data = self.test_data.copy()
        broken_test_data.loc[
            broken_test_data["outflow_to"] == "jail", "outflow_to"
        ] = "remaining"
        with self.assertRaises(ValueError):
            CompartmentTransitions(broken_test_data)

    def test_rejects_data_with_negative_populations_or_durations(self) -> None:
        negative_duration_data = pd.DataFrame(
            {
                "compartment_duration": [1, -1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test_compartment"] * 5,
            }
        )

        negative_population_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, -4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test_compartment"] * 5,
            }
        )

        with self.assertRaises(ValueError):
            CompartmentTransitions(negative_duration_data)

        with self.assertRaises(ValueError):
            CompartmentTransitions(negative_population_data)

    def test_identity_policies_dont_change_probabilities(self) -> None:
        """Make sure splitting transitions into identical TransitionTables doesn't change behavior"""

        identity_functions = [
            SparkPolicy(
                policy_fn=partial(
                    TransitionTable.use_alternate_transitions_data,
                    alternate_historical_transitions=self.test_data,
                    retroactive=False,
                ),
                sub_population={"compartment": "test_compartment"},
                spark_compartment="test_compartment",
                policy_ts=i,
                apply_retroactive=False,
            )
            for i in range(5)
        ]

        transitions = CompartmentTransitions(self.test_data)
        transitions.initialize_transition_tables([])

        split_transitions = CompartmentTransitions(self.test_data)
        split_transitions.initialize_transition_tables(identity_functions)

        for ts in range(-3, 8):
            assert_frame_equal(
                transitions.get_per_ts_transition_table(ts),
                split_transitions.get_per_ts_transition_table(ts),
            )

    def test_multiple_policies(self) -> None:
        """Ensure get_per_ts_transition_table returns the correct transistion
        table when there are multiple SparkPolicies that are applied at
        different time steps.

        See https://github.com/Recidiviz/pulse-data/issues/9284.
        """

        # Generates historical_outflows such that initial_ratio goes to the moon
        # at releast_ts and the rest (1 - initial_ratio) goes to the moon at
        # releast_ts + 1
        def test_data_n(release_ts: int, initial_ratio: float) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "compartment_duration": [release_ts, release_ts + 1],
                    "total_population": [initial_ratio, (1 - initial_ratio)],
                    "outflow_to": ["moon"] * 2,
                    "compartment": ["test_compartment"] * 2,
                }
            )

        def policy_from_data(test_data: pd.DataFrame, start_ts: int) -> SparkPolicy:
            return SparkPolicy(
                policy_fn=partial(
                    TransitionTable.use_alternate_transitions_data,
                    alternate_historical_transitions=test_data,
                    retroactive=False,
                ),
                sub_population={"compartment": "test_compartment"},
                spark_compartment="test_compartment",
                policy_ts=start_ts,
                apply_retroactive=False,
            )

        # Half of everyone goes to the moon after 7 years and the remaining
        # go the next year.
        test_data_7 = test_data_n(7, 0.5)
        # A quarter of everyone goes to the moon after 4 years and the remaining
        # go the next year.
        test_data_4 = test_data_n(4, 0.25)
        # An eighth of everyone goes to the moon after 1 year and the remaining
        # go the next year.
        test_data_1 = test_data_n(1, 0.125)

        # Testing the default: no policies.
        transitions = CompartmentTransitions(test_data_7)
        transitions.initialize_transition_tables([])
        tt = transitions.get_per_ts_transition_table(8)
        logging.debug("Default (no policies) transition table:\n%s\n", tt)
        self.assertListEqual(list(tt["moon"]), [0, 0, 0, 0, 0, 0, 0.5, 1])

        # Testing one policy.
        transitions = CompartmentTransitions(test_data_7)
        transitions.initialize_transition_tables(
            [
                policy_from_data(test_data_4, start_ts=3),
            ]
        )
        tt = transitions.get_per_ts_transition_table(8)
        logging.debug("First policy only transition table:\n%s\n", tt)
        self.assertListEqual(list(tt["moon"]), [0, 0, 0, 0.25, 1, 0, 0.5, 1])

        # Testing one policy.
        transitions = CompartmentTransitions(test_data_7)
        transitions.initialize_transition_tables(
            [
                policy_from_data(test_data_1, start_ts=6),
            ]
        )
        tt = transitions.get_per_ts_transition_table(8)
        logging.debug("Second policy only transition table:\n%s\n", tt)
        self.assertListEqual(list(tt["moon"]), [0.125, 1, 0, 0, 0, 0, 0.5, 1])

        # Test two combined policies.
        # This is the "real" test!
        transitions = CompartmentTransitions(test_data_7)
        transitions.initialize_transition_tables(
            [
                policy_from_data(test_data_4, start_ts=3),
                policy_from_data(test_data_1, start_ts=6),
            ]
        )
        tt = transitions.get_per_ts_transition_table(8)
        logging.debug("Both policies transition table:\n%s\n", tt)
        self.assertListEqual(list(tt["moon"]), [0.125, 1, 0, 0.25, 1, 0, 0.5, 1])

        tt = transitions.get_per_ts_transition_table(10)
        logging.debug(
            "Transition table just before old policies have aged out:\n%s\n", tt
        )
        self.assertListEqual(list(tt["moon"]), [0.125, 1, 0, 0, 1, 0, 0, 1])

        tt = transitions.get_per_ts_transition_table(11)
        logging.debug("Transition table after old policies have aged out:\n%s\n", tt)
        self.assertListEqual(list(tt["moon"]), [0.125, 1, 0, 0, 0, 0, 0, 0])

    def test_retroactive_policy(self) -> None:

        # After 5 years, half of people go to the moon and half go to mars.
        default_test_data = pd.DataFrame(
            {
                "compartment_duration": [5, 5],
                "total_population": [1, 1],
                "outflow_to": ["moon", "mars"],
                "compartment": ["test_compartment"] * 2,
            }
        )

        transitions = CompartmentTransitions(default_test_data)
        transitions.initialize_transition_tables([])
        tt = transitions.get_per_ts_transition_table(3)
        self.assertListEqual(list(tt["moon"]), [0, 0, 0, 0, 0.5])
        self.assertListEqual(list(tt["mars"]), [0, 0, 0, 0, 0.5])
        tt = transitions.get_per_ts_transition_table(4)
        self.assertListEqual(list(tt["moon"]), [0, 0, 0, 0, 0.5])
        self.assertListEqual(list(tt["mars"]), [0, 0, 0, 0, 0.5])

        # Our new policy is that after 2 years, half of people go to the moon
        # After 5 years, half of people still go to mars.
        retroactive_test_data = pd.DataFrame(
            {
                "compartment_duration": [2, 5],
                "total_population": [1, 1],
                "outflow_to": ["moon", "mars"],
                "compartment": ["test_compartment"] * 2,
            }
        )

        retroactive_policy = SparkPolicy(
            policy_fn=partial(
                TransitionTable.use_alternate_transitions_data,
                alternate_historical_transitions=retroactive_test_data,
                retroactive=True,
            ),
            sub_population={"compartment": "test_compartment"},
            spark_compartment="test_compartment",
            policy_ts=3,
            apply_retroactive=True,
        )

        transitions = CompartmentTransitions(default_test_data)
        transitions.initialize_transition_tables([retroactive_policy])
        # Our retroactive policy is applied at timestep 3, so
        # get_per_ts_transition_table should return a transitory table that makes
        # half of all people who have been here >=2 years to go to the moon.
        # The half of people that go to mars still wait 5 years.
        before = transitions.get_per_ts_transition_table(2)
        self.assertEqual(list(before["mars"]), [0, 0, 0, 0, 0.5])
        self.assertEqual(list(before["moon"]), [0, 0, 0, 0, 0.5])

        # Verify the transitory table is sending 50% of those who have been here at
        # least 2 time steps to the moon, and only those at least 5 time steps to mars
        tt = transitions.get_per_ts_transition_table(3)
        self.assertListEqual(list(tt["moon"]), [0, 0.5, 0.5, 0.5, 0.5])
        self.assertListEqual(list(tt["mars"]), [0, 0, 0, 0, 0.5])
        # Time step 4 should use the "after" table
        tt = transitions.get_per_ts_transition_table(4)
        self.assertListEqual(list(tt["moon"]), [0, 0.5, 0, 0, 0])
        self.assertListEqual(list(tt["mars"]), [0, 0, 0, 0, 1])
