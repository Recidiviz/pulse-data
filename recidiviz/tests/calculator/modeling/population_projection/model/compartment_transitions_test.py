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
import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.calculator.modeling.population_projection.simulations.compartment_transitions import (
    CompartmentTransitions,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.simulations.transition_table import (
    TransitionTable,
)


class TestCompartmentTransitions(unittest.TestCase):
    """Test the CompartmentTransitions class"""

    def setUp(self):
        self.test_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test_compartment"] * 5,
            }
        )

    def test_rejects_remaining_as_outflow(self):
        """Tests that compartment transitions won't accept 'remaining' as an outflow"""
        broken_test_data = self.test_data.copy()
        broken_test_data.loc[
            broken_test_data["outflow_to"] == "jail", "outflow_to"
        ] = "remaining"
        with self.assertRaises(ValueError):
            CompartmentTransitions(broken_test_data)

    def test_rejects_data_with_negative_populations_or_durations(self):
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

    def test_identity_policies_dont_change_probabilities(self):
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
