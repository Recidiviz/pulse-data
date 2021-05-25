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
"""Test the PopulationSimulation object"""
import unittest
from copy import deepcopy
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_index_equal, assert_frame_equal


from recidiviz.calculator.modeling.population_projection.population_simulation.population_simulation_factory import (
    PopulationSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)


class TestPopulationSimulation(unittest.TestCase):
    """Test the PopulationSimulation class runs correctly"""

    def setUp(self) -> None:
        self.test_outflows_data = pd.DataFrame(
            {
                "total_population": [4, 2, 2, 4, 3] * 2,
                "crime": ["NAR"] * 5 + ["BUR"] * 5,
                "outflow_to": [
                    "supervision",
                    "prison",
                    "supervision",
                    "prison",
                    "prison",
                ]
                * 2,
                "compartment": [
                    "prison",
                    "supervision",
                    "prison",
                    "pretrial",
                    "pretrial",
                ]
                * 2,
                "time_step": [0] * 10,
            }
        )

        self.test_transitions_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2] * 2,
                "total_population": [4, 2, 2] * 2,
                "crime": ["NAR"] * 3 + ["BUR"] * 3,
                "outflow_to": ["supervision", "prison", "supervision"] * 2,
                "compartment": ["prison", "supervision", "prison"] * 2,
                "time_step": [0] * 6,
            }
        )

        self.test_total_population_data = pd.DataFrame(
            {
                "total_population": [10] * 10,
                "compartment": ["prison"] * 10,
                "crime": ["NAR"] * 5 + ["BUR"] * 5,
                "time_step": list(range(-4, 1)) * 2,
            }
        )

        self.user_inputs = {
            "projection_time_steps": 10,
            "start_time_step": 0,
            "constant_admissions": True,
            "speed_run": False,
        }
        self.simulation_architecture = {
            "pretrial": "shell",
            "prison": "full",
            "supervision": "full",
        }

        self.macro_population_simulation = (
            PopulationSimulationFactory.build_population_simulation(
                self.test_outflows_data,
                self.test_transitions_data,
                self.test_total_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )
        )
        self.macro_projection = self.macro_population_simulation.simulate_policies()

    def test_disaggregation_axes_must_be_in_data_dfs(self) -> None:
        test_outflows_data = self.test_outflows_data.drop("crime", axis=1)

        test_transitions_data = self.test_transitions_data.drop("crime", axis=1)

        with self.assertRaises(ValueError):
            _ = PopulationSimulationFactory.build_population_simulation(
                test_outflows_data,
                self.test_transitions_data,
                self.test_total_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )

        with self.assertRaises(ValueError):
            _ = PopulationSimulationFactory.build_population_simulation(
                test_outflows_data,
                test_transitions_data,
                self.test_total_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )

        with self.assertRaises(ValueError):
            _ = PopulationSimulationFactory.build_population_simulation(
                self.test_outflows_data,
                test_transitions_data,
                self.test_total_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )

    def test_simulation_forces_complete_user_inputs_dict(self) -> None:

        for i in self.user_inputs:
            test_user_inputs = deepcopy(self.user_inputs)
            test_user_inputs.pop(i)
            with self.assertRaises(ValueError):
                _ = PopulationSimulationFactory.build_population_simulation(
                    self.test_outflows_data,
                    self.test_transitions_data,
                    self.test_total_population_data,
                    self.simulation_architecture,
                    ["crime"],
                    test_user_inputs,
                    [],
                    -5,
                    pd.DataFrame(),
                    False,
                    True,
                )

    def test_microsimulation_can_initialize_with_policy_list(self) -> None:
        """Run a policy scenario with a microsimulation to make sure it doesn't break along the way."""
        policy_list = [
            SparkPolicy(
                TransitionTable.test_non_retroactive_policy,
                "supervision",
                {"crime": "NAR"},
                self.user_inputs["start_time_step"] + 2,
            )
        ]
        policy_sim = PopulationSimulationFactory.build_population_simulation(
            self.test_outflows_data,
            self.test_transitions_data,
            self.test_total_population_data,
            self.simulation_architecture,
            ["crime"],
            self.user_inputs,
            policy_list,
            -5,
            self.test_transitions_data,
            True,
            False,
        )

        policy_sim.simulate_policies()

    def test_baseline_with_backcast_projection_on(self) -> None:
        """Assert that the simulation results has negative time steps when the back-cast is enabled"""
        assert_index_equal(
            self.macro_projection.index.unique().sort_values(),
            pd.Int64Index(range(-5, 10)),
        )

    def test_baseline_with_backcast_projection_off(self) -> None:
        """Assert that microsim simulation results only have positive time steps"""
        population_simulation = PopulationSimulationFactory.build_population_simulation(
            self.test_outflows_data,
            self.test_transitions_data,
            self.test_total_population_data,
            self.simulation_architecture,
            ["crime"],
            self.user_inputs,
            [],
            0,
            self.test_transitions_data,
            True,
            False,
        )
        projection = population_simulation.simulate_policies()

        assert_index_equal(
            projection.index.unique().sort_values(), pd.Int64Index(range(11))
        )

    def test_dropping_data_raises_warning(self) -> None:
        """Assert that PopulationSimulation throws an error when some input data goes unused"""
        non_disaggregated_outflows_data = self.test_outflows_data.copy()
        non_disaggregated_transitions_data = self.test_transitions_data.copy()

        non_disaggregated_outflows_data.loc[
            non_disaggregated_outflows_data.compartment != "pretrial", "crime"
        ] = None
        non_disaggregated_transitions_data.loc[
            non_disaggregated_transitions_data.index == 0, "crime"
        ] = None

        with patch("logging.Logger.warning") as mock:
            _ = PopulationSimulationFactory.build_population_simulation(
                self.test_outflows_data,
                non_disaggregated_transitions_data,
                self.test_total_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )
            mock.assert_called_once()
            self.assertEqual(
                mock.mock_calls[0].args[0],
                "Some transitions data left unused: %s",
            )

        with patch("logging.Logger.warning") as mock:
            _ = PopulationSimulationFactory.build_population_simulation(
                non_disaggregated_outflows_data,
                self.test_transitions_data,
                self.test_total_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )
            mock.assert_called_once()
            self.assertEqual(
                mock.mock_calls[0].args[0],
                "Some outflows data left unused: %s",
            )

    def test_doubled_populations_doubles_simulation_populations(self) -> None:
        """Validates population scaling is actually scaling populations"""

        doubled_population_data = self.test_total_population_data.copy()
        doubled_population_data.total_population *= 2

        doubled_population_simulation = (
            PopulationSimulationFactory.build_population_simulation(
                self.test_outflows_data,
                self.test_transitions_data,
                doubled_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )
        )

        doubled_projection = doubled_population_simulation.simulate_policies()

        for _, row in doubled_population_data.iterrows():
            baseline_pop = self.macro_projection.loc[
                (self.macro_projection.compartment == row.compartment)
                & (self.macro_projection.time_step == row.time_step),
                "total_population",
            ].iloc[0]
            doubled_pop = doubled_projection.loc[
                (doubled_projection.compartment == row.compartment)
                & (doubled_projection.time_step == row.time_step),
                "total_population",
            ].iloc[0]
            self.assertEqual(round(baseline_pop * 2), round(doubled_pop))

    def test_coarse_population_data(self) -> None:
        """
        Test that PopulationSimulation can handle total_population_data with one less disaggregation axis
            than other data dfs
        """
        coarse_population_data = (
            self.test_total_population_data.groupby(["compartment", "time_step"])
            .sum()
            .reset_index()
        )

        coarse_macro_population_simulation = (
            PopulationSimulationFactory.build_population_simulation(
                self.test_outflows_data,
                self.test_transitions_data,
                coarse_population_data,
                self.simulation_architecture,
                ["crime"],
                self.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
            )
        )
        coarse_population_projection = (
            coarse_macro_population_simulation.simulate_policies()
        )

        assert_frame_equal(coarse_population_projection, self.macro_projection)
