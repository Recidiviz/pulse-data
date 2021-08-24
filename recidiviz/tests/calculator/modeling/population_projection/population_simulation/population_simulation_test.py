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
from typing import Any, Dict, Optional
from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal, assert_index_equal

from recidiviz.calculator.modeling.population_projection.population_simulation.population_simulation_factory import (
    PopulationSimulation,
    PopulationSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)


class TestPopulationSimulation(unittest.TestCase):
    """Test the PopulationSimulation class runs correctly"""

    test_outflows_data = pd.DataFrame()
    test_transitions_data = pd.DataFrame()
    test_total_population_data = pd.DataFrame()
    user_inputs: Dict[str, Any] = {}
    simulation_architecture: Dict[str, str] = {}
    macro_population_simulation = Optional[PopulationSimulation]
    macro_projection = pd.DataFrame()

    @classmethod
    def setUp(cls) -> None:
        cls.test_outflows_data = pd.DataFrame(
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

        cls.test_transitions_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2] * 2,
                "total_population": [4, 2, 2] * 2,
                "crime": ["NAR"] * 3 + ["BUR"] * 3,
                "outflow_to": ["supervision", "prison", "supervision"] * 2,
                "compartment": ["prison", "supervision", "prison"] * 2,
                "time_step": [0] * 6,
            }
        )

        cls.test_total_population_data = pd.DataFrame(
            {
                "total_population": [10] * 10,
                "compartment": ["prison"] * 10,
                "crime": ["NAR"] * 5 + ["BUR"] * 5,
                "time_step": list(range(-4, 1)) * 2,
            }
        )

        cls.user_inputs = {
            "projection_time_steps": 10,
            "start_time_step": 0,
            "constant_admissions": True,
            "speed_run": False,
        }
        cls.simulation_architecture = {
            "pretrial": "shell",
            "prison": "full",
            "supervision": "full",
        }

        cls.macro_population_simulation = (
            PopulationSimulationFactory.build_population_simulation(
                cls.test_outflows_data,
                cls.test_transitions_data,
                cls.test_total_population_data,
                cls.simulation_architecture,
                ["crime"],
                cls.user_inputs,
                [],
                -5,
                pd.DataFrame(),
                False,
                True,
                None,
            )
        )
        cls.macro_projection = cls.macro_population_simulation.simulate_policies()

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
                None,
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
                None,
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
                None,
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
                    None,
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
            None,
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
            None,
        )
        projection = population_simulation.simulate_policies()

        assert_index_equal(
            projection.index.unique().sort_values(), pd.Int64Index(range(10))
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
                None,
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
                None,
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
                None,
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
                None,
            )
        )
        coarse_population_projection = (
            coarse_macro_population_simulation.simulate_policies()
        )

        assert_frame_equal(coarse_population_projection, self.macro_projection)

    def test_update_attributes_age_recidiviz_schema_matches_example_by_hand(
        self,
    ) -> None:
        """Validate age cross function works as expected."""
        age_outflows_data = pd.DataFrame(
            {
                "total_population": [0] * 5,
                "age": ["0-24", "25-29", "30-34", "35-39", "40+"],
                "compartment": ["pretrial"] * 5,
                "outflow_to": ["prison"] * 5,
                "time_step": [0] * 5,
            }
        )

        age_transitions_data = pd.DataFrame(
            {
                "compartment_duration": [1000, 1000, 1000] * 5,
                "total_population": [4, 2, 2] * 5,
                "age": ["0-24"] * 3
                + ["25-29"] * 3
                + ["30-34"] * 3
                + ["35-39"] * 3
                + ["40+"] * 3,
                "outflow_to": ["supervision", "prison", "supervision"] * 5,
                "compartment": ["prison", "supervision", "prison"] * 5,
                "time_step": [0] * 15,
            }
        )

        age_total_population_data = pd.DataFrame(
            {
                "total_population": [10, 0, 0, 5, 0],
                "compartment": ["prison"] * 5,
                "age": ["0-24", "25-29", "30-34", "35-39", "40+"],
                "time_step": [0] * 5,
            }
        )

        age_user_inputs = {
            "projection_time_steps": 121,
            "start_time_step": 0,
            "constant_admissions": True,
            "cross_flow_function": "update_attributes_age_recidiviz_schema",
        }

        population_simulation = PopulationSimulationFactory.build_population_simulation(
            age_outflows_data,
            age_transitions_data,
            age_total_population_data,
            self.simulation_architecture,
            ["age"],
            age_user_inputs,
            [],
            0,
            age_transitions_data,
            True,
            False,
            None,
        )
        population_projection = population_simulation.simulate_policies()
        prison_populations = (
            population_projection[population_projection.compartment == "prison"]
            .groupby(["time_step", "simulation_group"])
            .sum()
            .unstack("simulation_group")
        )

        prison_populations.columns = prison_populations.columns.get_level_values(
            "simulation_group"
        )

        expected = pd.DataFrame(index=range(121))
        expected.columns.name = "simulation_group"
        expected.index.name = "time_step"
        expected["0-24"] = [10.0] * 60 + [0.0] * 61
        expected["25-29"] = [0.0] * 60 + [10.0] * 60 + [0.0]
        expected["30-34"] = [0.0] * 120 + [10.0]
        expected["35-39"] = [5.0] * 60 + [0.0] * 61
        expected["40+"] = [0.0] * 60 + [5.0] * 61

        assert_frame_equal(expected, prison_populations)
