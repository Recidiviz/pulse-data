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
"""Test the SubSimulation class"""
# pylint: disable=super-init-not-called, unused-variable

import unittest
from typing import Dict, List, Optional, cast
from unittest.mock import patch

import pandas as pd

from recidiviz.calculator.modeling.population_projection.full_compartment import (
    FullCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation import (
    SubSimulation,
)
from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation_factory import (
    SubSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.initializer import (
    UserInputs,
)


class FakeCompartment(FullCompartment):
    """Skeleton compartment for integration tests"""

    def __init__(self) -> None:
        self.scale_factor: Optional[float] = None

    def scale_cohorts(self, scale_factor: float) -> None:
        self.scale_factor = scale_factor

    def get_cohort_df(self) -> pd.DataFrame:
        return pd.DataFrame([[1, 2, 3], [2, 3, 4]])

    def get_per_ts_population(self) -> pd.Series:
        return pd.Series([1, 2, 3])

    def get_current_population(self) -> float:
        return 1


class TestSubSimulation(unittest.TestCase):
    """Test the SubSimulation runs correctly"""

    test_outflow_data = pd.DataFrame()
    test_transitions_data = pd.DataFrame()
    starting_cohort_sizes = pd.DataFrame()
    test_architecture: Dict[str, str] = {}
    compartment_policies: List[SparkPolicy] = []
    test_user_inputs: UserInputs = cast(UserInputs, None)

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_outflow_data = pd.DataFrame(
            {
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": [
                    "supervision",
                    "prison",
                    "supervision",
                    "prison",
                    "prison",
                ],
                "compartment": [
                    "prison",
                    "supervision",
                    "prison",
                    "pretrial",
                    "pretrial",
                ],
                "time_step": [0, 0, 0, 0, 0],
            }
        )

        cls.test_transitions_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 5],
                "total_population": [4, 2, 2],
                "outflow_to": ["supervision", "prison", "supervision"],
                "compartment": ["prison", "supervision", "prison"],
            }
        )

        cls.starting_cohort_sizes = pd.DataFrame(
            {
                "total_population": [10],
                "compartment": ["prison"],
                "time_step": [0],
            }
        )

        cls.test_architecture = {
            "pretrial": "shell",
            "supervision": "full",
            "prison": "full",
        }

        cls.test_user_inputs = UserInputs(
            start_time_step=0,
            projection_time_steps=10,
            constant_admissions=False,
            speed_run=False,
        )

    def setUp(self) -> None:
        self.sub_simulation = SubSimulation(
            {"A": FakeCompartment(), "B": FakeCompartment()}
        )

    def test_dropping_data_raises_warning_or_error(self) -> None:
        """Assert that SubSimulation throws an error when some input data goes unused"""
        typo_transitions = self.test_transitions_data.copy()
        typo_transitions.loc[typo_transitions.index == 0, "compartment"] = "prison "

        typo_outflows = self.test_outflow_data.copy()
        typo_outflows.loc[typo_outflows.index == 4, "compartment"] = "pre-trial"

        with self.assertRaises(ValueError):
            _ = SubSimulationFactory.build_sub_simulation(
                typo_outflows,
                self.test_transitions_data,
                self.test_architecture,
                self.test_user_inputs,
                self.compartment_policies,
                0,
                True,
                self.starting_cohort_sizes,
            )

        with patch("logging.Logger.warning") as mock:
            _ = SubSimulationFactory.build_sub_simulation(
                self.test_outflow_data,
                typo_transitions,
                self.test_architecture,
                self.test_user_inputs,
                self.compartment_policies,
                0,
                True,
                self.starting_cohort_sizes,
            )
            mock.assert_called_once()
            self.assertEqual(
                mock.mock_calls[0].args[0],
                "Some transitions data not fed to a compartment:\n%s",
            )

    def test_scale_cohorts_passes_scale_factors_down(self) -> None:
        """Integration test that SubSimulation passes scale factors down to compartments correctly."""
        scale_factors = pd.DataFrame(
            {"compartment": ["A", "B"], "scale_factor": [0.5, 1.0]}
        )
        self.sub_simulation.scale_cohorts(scale_factors, 0)
        self.assertEqual(
            0.5, self.sub_simulation.simulation_compartments["A"].scale_factor  # type: ignore
        )
        self.assertEqual(
            1, self.sub_simulation.simulation_compartments["B"].scale_factor  # type: ignore
        )

    def test_cross_flow_passes_cohort_data_up(self) -> None:
        """Integration test that SubSimulation passes cohort data up to PopulationSimulation correctly."""
        self.assertEqual(
            len(self.sub_simulation.cross_flow()),
            2 * len(self.sub_simulation.simulation_compartments),
        )

    def test_get_population_projections_passes_populations_up(self) -> None:
        """Integration test that SubSimulation passes population data up to PopulationSimulation correctly."""
        self.assertEqual(
            len(self.sub_simulation.get_population_projections()),
            3 * len(self.sub_simulation.simulation_compartments),
        )

    def test_get_current_populations_passes_populations_up(self) -> None:
        """Integration test that SubSimulation passes current population data up to PopulationSimulation correctly."""
        current_populations = self.sub_simulation.get_current_populations()
        self.assertEqual(
            len(current_populations), len(self.sub_simulation.simulation_compartments)
        )
        for (
            compartment_name,
            compartment,
        ) in self.sub_simulation.simulation_compartments.items():
            self.assertEqual(
                current_populations[
                    current_populations.compartment == compartment_name
                ].total_population.sum(),
                1,
            )
