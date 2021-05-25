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

import unittest
from typing import List
from unittest.mock import patch

import pandas as pd
from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation_factory import (
    SubSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class TestSubSimulation(unittest.TestCase):
    """Test the SubSimulation runs correctly"""

    def setUp(self) -> None:
        self.test_outflow_data = pd.DataFrame(
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

        self.test_transitions_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 5],
                "total_population": [4, 2, 2],
                "outflow_to": ["supervision", "prison", "supervision"],
                "compartment": ["prison", "supervision", "prison"],
            }
        )

        self.starting_cohort_sizes = pd.DataFrame(
            {
                "total_population": [10],
                "compartment": ["prison"],
                "time_step": [0],
            }
        )

        self.test_architecture = {
            "pretrial": "shell",
            "supervision": "full",
            "prison": "full",
        }

        self.compartment_policies: List[SparkPolicy] = []

        self.test_user_inputs = {
            "start_time_step": 0,
            "policy_time_step": 5,
            "projection_time_steps": 10,
            "constant_admissions": False,
            "speed_run": False,
        }

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
                True,
                self.starting_cohort_sizes,
            )
            mock.assert_called_once()
            self.assertEqual(
                mock.mock_calls[0].args[0],
                "Some transitions data not fed to a compartment: %s",
            )
