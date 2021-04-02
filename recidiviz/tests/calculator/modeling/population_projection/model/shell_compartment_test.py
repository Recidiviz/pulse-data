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
"""Test the ShellCompartment object"""
from functools import partial
import unittest
import pandas as pd

from recidiviz.calculator.modeling.population_projection.full_compartment import (
    FullCompartment,
)
from recidiviz.calculator.modeling.population_projection.simulations.compartment_transitions import (
    CompartmentTransitions,
)
from recidiviz.calculator.modeling.population_projection.shell_compartment import (
    ShellCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class TestShellCompartment(unittest.TestCase):
    """Test the ShellCompartment class runs correctly"""

    def setUp(self):
        self.historical_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test"] * 5,
            }
        )

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
                "compartment": ["pretrial"] * 5,
                "time_step": [0, 0, 0, 0, 0],
            }
        )
        self.test_outflow_data = self.test_outflow_data.groupby(
            ["compartment", "outflow_to", "time_step"]
        )["total_population"].sum()
        # shadows logic in SubSimulationFactory._load_data()
        self.test_outflow_data = (
            self.test_outflow_data.unstack(level=["outflow_to", "time_step"])
            .stack(level="outflow_to", dropna=False)
            .loc["pretrial"]
            .fillna(0)
        )

        self.compartment_policies = []

        self.test_transition_table = CompartmentTransitions(self.historical_data)
        self.test_transition_table.initialize_transition_tables(
            self.compartment_policies
        )

        self.starting_ts = 2015
        self.policy_ts = 2018

    def test_all_edges_fed_to(self) -> None:
        """ShellCompartments require edges to the compartments defined in the outflows_data"""
        test_shell_compartment = ShellCompartment(
            self.test_outflow_data,
            starting_ts=self.starting_ts,
            tag="test_shell",
            constant_admissions=True,
            policy_list=[],
        )
        test_full_compartment = FullCompartment(
            pd.DataFrame(),
            self.test_transition_table,
            starting_ts=self.starting_ts,
            tag="test_compartment",
        )
        with self.assertRaises(ValueError):
            test_shell_compartment.initialize_edges(
                [test_shell_compartment, test_full_compartment]
            )

    def test_use_alternate_data_equal_to_differently_instantiated_shell_compartment(
        self,
    ) -> None:
        alternate_outflow_data = pd.DataFrame(
            {
                "total_population": [40, 21, 25, 30],
                "outflow_to": ["supervision", "prison", "supervision", "prison"],
                "compartment": ["pretrial"] * 4,
                "time_step": [1, 1, 2, 2],
            }
        )
        preprocessed_alternate_outflows = alternate_outflow_data.groupby(
            ["compartment", "outflow_to", "time_step"]
        )["total_population"].sum()
        # shadows logic in SubSimulationFactory._load_data()
        preprocessed_alternate_outflows = (
            preprocessed_alternate_outflows.unstack(level=["outflow_to", "time_step"])
            .stack(level="outflow_to", dropna=False)
            .loc["pretrial"]
            .fillna(0)
        )

        use_alternate_outflows = SparkPolicy(
            partial(
                ShellCompartment.use_alternate_outflows_data,
                alternate_outflows_data=alternate_outflow_data,
                tag="pretrial",
            ),
            spark_compartment="pretrial",
            sub_population={"subgroup": "test"},
            policy_ts=self.policy_ts,
            apply_retroactive=False,
        )

        policy_shell_compartment = ShellCompartment(
            self.test_outflow_data,
            starting_ts=self.starting_ts,
            tag="pretrial",
            constant_admissions=True,
            policy_list=[use_alternate_outflows],
        )

        alternate_shell_compartment = ShellCompartment(
            preprocessed_alternate_outflows,
            starting_ts=self.starting_ts,
            tag="pretrial",
            constant_admissions=True,
            policy_list=[],
        )

        self.assertEqual(
            policy_shell_compartment.admissions_predictors[
                max(policy_shell_compartment.admissions_predictors)
            ],
            alternate_shell_compartment.admissions_predictors[
                max(alternate_shell_compartment.admissions_predictors)
            ],
        )
