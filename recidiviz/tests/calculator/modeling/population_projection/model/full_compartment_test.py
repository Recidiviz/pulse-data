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
"""Test the FullCompartment object"""

import unittest
import pandas as pd
from recidiviz.calculator.modeling.population_projection.full_compartment import (
    FullCompartment,
)
from recidiviz.calculator.modeling.population_projection.simulations.compartment_transitions import (
    CompartmentTransitions,
)


class TestFullCompartment(unittest.TestCase):
    """Test the FullCompartment runs correctly"""

    def setUp(self):
        self.test_supervision_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test"] * 5,
            }
        )
        self.test_incarceration_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": [
                    "supervision",
                    "release",
                    "supervision",
                    "release",
                    "release",
                ],
                "compartment": ["test"] * 5,
            }
        )

        self.compartment_policies = []

        self.incarceration_transition_table = CompartmentTransitions(
            self.test_incarceration_data
        )
        self.incarceration_transition_table.initialize_transition_tables(
            self.compartment_policies
        )

        self.release_transition_table = CompartmentTransitions(
            self.test_supervision_data
        )
        self.release_transition_table.initialize_transition_tables(
            self.compartment_policies
        )

        self.historical_data = pd.DataFrame(
            {
                2015: {"jail": 2, "prison": 2},
                2016: {"jail": 1, "prison": 0},
                2017: {"jail": 1, "prison": 1},
            }
        )

    def test_step_forward_fails_without_initialized_edges(self):
        """Tests that step_forward() needs the initialize_edges() to have been run"""
        rel_compartment = FullCompartment(
            self.historical_data, self.release_transition_table, 2015, "release"
        )
        with self.assertRaises(ValueError):
            rel_compartment.step_forward()

    def test_all_edges_fed_to(self):
        """Tests that all edges in self.edges are included in self.compartment_transitions"""
        rel_compartment = FullCompartment(
            self.historical_data, self.release_transition_table, 2015, "release"
        )
        test_compartment = FullCompartment(
            self.historical_data,
            self.incarceration_transition_table,
            2015,
            "test_compartment",
        )
        compartment_list = [rel_compartment, test_compartment]
        for compartment in compartment_list:
            compartment.initialize_edges(compartment_list)

        for compartment in compartment_list:
            compartment.step_forward()
