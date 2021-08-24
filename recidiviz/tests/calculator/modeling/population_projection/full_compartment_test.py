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
from typing import List, Optional

import pandas as pd

from recidiviz.calculator.modeling.population_projection.compartment_transitions import (
    CompartmentTransitions,
)
from recidiviz.calculator.modeling.population_projection.full_compartment import (
    FullCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class TestFullCompartment(unittest.TestCase):
    """Test the FullCompartment runs correctly"""

    test_supervision_data = pd.DataFrame()
    test_incarceration_data = pd.DataFrame()
    compartment_policies: List[SparkPolicy] = []
    incarceration_transition_table: Optional[CompartmentTransitions] = None
    release_transition_table: Optional[CompartmentTransitions] = None
    historical_data = pd.DataFrame()

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_supervision_data = pd.DataFrame(
            {
                "compartment_duration": [1, 1, 2, 2.5, 10],
                "total_population": [4, 2, 2, 4, 3],
                "outflow_to": ["jail", "prison", "jail", "prison", "prison"],
                "compartment": ["test"] * 5,
            }
        )
        cls.test_incarceration_data = pd.DataFrame(
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

        cls.incarceration_transition_table = CompartmentTransitions(
            cls.test_incarceration_data
        )
        cls.incarceration_transition_table.initialize_transition_tables(
            cls.compartment_policies
        )

        cls.release_transition_table = CompartmentTransitions(cls.test_supervision_data)
        cls.release_transition_table.initialize_transition_tables(
            cls.compartment_policies
        )

        cls.historical_data = pd.DataFrame(
            {
                2015: {"jail": 2, "prison": 2},
                2016: {"jail": 1, "prison": 0},
                2017: {"jail": 1, "prison": 1},
            }
        )

    def test_step_forward_fails_without_initialized_edges(self) -> None:
        """Tests that step_forward() needs the initialize_edges() to have been run"""
        assert self.release_transition_table is not None

        rel_compartment = FullCompartment(
            self.historical_data, self.release_transition_table, 2015, "release"
        )
        with self.assertRaises(ValueError):
            rel_compartment.step_forward()

    def test_all_edges_fed_to(self) -> None:
        """Tests that all edges in self.edges are included in self.compartment_transitions"""
        assert self.incarceration_transition_table is not None
        assert self.release_transition_table is not None

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
