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
"""Test the SuperSimulation object"""

import unittest
import os
from typing import Dict, Any

from recidiviz.calculator.modeling.population_projection.super_simulation import SuperSimulation


def get_inputs_path(file_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), 'test_configurations', file_name)


class ParentSuperSimulation(SuperSimulation):
    def _initialize_data(self, initialization_params: Dict[str, Any]):
        pass

    def _set_user_inputs(self, yaml_user_inputs: Dict[str, Any]):
        pass

    def _simulate_baseline(self, simulation_title: str, first_relevant_ts: int = None):
        pass


class TestSuperSimulation(unittest.TestCase):
    """Test the SuperSimulation object runs correctly"""

    def test_simulation_architecture_must_match_compartment_costs(self):
        with open(get_inputs_path(
                'super_simulation_mismatched_compartments.yaml')) as test_configuration:
            with self.assertRaises(ValueError):
                ParentSuperSimulation(test_configuration)
