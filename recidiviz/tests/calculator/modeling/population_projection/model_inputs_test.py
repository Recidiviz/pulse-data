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
"""Tests that all YAMLs provided for population projection provide the correct format."""

import os
import unittest

from unittest import mock
from unittest.mock import patch

import pandas


from recidiviz.calculator.modeling import population_projection
from recidiviz.calculator.modeling.population_projection.simulations.super_simulation.super_simulation_factory import (
    SuperSimulationFactory,
)

root_dir_path = os.path.join(os.path.dirname(population_projection.__file__), "state")


class TestModelInputs(unittest.TestCase):
    """Test that all model inputs for population projections are valid"""

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.load_spark_table_from_big_query",
        mock.MagicMock(return_value=pandas.DataFrame([])),
    )
    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock.MagicMock(return_value=pandas.DataFrame([])),
    )
    def test_existing_model_inputs(self) -> None:
        for dir_path, _, files in os.walk(root_dir_path):
            for file in files:
                if not file.endswith("yaml") and not file.endswith("yml"):
                    continue
                file_path = os.path.join(dir_path, file)
                try:
                    _ = SuperSimulationFactory.build_super_simulation(file_path)
                except Exception as e:
                    e.args = (
                        f"Invalid configuration at {file}. " + e.args[0],
                    ) + e.args[1:]
                    raise
