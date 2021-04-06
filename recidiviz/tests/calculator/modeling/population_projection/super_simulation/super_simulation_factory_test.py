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
"""Tests for the SuperSimulationFactory."""
import os
import unittest
from unittest.mock import patch

from recidiviz.calculator.modeling.population_projection.super_simulation.super_simulation_factory import (
    SuperSimulationFactory,
)
from recidiviz.tests.calculator.modeling.population_projection.super_simulation.super_simulation_test import (
    mock_load_table_from_big_query_macro,
    mock_load_table_from_big_query_micro,
)


def get_inputs_path(file_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "test_configurations", file_name)


class TestSuperSimulationFactory(unittest.TestCase):
    """Tests the SuperSimulationFactory works correctly."""

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.load_spark_table_from_big_query",
        mock_load_table_from_big_query_macro,
    )
    def test_build_super_simulation_macrosim(self) -> None:
        macrosim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_data_ingest.yaml")
        )
        self.assertFalse(macrosim.initializer.microsim)
        self.assertFalse(macrosim.simulator.microsim)
        self.assertFalse(macrosim.validator.microsim)
        self.assertFalse(macrosim.exporter.microsim)

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_micro,
    )
    def test_build_super_simulation_microsim(self) -> None:
        microsim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_model_inputs.yaml")
        )
        self.assertTrue(microsim.initializer.microsim)
        self.assertTrue(microsim.simulator.microsim)
        self.assertTrue(microsim.validator.microsim)
        self.assertTrue(microsim.exporter.microsim)

    def test_invalid_yaml_configs(self) -> None:
        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(
                get_inputs_path("super_simulation_invalid_reference_date.yaml")
            )

        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(
                get_inputs_path("super_simulation_extra_data_inputs.yaml")
            )

        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(
                get_inputs_path("super_simulation_extra_user_inputs.yaml")
            )
