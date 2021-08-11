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
"""Tests for Spark BigQuery Utils."""
import os
import unittest
from math import nan
from typing import Any
from unittest.mock import patch

import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)


def get_inputs_path(file_name: str) -> str:
    return os.path.join(
        os.path.dirname(__file__), "super_simulation/test_configurations", file_name
    )


class TestSparkBQUtils(unittest.TestCase):
    """Tests that the validation functions in the spark bq utils work correctly."""

    def setUp(self) -> None:
        self.outflows_data = pd.DataFrame(
            {
                "compartment": ["PRETRIAL"] * 12,
                "outflow_to": ["PRISON"] * 12,
                "time_step": list(range(5, 11)) * 2,
                "crime_type": ["NONVIOLENT"] * 6 + ["VIOLENT"] * 6,
                "total_population": [100.0]
                + [100.0 + 2 * i for i in range(5)]
                + [10]
                + [10 + i for i in range(5)],
            }
        )

        self.outflows_data_no_disaggregation_axis = pd.DataFrame(
            {
                "compartment": ["PRETRIAL"] * 12,
                "outflow_to": ["PRISON"] * 12,
                "time_step": list(range(5, 11)) * 2,
                "total_population": [100.0]
                + [100.0 + 2 * i for i in range(5)]
                + [10]
                + [10 + i for i in range(5)],
            }
        )

        self.outflows_data_wrong_disaggregation_axis = pd.DataFrame(
            {
                "compartment": ["PRETRIAL"] * 12,
                "outflow_to": ["PRISON"] * 12,
                "time_step": list(range(5, 11)) * 2,
                "age": ["young"] * 6 + ["old"] * 6,
                "total_population": [100.0]
                + [100.0 + 2 * i for i in range(5)]
                + [10]
                + [10 + i for i in range(5)],
            }
        )

        self.transitions_data = pd.DataFrame(
            {
                "compartment": ["PRISON", "PRISON", "RELEASE", "RELEASE"] * 2,
                "outflow_to": ["RELEASE", "RELEASE", "PRISON", "RELEASE"] * 2,
                "compartment_duration": [3.0, 5.0, 3.0, 50.0] * 2,
                "crime_type": ["NONVIOLENT"] * 4 + ["VIOLENT"] * 4,
                "total_population": [0.6, 0.4, 0.3, 0.7] * 2,
            }
        )

        self.transitions_data_with_null_values = pd.DataFrame(
            {
                "compartment": ["PRISON", "PRISON", "RELEASE", "RELEASE"] * 2,
                "outflow_to": ["RELEASE", "RELEASE", "PRISON", "RELEASE"] * 2,
                "compartment_duration": [3.0, 5.0, 3.0, 50.0] * 2,
                "crime_type": [nan] * 4 + ["VIOLENT"] * 4,  # type: ignore
                "total_population": [0.6, 0.4, 0.3, 0.7] * 2,
            }
        )

        self.transitions_data_wrong_disaggregation_axis = pd.DataFrame(
            {
                "compartment": ["PRISON", "PRISON", "RELEASE", "RELEASE"] * 2,
                "outflow_to": ["RELEASE", "RELEASE", "PRISON", "RELEASE"] * 2,
                "compartment_duration": [3.0, 5.0, 3.0, 50.0] * 2,
                "age": ["young"] * 4 + ["old"] * 4,
                "total_population": [0.6, 0.4, 0.3, 0.7] * 2,
            }
        )

        self.total_population_data = pd.DataFrame(
            {
                "compartment": ["PRISON", "RELEASE"] * 2,
                "time_step": [9] * 4,
                "crime_type": ["NONVIOLENT"] * 2 + ["VIOLENT"] * 2,
                "total_population": [300.0, 500.0, 30.0, 50.0],
            }
        )

        self.total_population_data_wrong_disaggregation_axis = pd.DataFrame(
            {
                "compartment": ["PRISON", "RELEASE"] * 2,
                "time_step": [9] * 4,
                "age": ["young"] * 2 + ["old"] * 2,
                "total_population": [300.0, 500.0, 30.0, 50.0],
            }
        )

        self.total_population_data_missing_column = pd.DataFrame(
            {
                "compartment": ["PRISON", "RELEASE"] * 2,
                "crime_type": ["NONVIOLENT"] * 2 + ["VIOLENT"] * 2,
                "total_population": [300.0, 500.0, 30.0, 50.0],
            }
        )

        self.total_population_data_extra_column = pd.DataFrame(
            {
                "compartment": ["PRISON", "RELEASE"] * 2,
                "time_step": [9] * 4,
                "crime_type": ["NONVIOLENT"] * 2 + ["VIOLENT"] * 2,
                "total_population": [300.0, 500.0, 30.0, 50.0],
                "random_extra_column": [1, 1, 1, 1],
            }
        )

        self.total_population_data_wrong_type = pd.DataFrame(
            {
                "compartment": ["PRISON", "RELEASE"] * 2,
                "time_step": [9] * 4,
                "crime_type": ["NONVIOLENT"] * 2 + ["VIOLENT"] * 2,
                "total_population": [300, 500, 30, 50],
            }
        )

    def test_upload_spark_model_inputs_with_missing_yaml_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "^Missing yaml inputs"):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data,
                self.transitions_data,
                self.total_population_data,
                get_inputs_path("super_simulation_missing_inputs.yaml"),
            )

    def test_upload_spark_model_inputs_with_unexpected_yaml_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "^Unexpected yaml inputs"):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data,
                self.transitions_data,
                self.total_population_data,
                get_inputs_path("super_simulation_unexpected_inputs.yaml"),
            )

    def test_upload_spark_model_inputs_with_wrong_disaggregation_axis_in_yaml(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^All disagregation axes must be included in the input dataframe columns\n"
            r"Expected: \['crime_type'\], Actual: Index\(\['compartment', 'outflow_to', 'time_step', 'age', 'total_population'\], dtype='object'\)$",
        ):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data_wrong_disaggregation_axis,
                self.transitions_data_wrong_disaggregation_axis,
                self.total_population_data_wrong_disaggregation_axis,
                get_inputs_path("super_simulation_data_ingest.yaml"),
            )

    def test_upload_spark_model_inputs_with_invalid_project_id(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "^bad_project_id is not a supported gcloud BigQuery project$"
        ):
            upload_spark_model_inputs(
                "bad_project_id",
                "test",
                self.outflows_data,
                self.transitions_data,
                self.total_population_data,
                get_inputs_path("super_simulation_data_ingest.yaml"),
            )

    def test_upload_spark_model_inputs_with_missing_disaggregation_axis(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Tables \['outflows_data'\] must have dissaggregation axis of 'crime', 'crime_type', 'age', or 'race'$",
        ):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data_no_disaggregation_axis,
                self.transitions_data,
                self.total_population_data,
                get_inputs_path("super_simulation_data_ingest.yaml"),
            )

    def test_upload_spark_model_inputs_with_null_values(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "^Table 'transitions_data' must not contain null values$"
        ):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data,
                self.transitions_data_with_null_values,
                self.total_population_data,
                get_inputs_path("super_simulation_data_ingest.yaml"),
            )

    def test_upload_spark_model_inputs_with_missing_column(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Table 'total_population_data' missing required columns \{'time_step'\}$",
        ):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data,
                self.transitions_data,
                self.total_population_data_missing_column,
                get_inputs_path("super_simulation_data_ingest.yaml"),
            )

    def test_upload_spark_model_inputs_with_extra_column(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Table 'total_population_data' contains unexpected columns \{'random_extra_column'\}$",
        ):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data,
                self.transitions_data,
                self.total_population_data_extra_column,
                get_inputs_path("super_simulation_data_ingest.yaml"),
            )

    def test_upload_spark_model_inputs_with_column_wrong_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^Table 'total_population_data' has wrong type for column 'total_population'. Type 'int64' should be 'float64'$",
        ):
            upload_spark_model_inputs(
                "recidiviz-staging",
                "test",
                self.outflows_data,
                self.transitions_data,
                self.total_population_data_wrong_type,
                get_inputs_path("super_simulation_data_ingest.yaml"),
            )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.store_simulation_results"
    )
    def test_upload_spark_model_inputs_with_valid_inputs(self, mock_store: Any) -> None:
        upload_spark_model_inputs(
            "recidiviz-staging",
            "test",
            self.outflows_data,
            self.transitions_data,
            self.total_population_data,
            get_inputs_path("super_simulation_data_ingest.yaml"),
        )
        assert mock_store.call_count == 3
