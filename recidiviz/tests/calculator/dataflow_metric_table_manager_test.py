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
"""Tests the dataflow_metric_table_manager."""
import unittest
from typing import cast
from unittest import mock
from mock import patch

from google.cloud import bigquery

from recidiviz.calculator import dataflow_metric_table_manager
from recidiviz.calculator.dataflow_config import (
    DATAFLOW_TABLES_TO_METRIC_TYPES,
    DATAFLOW_METRICS_TO_TABLES,
)
from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetric


class DataflowMetricTableManagerTest(unittest.TestCase):
    """Tests for dataflow_metric_table_manager.py."""

    def setUp(self) -> None:

        self.project_id = "fake-recidiviz-project"
        self.mock_view_dataset_name = "my_views_dataset"
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.project_id, self.mock_view_dataset_name
        )

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id
        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"

        self.bq_client_patcher = mock.patch(
            "recidiviz.calculator.dataflow_metric_table_manager.BigQueryClientImpl"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset
        self.mock_dataflow_tables = [
            bigquery.TableReference(self.mock_dataset, "fake_table_1"),
            bigquery.TableReference(self.mock_dataset, "fake_table_2"),
        ]
        self.mock_client.list_tables.return_value = self.mock_dataflow_tables
        self.mock_client.project_id.return_value = self.project_id

        self.fake_dataflow_metrics_dataset = "fake_dataflow_metrics_dataset"
        self.fake_cold_storage_dataset = "fake_cold_storage_dataset"
        self.fake_max_jobs = 10

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()

    def test_update_dataflow_metric_tables_schemas(self) -> None:
        """Test that update_dataflow_metric_tables_schemas calls the client to update the schemas of the metric
        tables."""
        dataflow_metric_table_manager.update_dataflow_metric_tables_schemas()

        self.mock_client.update_schema.assert_called()

    def test_update_dataflow_metric_tables_schemas_create_table(self) -> None:
        """Test that update_dataflow_metric_tables_schemas calls the client to create a new table when the table
        does not yet exist."""
        self.mock_client.table_exists.return_value = False

        dataflow_metric_table_manager.update_dataflow_metric_tables_schemas()

        self.mock_client.create_table_with_schema.assert_called()

    def test_dataflow_metric_table_config_consistency(self) -> None:
        """Asserts that the mapping from RecidivizMetric to table in BigQuery matches
        the mapping from BigQuery table to the RecidivizMetricType enum in the metric_type
        column of that table."""
        # Assert that every metric class in the DATAFLOW_METRICS_TO_TABLES map has
        # a matching entry in the DATAFLOW_TABLES_TO_METRIC_TYPES map
        for metric_class, metric_table in DATAFLOW_METRICS_TO_TABLES.items():
            # Hack to get the value of the metric_type on this RecidivizMetric class
            metric_instance = cast(
                RecidivizMetric,
                metric_class.build_from_dictionary(
                    {"job_id": "xxx", "state_code": "US_XX"}
                ),
            )

            metric_type_from_class = metric_instance.metric_type
            metric_type_from_table = DATAFLOW_TABLES_TO_METRIC_TYPES[metric_table]

            self.assertEqual(metric_type_from_class, metric_type_from_table)

        # Invert the DATAFLOW_METRICS_TO_TABLES dictionary so that we have
        # tables -> metric types
        dataflow_tables_to_metric_classes = {
            v: k for k, v in DATAFLOW_METRICS_TO_TABLES.items()
        }

        # Assert that every metric table and metric type in the
        # DATAFLOW_TABLES_TO_METRIC_TYPES map has a matching entry in the
        # DATAFLOW_METRICS_TO_TABLES map
        for metric_table, metric_type in DATAFLOW_TABLES_TO_METRIC_TYPES.items():
            metric_class_for_table = dataflow_tables_to_metric_classes[metric_table]

            # Hack to get the value of the metric_type on this RecidivizMetric class
            metric_instance = cast(
                RecidivizMetric,
                metric_class_for_table.build_from_dictionary(
                    {"job_id": "xxx", "state_code": "US_XX"}
                ),
            )

            metric_type_from_class = metric_instance.metric_type

            self.assertEqual(metric_type_from_class, metric_type)
