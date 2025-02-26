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
"""Tests the dataflow_output_table_manager."""
import os
import unittest
from typing import List, cast
from unittest import mock

from google.cloud import bigquery
from mock import patch
from mock.mock import Mock

from recidiviz.calculator import dataflow_output_table_manager
from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
    METRIC_CLUSTERING_FIELDS,
)
from recidiviz.calculator.dataflow_orchestration_utils import (
    get_metric_pipeline_enabled_states,
)
from recidiviz.calculator.pipeline.metrics.population_spans.metrics import (
    IncarcerationPopulationSpanMetric,
    SupervisionPopulationSpanMetric,
)
from recidiviz.calculator.pipeline.metrics.recidivism.metrics import (
    ReincarcerationRecidivismRateMetric,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view_collector_test import (
    NO_PAUSED_REGIONS_CLOUD_SQL_CONFIG_YAML,
)

FAKE_PIPELINE_CONFIG_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "fake_calculation_pipeline_templates.yaml",
)


class DataflowMetricTableManagerTest(unittest.TestCase):
    """Tests the update_dataflow_metric_tables_schemas function in
    dataflow_output_table_manager.py."""

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
            "recidiviz.calculator.dataflow_output_table_manager.BigQueryClientImpl"
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

    def test_update_dataflow_metric_tables_schemas_with_clustering_set(self) -> None:
        """Test that update_dataflow_metric_tables_schemas calls the client to update the schemas of the metric
        tables when the clustering fields are correct."""

        def get_table(_dataset_id: str, table_id: str) -> Mock:
            table = Mock()
            # These tables are special, it does not have year and month
            if table_id in (
                DATAFLOW_METRICS_TO_TABLES[ReincarcerationRecidivismRateMetric],
                DATAFLOW_METRICS_TO_TABLES[IncarcerationPopulationSpanMetric],
                DATAFLOW_METRICS_TO_TABLES[SupervisionPopulationSpanMetric],
            ):
                table.clustering_fields = None
            else:
                table.clustering_fields = METRIC_CLUSTERING_FIELDS
            return table

        self.mock_client.get_table.side_effect = get_table
        dataflow_output_table_manager.update_dataflow_metric_tables_schemas()
        self.mock_client.update_schema.assert_called()

    def test_update_dataflow_metric_tables_schemas_without_clustering_set(self) -> None:
        """Test that update_dataflow_metric_tables_schemas fails when the clustering fields are incorrect."""

        self.mock_client.get_table.return_value.clustering_fields = None
        with self.assertRaises(ValueError):
            dataflow_output_table_manager.update_dataflow_metric_tables_schemas()

    def test_update_dataflow_metric_tables_schemas_create_table(self) -> None:
        """Test that update_dataflow_metric_tables_schemas calls the client to create a new table when the table
        does not yet exist."""
        self.mock_client.table_exists.return_value = False

        dataflow_output_table_manager.update_dataflow_metric_tables_schemas()

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


class NormalizedStateTableManagerTest(unittest.TestCase):
    """Tests for dataflow_output_table_manager.py."""

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
            "recidiviz.calculator.dataflow_output_table_manager.BigQueryClientImpl"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.project_id.return_value = self.project_id

        self.gcs_factory_patcher = mock.patch(
            "recidiviz.admin_panel.dataset_metadata_store.GcsfsFactory.build"
        )
        self.fake_fs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_fs

        self.fake_config_path = GcsfsFilePath.from_absolute_path(
            "gs://fake-recidiviz-project-configs/cloud_sql_to_bq_config.yaml"
        )

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.gcs_factory_patcher.stop()

    @mock.patch(
        "recidiviz.calculator.dataflow_orchestration_utils"
        ".PIPELINE_CONFIG_YAML_PATH",
        FAKE_PIPELINE_CONFIG_YAML_PATH,
    )
    def test_update_state_specific_normalized_state_schemas(self) -> None:
        def mock_dataset_ref_for_id(
            dataset_id: str,
        ) -> bigquery.dataset.DatasetReference:
            return bigquery.dataset.DatasetReference(self.project_id, dataset_id)

        self.mock_client.dataset_ref_for_id.side_effect = mock_dataset_ref_for_id

        dataflow_output_table_manager.update_state_specific_normalized_state_schemas()

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_xx_normalized_state"
                    )
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_yy_normalized_state"
                    )
                ),
            ],
            any_order=True,
        )

    def test_update_normalized_table_schemas_in_dataset_create_table(self) -> None:
        """Test that update_normalized_table_schemas_in_dataset calls the client to
        create a new table when the table does not yet exist."""
        self.mock_client.table_exists.return_value = False

        dataflow_output_table_manager.update_normalized_table_schemas_in_dataset(
            "us_xx_normalized_state"
        )

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()

    def test_update_normalized_table_schemas_in_dataset_update_table(self) -> None:
        """Test that update_normalized_table_schemas_in_dataset calls the client to
        update a table when the table already exists."""
        self.mock_client.table_exists.return_value = True

        dataflow_output_table_manager.update_normalized_table_schemas_in_dataset(
            "us_xx_normalized_state"
        )
        self.mock_client.update_schema.assert_called()
        self.mock_client.create_table_with_schema.assert_not_called()

    @mock.patch(
        "recidiviz.calculator.dataflow_orchestration_utils.PIPELINE_CONFIG_YAML_PATH",
        FAKE_PIPELINE_CONFIG_YAML_PATH,
    )
    def test_get_all_state_specific_normalized_state_datasets(self) -> None:
        dataset_ids: List[str] = []
        for state_code in get_metric_pipeline_enabled_states():
            dataset_ids.append(
                dataflow_output_table_manager.normalized_state_dataset_for_state_code(
                    state_code
                )
            )

        expected_dataset_ids = ["us_xx_normalized_state", "us_yy_normalized_state"]

        self.assertCountEqual(expected_dataset_ids, dataset_ids)

    @mock.patch(
        "recidiviz.calculator.dataflow_output_table_manager"
        ".update_normalized_table_schemas_in_dataset"
    )
    def test_update_normalized_state_schema(
        self, mock_update_norm_schemas: mock.MagicMock
    ) -> None:
        self.fake_fs.upload_from_string(
            path=self.fake_config_path,
            contents=NO_PAUSED_REGIONS_CLOUD_SQL_CONFIG_YAML,
            content_type="text/yaml",
        )

        mock_update_norm_schemas.return_value = [
            "state_incarceration_period",
            "state_supervision_period",
        ]

        dataflow_output_table_manager.update_normalized_state_schema()


class SupplementalDatasetTableManagerTest(unittest.TestCase):
    """Tests for dataflow_output_table_manager.py."""

    def setUp(self) -> None:

        self.project_id = "fake-recidiviz-project"
        self.mock_view_dataset_name = "my_views_dataset"
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.project_id, self.mock_view_dataset_name
        )

        self.dataflow_config_patcher = mock.patch(
            "recidiviz.calculator.dataflow_output_table_manager.dataflow_config"
        )
        self.mock_dataflow_config = self.dataflow_config_patcher.start()

        self.mock_pipeline_template_path = FAKE_PIPELINE_CONFIG_YAML_PATH
        self.mock_dataflow_config.PIPELINE_CONFIG_YAML_PATH = (
            self.mock_pipeline_template_path
        )

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id
        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"

        self.bq_client_patcher = mock.patch(
            "recidiviz.calculator.dataflow_output_table_manager.BigQueryClientImpl"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset
        self.mock_client.project_id.return_value = self.project_id

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.dataflow_config_patcher.stop()

    def test_update_supplemental_data_schemas_create_table(self) -> None:
        """Test that update_normalized_state_schema calls the client to create a
        new table when the table does not yet exist."""
        self.mock_client.table_exists.return_value = False

        dataflow_output_table_manager.update_supplemental_dataset_schemas()

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()

    def test_update_supplemental_data_schemas_update_table(self) -> None:
        """Test that update_normalized_state_schema calls the client to update a
        table when the table already exists."""
        self.mock_client.table_exists.return_value = True

        dataflow_output_table_manager.update_supplemental_dataset_schemas()

        self.mock_client.update_schema.assert_called()
        self.mock_client.create_table_with_schema.assert_not_called()
