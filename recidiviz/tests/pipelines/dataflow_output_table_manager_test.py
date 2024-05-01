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
import unittest
from typing import List, cast
from unittest import mock
from unittest.mock import MagicMock

from google.cloud import bigquery
from mock import patch
from mock.mock import Mock

from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines import dataflow_output_table_manager
from recidiviz.pipelines.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRIC_TYPES,
    METRIC_CLUSTERING_FIELDS,
)
from recidiviz.pipelines.dataflow_orchestration_utils import (
    get_normalization_pipeline_enabled_states,
)
from recidiviz.pipelines.metrics.population_spans.metrics import (
    IncarcerationPopulationSpanMetric,
    SupervisionPopulationSpanMetric,
)
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismRateMetric,
)
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.pipelines.normalization.dataset_config import (
    normalized_state_dataset_for_state_code,
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
            "recidiviz.pipelines.dataflow_output_table_manager.BigQueryClientImpl"
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

    def test_update_dataflow_metric_tables_schemas_create_table_with_prefix(
        self,
    ) -> None:
        """Test that update_dataflow_metric_tables_schemas calls the client to create a new table with a dataset prefix."""
        self.mock_client.table_exists.return_value = False

        dataflow_output_table_manager.update_dataflow_metric_tables_schemas(
            dataflow_metrics_dataset_id=self.fake_dataflow_metrics_dataset,
            sandbox_dataset_prefix="my_prefix",
        )

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.dataset_ref_for_id.assert_called_with(
            f"my_prefix_{self.fake_dataflow_metrics_dataset}"
        )

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
            "recidiviz.pipelines.dataflow_output_table_manager.BigQueryClientImpl"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.project_id.return_value = self.project_id

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()

    @mock.patch(
        "recidiviz.pipelines.dataflow_orchestration_utils.get_direct_ingest_states_launched_in_env",
        MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
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
                    ),
                    None,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_yy_normalized_state"
                    ),
                    None,
                ),
            ],
            any_order=True,
        )

    @mock.patch(
        "recidiviz.pipelines.dataflow_orchestration_utils.get_direct_ingest_states_launched_in_env",
        MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
    )
    def test_update_state_specific_normalized_state_schemas_adds_dataset_prefix(
        self,
    ) -> None:
        def mock_dataset_ref_for_id(
            dataset_id: str,
        ) -> bigquery.dataset.DatasetReference:
            return bigquery.dataset.DatasetReference(self.project_id, dataset_id)

        self.mock_client.dataset_ref_for_id.side_effect = mock_dataset_ref_for_id

        dataflow_output_table_manager.update_state_specific_normalized_state_schemas(
            sandbox_dataset_prefix="my_prefix"
        )

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "my_prefix_us_xx_normalized_state"
                    ),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "my_prefix_us_yy_normalized_state"
                    ),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
            ],
            any_order=True,
        )

    def test_update_normalized_table_schemas_in_dataset_create_table(self) -> None:
        """Test that update_normalized_table_schemas_in_dataset calls the client to
        create a new table when the table does not yet exist."""
        self.mock_client.table_exists.return_value = False

        dataflow_output_table_manager.update_normalized_table_schemas_in_dataset(
            "us_xx_normalized_state", default_table_expiration_ms=None
        )

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()

    def test_update_normalized_table_schemas_in_dataset_update_table(self) -> None:
        """Test that update_normalized_table_schemas_in_dataset calls the client to
        update a table when the table already exists."""
        self.mock_client.table_exists.return_value = True

        dataflow_output_table_manager.update_normalized_table_schemas_in_dataset(
            "us_xx_normalized_state", default_table_expiration_ms=None
        )
        self.mock_client.update_schema.assert_called()
        self.mock_client.create_table_with_schema.assert_not_called()

    @mock.patch(
        "recidiviz.pipelines.dataflow_orchestration_utils.get_direct_ingest_states_launched_in_env",
        MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
    )
    def test_get_all_state_specific_normalized_state_datasets(self) -> None:
        dataset_ids: List[str] = []
        for state_code in get_normalization_pipeline_enabled_states():
            dataset_ids.append(normalized_state_dataset_for_state_code(state_code))

        expected_dataset_ids = ["us_xx_normalized_state", "us_yy_normalized_state"]

        self.assertCountEqual(expected_dataset_ids, dataset_ids)

    @mock.patch(
        "recidiviz.pipelines.dataflow_output_table_manager"
        ".update_normalized_table_schemas_in_dataset"
    )
    def test_update_normalized_state_schema(
        self, mock_update_norm_schemas: mock.MagicMock
    ) -> None:
        mock_update_norm_schemas.return_value = [
            "state_incarceration_period",
            "state_supervision_period",
        ]

        dataflow_output_table_manager.update_normalized_state_schema()

    @mock.patch(
        "recidiviz.pipelines.dataflow_output_table_manager"
        ".update_normalized_table_schemas_in_dataset"
    )
    def test_update_normalized_state_schema_adds_dataset_prefix(
        self, mock_update_norm_schemas: mock.MagicMock
    ) -> None:
        mock_update_norm_schemas.return_value = [
            "state_incarceration_period",
            "state_supervision_period",
        ]

        dataflow_output_table_manager.update_normalized_state_schema(
            sandbox_dataset_prefix="my_prefix"
        )
        self.mock_client.dataset_ref_for_id.assert_called_with(
            f"my_prefix_{dataset_config.NORMALIZED_STATE_DATASET}"
        )


class SupplementalDatasetTableManagerTest(unittest.TestCase):
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
            "recidiviz.pipelines.dataflow_output_table_manager.BigQueryClientImpl"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset
        self.mock_client.project_id.return_value = self.project_id

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()

    def test_update_supplemental_data_schemas_create_table(self) -> None:
        """Test that update_supplemental_dataset_schemas calls the client to create a
        new table when the table does not yet exist."""
        self.mock_client.table_exists.return_value = False

        dataflow_output_table_manager.update_supplemental_dataset_schemas()

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()

    def test_update_supplemental_data_schemas_update_table(self) -> None:
        """Test that update_supplemental_dataset_schemas calls the client to update a
        table when the table already exists."""
        self.mock_client.table_exists.return_value = True

        dataflow_output_table_manager.update_supplemental_dataset_schemas()

        self.mock_client.update_schema.assert_called()
        self.mock_client.create_table_with_schema.assert_not_called()

    def test_update_supplemental_data_schemas_create_table_with_prefix(self) -> None:
        """Test that update_supplemental_dataset_schemas calls the client to create a
        new table with a dataset prefix."""
        self.mock_client.table_exists.return_value = False

        dataflow_output_table_manager.update_supplemental_dataset_schemas(
            supplemental_metrics_dataset_id="supplemental_metrics_dataset",
            sandbox_dataset_prefix="my_prefix",
        )

        self.mock_client.dataset_ref_for_id.assert_called_with(
            "my_prefix_supplemental_metrics_dataset"
        )
        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()


class IngestDatasetTableManagerTest(unittest.TestCase):
    """Tests for dataflow_output_table_manager.py"""

    def setUp(self) -> None:
        self.project_id = "recidiviz-project"
        self.mock_view_dataset_name = "my_views_dataset"
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.project_id, self.mock_view_dataset_name
        )

        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id
        self.project_number_patcher = patch("recidiviz.utils.metadata.project_number")
        self.project_number_patcher.start().return_value = "123456789"

        self.bq_client_patcher = mock.patch(
            "recidiviz.pipelines.dataflow_output_table_manager.BigQueryClientImpl"
        )
        self.mock_client = self.bq_client_patcher.start().return_value

        self.mock_client.project_id.return_value = self.project_id

        self.query_builder_patcher = mock.patch(
            "recidiviz.ingest.direct.views.direct_ingest_view_query_builder.DirectIngestViewQueryBuilder"
        )
        self.mock_query_builder = self.query_builder_patcher.start().return_value

        self.mock_query_builder.build_query.return_value = "SELECT * FROM table"
        self.mock_query_builder.ingest_view_name.return_value = "us_xx_view"

        self.get_view_builder_patcher = mock.patch(
            "recidiviz.pipelines.dataflow_output_table_manager._get_ingest_view_builders"
        )
        self.mock_get_view_builder = self.get_view_builder_patcher.start()

    def tearDown(self) -> None:
        self.bq_client_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.query_builder_patcher.stop()
        self.get_view_builder_patcher.stop()

    @mock.patch(
        "recidiviz.pipelines.dataflow_output_table_manager.get_ingest_pipeline_enabled_state_and_instance_pairs",
        return_value=[
            (StateCode.US_XX, DirectIngestInstance.PRIMARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
            (StateCode.US_YY, DirectIngestInstance.PRIMARY),
            (StateCode.US_YY, DirectIngestInstance.SECONDARY),
        ],
    )
    def test_update_state_specific_ingest_view_results_datasets(
        self, _mock_get_ingest_pipeline_enabled_state_and_instance_pairs: MagicMock
    ) -> None:
        def mock_dataset_ref_for_id(
            dataset_id: str,
        ) -> bigquery.dataset.DatasetReference:
            return bigquery.dataset.DatasetReference(self.project_id, dataset_id)

        self.mock_client.dataset_ref_for_id.side_effect = mock_dataset_ref_for_id
        self.mock_get_view_builder.return_value = []

        dataflow_output_table_manager.update_state_specific_ingest_view_results_schemas()

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_xx_dataflow_ingest_view_results_primary"
                    ),
                    None,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_xx_dataflow_ingest_view_results_secondary"
                    ),
                    None,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_yy_dataflow_ingest_view_results_primary"
                    ),
                    None,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_yy_dataflow_ingest_view_results_secondary"
                    ),
                    None,
                ),
            ],
            any_order=True,
        )

    @mock.patch(
        "recidiviz.pipelines.dataflow_output_table_manager.get_ingest_pipeline_enabled_state_and_instance_pairs",
        return_value=[
            (StateCode.US_XX, DirectIngestInstance.PRIMARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
            (StateCode.US_YY, DirectIngestInstance.PRIMARY),
            (StateCode.US_YY, DirectIngestInstance.SECONDARY),
        ],
    )
    def test_update_state_specific_ingest_view_results_datasets_with_dataset_prefix(
        self, _mock_get_ingest_pipeline_enabled_state_and_instance_pairs: MagicMock
    ) -> None:
        def mock_dataset_ref_for_id(
            dataset_id: str,
        ) -> bigquery.dataset.DatasetReference:
            return bigquery.dataset.DatasetReference(self.project_id, dataset_id)

        self.mock_client.dataset_ref_for_id.side_effect = mock_dataset_ref_for_id
        self.mock_get_view_builder.return_value = []

        dataflow_output_table_manager.update_state_specific_ingest_view_results_schemas(
            sandbox_dataset_prefix="my_prefix"
        )

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id,
                        "my_prefix_us_xx_dataflow_ingest_view_results_primary",
                    ),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id,
                        "my_prefix_us_xx_dataflow_ingest_view_results_secondary",
                    ),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id,
                        "my_prefix_us_yy_dataflow_ingest_view_results_primary",
                    ),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id,
                        "my_prefix_us_yy_dataflow_ingest_view_results_secondary",
                    ),
                    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
                ),
            ],
            any_order=True,
        )

    @mock.patch(
        "recidiviz.pipelines.dataflow_output_table_manager.get_ingest_pipeline_enabled_state_and_instance_pairs",
        return_value=[
            (StateCode.US_XX, DirectIngestInstance.PRIMARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
            (StateCode.US_YY, DirectIngestInstance.PRIMARY),
            (StateCode.US_YY, DirectIngestInstance.SECONDARY),
        ],
    )
    def test_update_state_specific_ingest_view_results_datasets_schema_update_failure(
        self, _mock_get_ingest_pipeline_enabled_state_and_instance_pairs: MagicMock
    ) -> None:
        def mock_dataset_ref_for_id(
            dataset_id: str,
        ) -> bigquery.dataset.DatasetReference:
            return bigquery.dataset.DatasetReference(self.project_id, dataset_id)

        self.mock_client.dataset_ref_for_id.side_effect = mock_dataset_ref_for_id
        self.mock_client.update_schema.side_effect = ValueError(
            "Failed to update schema"
        )
        self.mock_get_view_builder.return_value = []

        dataflow_output_table_manager.update_state_specific_ingest_view_results_schemas()

        self.mock_client.create_dataset_if_necessary.assert_has_calls(
            [
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_xx_dataflow_ingest_view_results_primary"
                    ),
                    None,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_xx_dataflow_ingest_view_results_secondary"
                    ),
                    None,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_yy_dataflow_ingest_view_results_primary"
                    ),
                    None,
                ),
                mock.call(
                    bigquery.dataset.DatasetReference(
                        self.project_id, "us_yy_dataflow_ingest_view_results_secondary"
                    ),
                    None,
                ),
            ],
            any_order=True,
        )

    def test_update_state_specific_ingest_view_results_schema_create_table(
        self,
    ) -> None:
        """Test that update_state_specific_ingest_view_result_schema calls the client to
        create a new table when the table does not yet exist."""

        def mock_dataset_ref_for_id(
            dataset_id: str,
        ) -> bigquery.dataset.DatasetReference:
            return bigquery.dataset.DatasetReference(self.project_id, dataset_id)

        self.mock_client.dataset_ref_for_id.side_effect = mock_dataset_ref_for_id

        self.mock_client.table_exists.return_value = False
        self.mock_get_view_builder.return_value = [self.mock_query_builder]

        self.mock_client.run_query.return_value = mock.create_autospec(
            bigquery.QueryJob
        )

        dataflow_output_table_manager.update_state_specific_ingest_view_result_schema(
            "us_xx_dataflow_ingest_view_results_primary",
            StateCode.US_XX,
            DirectIngestInstance.PRIMARY,
            default_table_expiration_ms=None,
        )

        self.mock_client.create_table_with_schema.assert_called()
        self.mock_client.update_schema.assert_not_called()

    def test_update_state_specific_ingest_view_results_schema_update_table(
        self,
    ) -> None:
        """Test that update_state_specific_ingest_view_result_schema calls the client to
        update a table when the table already exists."""

        def mock_dataset_ref_for_id(
            dataset_id: str,
        ) -> bigquery.dataset.DatasetReference:
            return bigquery.dataset.DatasetReference(self.project_id, dataset_id)

        self.mock_client.dataset_ref_for_id.side_effect = mock_dataset_ref_for_id

        self.mock_client.table_exists.return_value = True
        self.mock_get_view_builder.return_value = [self.mock_query_builder]

        self.mock_client.run_query.return_value = mock.create_autospec(
            bigquery.QueryJob
        )

        dataflow_output_table_manager.update_state_specific_ingest_view_result_schema(
            "us_xx_dataflow_ingest_view_results_primary",
            StateCode.US_XX,
            DirectIngestInstance.PRIMARY,
            default_table_expiration_ms=None,
        )
        self.mock_client.update_schema.assert_called()
        self.mock_client.create_table_with_schema.assert_not_called()
