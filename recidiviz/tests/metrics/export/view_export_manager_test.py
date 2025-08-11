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

"""Tests for view_export_manager.py."""
import json
import unittest
from typing import Sequence
from unittest import mock
from unittest.mock import MagicMock, Mock, patch

from google.cloud import bigquery

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.export.big_query_view_exporter import ViewExportValidationError
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
    ExportValidationType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export import view_export_manager
from recidiviz.metrics.export.export_config import ExportViewCollectionConfig
from recidiviz.metrics.export.view_export_manager import (
    MetricViewDataExportSuccessPersister,
    ViewExportConfigurationError,
    execute_metric_view_data_export,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    GCPEnvironment,
)
from recidiviz.utils.metadata import local_project_id_override


@mock.patch(
    "recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789")
)
@mock.patch(
    f"{view_export_manager.__name__}.PRODUCTS_CONFIG_PATH",
    fixtures.as_filepath("fixture_products.yaml"),
)
class ViewCollectionExportManagerTest(unittest.TestCase):
    """Tests for view_export_manager.py."""

    def setUp(self) -> None:
        self.mock_cloud_task_client_patcher = mock.patch(
            "google.cloud.tasks_v2.CloudTasksClient"
        )
        self.mock_cloud_task_client_patcher.start()
        self.mock_state_code = "US_XX"
        self.mock_project_id = GCP_PROJECT_STAGING
        self.mock_dataset_id = "base_dataset"
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id
        )

        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.metric_view_data_export_success_persister_patcher = patch(
            "recidiviz.metrics.export.view_export_manager.MetricViewDataExportSuccessPersister"
        )
        self.metric_view_data_export_success_persister_constructor = (
            self.metric_view_data_export_success_persister_patcher.start()
        )
        self.mock_metric_view_data_export_success_persister = (
            self.metric_view_data_export_success_persister_constructor.return_value
        )

        self.client_patcher = mock.patch(
            "recidiviz.metrics.export.view_export_manager.BigQueryClientImpl"
        )
        self.mock_client = self.client_patcher.start().return_value

        self.mock_view_builder = SimpleBigQueryViewBuilder(
            dataset_id=self.mock_dataset.dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
        )
        self.mock_metric_view_builder = MetricBigQueryViewBuilder(
            dataset_id=self.mock_dataset.dataset_id,
            view_id="test_view",
            description="test_view description",
            view_query_template="SELECT NULL LIMIT 0",
            dimensions=tuple(),
        )

        self.view_builders_for_dataset: list[BigQueryViewBuilder] = [
            self.mock_view_builder,
            self.mock_metric_view_builder,
        ]

        self.output_uri_template_for_dataset = {
            "dataset_id": "gs://{project_id}-dataset-location/subdirectory",
        }

        self.views_to_update = {self.mock_dataset_id: self.view_builders_for_dataset}

        self.mock_export_name = "MOCK_EXPORT_NAME"

        self.metric_dataset_export_configs_index = {
            "EXPORT": ExportViewCollectionConfig(
                view_builders_to_export=[self.mock_view_builder],
                output_directory_uri_template="gs://{project_id}-dataset-location/subdirectory",
                export_name="EXPORT",
            ),
            "OTHER_EXPORT": ExportViewCollectionConfig(
                view_builders_to_export=[self.mock_metric_view_builder],
                output_directory_uri_template="gs://{project_id}-dataset-location/subdirectory",
                export_name="OTHER_EXPORT",
            ),
            self.mock_export_name: ExportViewCollectionConfig(
                view_builders_to_export=self.view_builders_for_dataset,
                output_directory_uri_template="gs://{project_id}-dataset-location/subdirectory",
                export_name=self.mock_export_name,
            ),
        }

        export_config_values = {
            "OUTPUT_DIRECTORY_URI_TEMPLATE_FOR_DATASET_EXPORT": self.output_uri_template_for_dataset,
            "VIEW_COLLECTION_EXPORT_INDEX": self.metric_dataset_export_configs_index,
        }

        self.export_config_patcher = mock.patch(  # type: ignore[call-overload]
            "recidiviz.metrics.export.view_export_manager.export_config",
            **export_config_values,
        )
        self.mock_export_config = self.export_config_patcher.start()

        self.gcs_factory_patcher = mock.patch(
            "recidiviz.metrics.export.view_export_manager.GcsfsFactory.build"
        )
        self.gcs_factory_patcher.start().return_value = FakeGCSFileSystem()
        self.environment_patcher = mock.patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        )
        self.environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.client_patcher.stop()
        self.export_config_patcher.stop()
        self.metadata_patcher.stop()
        self.gcs_factory_patcher.stop()
        self.mock_cloud_task_client_patcher.stop()
        self.metric_view_data_export_success_persister_patcher.stop()

    def test_get_configs_for_export_name(self) -> None:
        """Tests get_configs_for_export_name function to ensure that export names correctly match"""
        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name,
            state_code=self.mock_state_code,
        )
        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        expected_view_config_list = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=f" WHERE state_code = '{self.mock_state_code}'",
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table_{self.mock_state_code}",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/{self.mock_state_code}"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                },
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=f" WHERE state_code = '{self.mock_state_code}'",
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table_{self.mock_state_code}",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/{self.mock_state_code}"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
            ),
        ]

        self.assertEqual(expected_view_config_list, export_configs_for_filter)

        # Test for case insensitivity

        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name.lower(),
            state_code=self.mock_state_code.lower(),
        )
        self.assertEqual(expected_view_config_list, export_configs_for_filter)

    def test_get_configs_for_export_name_state_agnostic(self) -> None:
        """Tests get_configs_for_export_name function to ensure that export names correctly match"""
        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name
        )
        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        expected_view_config_list = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=None,
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                },
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=None,
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
            ),
        ]

        self.assertEqual(expected_view_config_list, export_configs_for_filter)

        # Test for case insensitivity

        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name.lower()
        )

        self.assertEqual(expected_view_config_list, export_configs_for_filter)

    @mock.patch(
        "recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter"
    )
    def test_export_dashboard_data_to_cloud_storage(
        self, mock_view_exporter: Mock
    ) -> None:
        """Tests the table is created from the view and then extracted."""
        view_export_manager.export_view_data_to_cloud_storage(
            export_job_name=self.mock_export_name,
            state_code=self.mock_state_code,
            override_view_exporter=mock_view_exporter,
            gcs_output_sandbox_subdir=None,
            view_sandbox_context=None,
        )

        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        expected_view_config_list_1 = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                },
            )
        ]

        expected_view_config_list_2 = [
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
            )
        ]
        mock_view_exporter.export_and_validate.assert_has_calls(
            [
                mock.call([]),  # CSV export
                mock.call([]),
                mock.call(
                    [
                        expected_view_config_list_1[
                            0
                        ].pointed_to_staging_subdirectory(),
                        expected_view_config_list_2[
                            0
                        ].pointed_to_staging_subdirectory(),
                    ]
                ),  # JSON exports
                mock.call(
                    [expected_view_config_list_2[0].pointed_to_staging_subdirectory()]
                ),  # METRIC export ("OTHER_EXPORT")
            ],
            any_order=True,
        )

    @mock.patch(
        "recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter"
    )
    def test_raise_exception_no_export_matched(self, mock_view_exporter: Mock) -> None:
        # pylint: disable=unused-argument
        """Tests the table is created from the view and then extracted."""
        self.mock_export_config.NAMESPACE_TO_UPDATE_FOR_EXPORT_FILTER = {
            "US_YY": "NAMESPACE"
        }

        with self.assertRaisesRegex(
            ValueError, r"^No export configs matching export name: \[JOBZZZ\]$"
        ):
            view_export_manager.export_view_data_to_cloud_storage(
                export_job_name="JOBZZZ",
                override_view_exporter=mock_view_exporter,
                state_code=None,
                gcs_output_sandbox_subdir=None,
                view_sandbox_context=None,
            )

    @mock.patch(
        "recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter"
    )
    def test_export_dashboard_data_to_cloud_storage_state_agnostic(
        self, mock_view_exporter: Mock
    ) -> None:
        """Tests the table is created from the view and then extracted, where the export is not state-specific."""
        state_agnostic_dataset_export_configs = {
            self.mock_export_name: ExportViewCollectionConfig(
                view_builders_to_export=self.view_builders_for_dataset,
                output_directory_uri_template="gs://{project_id}-bucket-without-state-codes",
                export_name=self.mock_export_name,
            ),
        }

        self.mock_export_config.VIEW_COLLECTION_EXPORT_INDEX = (
            state_agnostic_dataset_export_configs
        )

        view_export_manager.export_view_data_to_cloud_storage(
            export_job_name=self.mock_export_name,
            override_view_exporter=mock_view_exporter,
            state_code=None,
            gcs_output_sandbox_subdir=None,
            view_sandbox_context=None,
        )

        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        view_export_configs: list[ExportBigQueryViewConfig] = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=None,
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket-without-state-codes"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                },
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=None,
                intermediate_table_name=f"{self.mock_export_name}_{view.dataset_id}_{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket-without-state-codes"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                    ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
                },
            ),
        ]

        mock_view_exporter.export_and_validate.assert_has_calls(
            [
                mock.call([]),  # CSV export
                mock.call(
                    [view_export_configs[1].pointed_to_staging_subdirectory()]
                ),  # JSON export
                mock.call(
                    [
                        conf.pointed_to_staging_subdirectory()
                        for conf in view_export_configs
                    ]
                ),  # METRIC export
            ],
            any_order=True,
        )

    @mock.patch(
        "recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter"
    )
    def test_export_dashboard_data_to_cloud_storage_value_error(
        self, mock_view_exporter: Mock
    ) -> None:
        """Tests the table is created from the view and then extracted."""

        mock_view_exporter.export_and_validate.side_effect = ValueError
        with self.assertRaises(ValueError):
            view_export_manager.export_view_data_to_cloud_storage(
                export_job_name=self.mock_export_name,
                override_view_exporter=mock_view_exporter,
                state_code=None,
                gcs_output_sandbox_subdir=None,
                view_sandbox_context=None,
            )

    @mock.patch(
        "recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter"
    )
    def test_export_dashboard_data_to_cloud_storage_validation_error(
        self, mock_view_exporter: Mock
    ) -> None:
        """Tests the table is created from the view and then extracted."""

        mock_view_exporter.export_and_validate.side_effect = ViewExportValidationError

        with self.assertRaises(ViewExportValidationError):
            view_export_manager.export_view_data_to_cloud_storage(
                export_job_name=self.mock_export_name,
                override_view_exporter=mock_view_exporter,
                state_code=None,
                gcs_output_sandbox_subdir=None,
                view_sandbox_context=None,
            )

    @mock.patch(
        "recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter"
    )
    def test_export_dashboard_data_to_cloud_storage_update_materialized_views_only(
        self,
        mock_view_exporter: Mock,
    ) -> None:
        """Tests that only materialized views are updated before the export."""
        view_export_manager.export_view_data_to_cloud_storage(
            export_job_name=self.mock_export_name,
            override_view_exporter=mock_view_exporter,
            state_code=None,
            gcs_output_sandbox_subdir=None,
            view_sandbox_context=None,
        )

    def test_invalid_export_configuration(self) -> None:
        """Tests that an invalid export configuration throws"""

        # These need to be created manually here because our ExportViewCollectionConfig framework
        # actually gives no ability to create configurations with different export types per config
        # except by not setting export_output_formats_and_validations at all, which sets it to a
        # correctly-configured default.
        view_export_configs: Sequence[ExportBigQueryViewConfig] = [
            ExportBigQueryViewConfig(
                view=self.mock_view_builder.build(),
                intermediate_table_name=f"{self.mock_view_builder.view_id}_intermediate",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.CSV: [ExportValidationType.EXISTS],
                },
            ),
            ExportBigQueryViewConfig(
                view=self.mock_metric_view_builder.build(),
                intermediate_table_name=f"{self.mock_metric_view_builder.view_id}_intermediate",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory"
                ),
                export_output_formats_and_validations={
                    ExportOutputFormatType.CSV: [
                        ExportValidationType.NON_EMPTY_COLUMNS
                    ],
                },
            ),
        ]

        with self.assertRaises(ViewExportConfigurationError):
            view_export_manager.do_metric_export_for_configs(
                export_name="EXPORT",
                view_export_configs=view_export_configs,
                state_code_filter=None,
            )

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_execute_metric_view_data_export(
        self, mock_export_view_data_to_cloud_storage: Mock
    ) -> None:
        mock_export_view_data_to_cloud_storage.return_value = None
        execute_metric_view_data_export(
            export_job_name="EXPORT",
            state_code=StateCode.US_WW,
        )
        mock_export_view_data_to_cloud_storage.assert_called()
        execute_metric_view_data_export(
            export_job_name="export",
            state_code=StateCode.US_WW,
        )
        mock_export_view_data_to_cloud_storage.assert_called()

    @mock.patch("recidiviz.big_query.view_update_manager.BigQueryClientImpl")
    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_execute_metric_view_data_export_persists_success(
        self, mock_export_view_data_to_cloud_storage: Mock, _mock_bq_client: MagicMock
    ) -> None:
        export_job_name = "export"
        state_code = StateCode.US_WW
        execute_metric_view_data_export(
            export_job_name=export_job_name,
            state_code=state_code,
        )
        mock_export_view_data_to_cloud_storage.assert_called()
        self.mock_metric_view_data_export_success_persister.record_success_in_bq.assert_called_with(
            export_job_name=export_job_name,
            state_code=state_code.value,
            sandbox_dataset_prefix=None,
            runtime_sec=mock.ANY,
        )

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_execute_metric_view_data_export_not_launched_in_env(
        self,
        mock_export_view_data_to_cloud_storage: Mock,
    ) -> None:
        mock_export_view_data_to_cloud_storage.return_value = None
        self.mock_project_id_fn.return_value = GCP_PROJECT_PRODUCTION
        execute_metric_view_data_export(
            export_job_name="EXPORT",
            state_code=StateCode.US_WW,
        )
        mock_export_view_data_to_cloud_storage.assert_not_called()

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_execute_metric_view_data_export_state_agnostic(
        self, mock_export_view_data_to_cloud_storage: Mock
    ) -> None:
        mock_export_view_data_to_cloud_storage.return_value = None
        execute_metric_view_data_export(
            export_job_name="MOCK_EXPORT_NAME",
            state_code=StateCode.US_WW,
        )
        mock_export_view_data_to_cloud_storage.assert_called()

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.pubsub_helper.publish_message_to_topic"
    )
    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_execute_metric_view_data_export_publishes_message(
        self, mock_export_view_data_to_cloud_storage: Mock, mock_publish_message: Mock
    ) -> None:
        """Tests that Pub/Sub is called when the export should publish a message"""
        mock_export_view_data_to_cloud_storage.return_value = None

        export_configs_to_publish = {
            self.mock_export_name: ExportViewCollectionConfig(
                view_builders_to_export=self.view_builders_for_dataset,
                output_directory_uri_template="gs://{project_id}-bucket",
                export_name=self.mock_export_name,
                publish_success_pubsub_message=True,
            ),
        }

        self.mock_export_config.VIEW_COLLECTION_EXPORT_INDEX = export_configs_to_publish
        execute_metric_view_data_export(
            export_job_name="MOCK_EXPORT_NAME",
            state_code=StateCode.US_WW,
        )

        mock_publish_message.assert_called_once_with(
            destination_project_id=self.mock_project_id,
            topic=f"{self.mock_export_name.lower()}_export_success",
            message=json.dumps({"state_code": "US_WW"}),
        )

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.pubsub_helper.publish_message_to_topic"
    )
    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_execute_metric_view_data_export_publishes_message_to_nondata_project(
        self, mock_export_view_data_to_cloud_storage: Mock, mock_publish_message: Mock
    ) -> None:
        """Tests that Pub/Sub is called when the export should publish a message"""
        mock_export_view_data_to_cloud_storage.return_value = None

        export_configs_to_publish = {
            self.mock_export_name: ExportViewCollectionConfig(
                view_builders_to_export=self.view_builders_for_dataset,
                output_directory_uri_template="gs://{project_id}-bucket",
                export_name=self.mock_export_name,
                publish_success_pubsub_message=True,
                output_project_by_data_project={
                    self.mock_project_id: "recidiviz-frontend"
                },
            ),
        }

        self.mock_export_config.VIEW_COLLECTION_EXPORT_INDEX = export_configs_to_publish
        execute_metric_view_data_export(
            export_job_name="MOCK_EXPORT_NAME",
            state_code=StateCode.US_WW,
        )

        mock_publish_message.assert_called_once_with(
            destination_project_id="recidiviz-frontend",
            topic=f"{self.mock_export_name.lower()}_export_success",
            message=json.dumps({"state_code": "US_WW"}),
        )

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.pubsub_helper.publish_message_to_topic"
    )
    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_execute_metric_view_data_export_publishes_message_with_override_state(
        self, mock_export_view_data_to_cloud_storage: Mock, mock_publish_message: Mock
    ) -> None:
        """Tests that Pub/Sub is called when the export should publish a message"""
        mock_export_view_data_to_cloud_storage.return_value = None

        export_configs_to_publish = {
            self.mock_export_name: ExportViewCollectionConfig(
                view_builders_to_export=self.view_builders_for_dataset,
                output_directory_uri_template="gs://{project_id}-bucket",
                export_name=self.mock_export_name,
                publish_success_pubsub_message=True,
                output_project_by_data_project={
                    self.mock_project_id: "recidiviz-frontend"
                },
                export_override_state_codes={
                    StateCode.US_WW.value: StateCode.US_XX.value
                },
            ),
        }

        self.mock_export_config.VIEW_COLLECTION_EXPORT_INDEX = export_configs_to_publish
        execute_metric_view_data_export(
            export_job_name="MOCK_EXPORT_NAME",
            state_code=StateCode.US_WW,
        )

        mock_publish_message.assert_called_once_with(
            destination_project_id="recidiviz-frontend",
            topic=f"{self.mock_export_name.lower()}_export_success",
            message=json.dumps({"state_code": "US_XX"}),
        )


class TestMetricViewDataExportSuccessPersister(unittest.TestCase):
    def test_persist(self) -> None:
        mock_client = MagicMock()
        with local_project_id_override(GCP_PROJECT_STAGING):
            persister = MetricViewDataExportSuccessPersister(bq_client=mock_client)

            # Just shouldn't crash
            persister.record_success_in_bq(
                export_job_name="MY_EXPORT",
                runtime_sec=100,
                state_code=StateCode.US_XX.value,
                sandbox_dataset_prefix=None,
            )
