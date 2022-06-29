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

import unittest
from http import HTTPStatus
from typing import Any, Dict
from unittest import mock
from unittest.mock import MagicMock, Mock

import flask
from flask import Flask
from google.cloud import bigquery

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.export.big_query_view_exporter import ViewExportValidationError
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.export import view_export_manager
from recidiviz.metrics.export.export_config import ExportViewCollectionConfig
from recidiviz.metrics.export.view_export_manager import export_blueprint
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.scraper_cloud_task_manager_test import (
    CLOUD_TASK_MANAGER_PACKAGE_NAME,
)
from recidiviz.utils.environment import GCPEnvironment


@mock.patch(
    "recidiviz.utils.metadata.project_id", MagicMock(return_value="test-project")
)
@mock.patch(
    "recidiviz.utils.metadata.project_number", MagicMock(return_value="123456789")
)
@mock.patch(
    "recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine",
    MagicMock(return_value=("test-user", "test-user@recidiviz.org", None)),
)
@mock.patch(
    f"{view_export_manager.__name__}.PRODUCTS_CONFIG_PATH",
    fixtures.as_filepath("fixture_products.yaml"),
)
class ViewCollectionExportManagerTest(unittest.TestCase):
    """Tests for view_export_manager.py."""

    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.register_blueprint(export_blueprint)
        self.app.config["TESTING"] = True
        self.headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}
        self.client = self.app.test_client()
        self.mock_cloud_task_client_patcher = mock.patch(
            "google.cloud.tasks_v2.CloudTasksClient"
        )
        self.mock_cloud_task_client_patcher.start()
        self.mock_uuid_patcher = mock.patch(f"{CLOUD_TASK_MANAGER_PACKAGE_NAME}.uuid")
        self.mock_uuid = self.mock_uuid_patcher.start()
        with self.app.test_request_context():
            self.metric_view_data_export_url = flask.url_for(
                "export.metric_view_data_export"
            )
            self.create_metric_view_data_export_tasks_url = flask.url_for(
                "export.create_metric_view_data_export_tasks"
            )
        self.mock_state_code = "US_XX"
        self.mock_project_id = "test-project"
        self.mock_dataset_id = "base_dataset"
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id
        )

        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.mock_project_id

        self.client_patcher = mock.patch(
            "recidiviz.metrics.export.view_export_manager.BigQueryClientImpl"
        )
        self.mock_client = self.client_patcher.start().return_value

        self.mock_client.dataset_ref_for_id.return_value = self.mock_dataset

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

        self.view_builders_for_dataset = [
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

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.export_config_patcher.stop()
        self.metadata_patcher.stop()
        self.gcs_factory_patcher.stop()
        self.mock_uuid_patcher.stop()
        self.mock_cloud_task_client_patcher.stop()

    @mock.patch("recidiviz.utils.environment.get_gcp_environment")
    def test_get_configs_for_export_name(
        self, mock_environment: mock.MagicMock
    ) -> None:
        """Tests get_configs_for_export_name function to ensure that export names correctly match"""

        mock_environment.return_value = GCPEnvironment.PRODUCTION.value
        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name,
            state_code=self.mock_state_code,
            project_id=self.mock_project_id,
        )
        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        expected_view_config_list = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=f" WHERE state_code = '{self.mock_state_code}'",
                intermediate_table_name=f"{view.view_id}_table_{self.mock_state_code}",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/{self.mock_state_code}"
                ),
                export_output_formats=[ExportOutputFormatType.JSON],
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=f" WHERE state_code = '{self.mock_state_code}'",
                intermediate_table_name=f"{view.view_id}_table_{self.mock_state_code}",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/{self.mock_state_code}"
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
            ),
        ]

        self.assertEqual(expected_view_config_list, export_configs_for_filter)

        # Test for case insensitivity

        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name.lower(),
            state_code=self.mock_state_code.lower(),
            project_id=self.mock_project_id,
        )
        self.assertEqual(expected_view_config_list, export_configs_for_filter)

    @mock.patch("recidiviz.utils.environment.get_gcp_environment")
    def test_get_configs_for_export_name_state_agnostic(
        self, mock_environment: mock.MagicMock
    ) -> None:
        """Tests get_configs_for_export_name function to ensure that export names correctly match"""

        mock_environment.return_value = GCPEnvironment.PRODUCTION.value
        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name, project_id=self.mock_project_id
        )
        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        expected_view_config_list = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=None,
                intermediate_table_name=f"{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory"
                ),
                export_output_formats=[ExportOutputFormatType.JSON],
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=None,
                intermediate_table_name=f"{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory"
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
            ),
        ]

        self.assertEqual(expected_view_config_list, export_configs_for_filter)

        # Test for case insensitivity

        export_configs_for_filter = view_export_manager.get_configs_for_export_name(
            export_name=self.mock_export_name.lower(), project_id=self.mock_project_id
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
            self.mock_export_name, self.mock_state_code, mock_view_exporter
        )

        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        expected_view_config_list_1 = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats=[ExportOutputFormatType.JSON],
            )
        ]

        expected_view_config_list_2 = [
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=" WHERE state_code = 'US_XX'",
                intermediate_table_name=f"{view.view_id}_table_US_XX",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-dataset-location/subdirectory/US_XX"
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
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
                export_job_name="JOBZZZ", override_view_exporter=mock_view_exporter
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
        )

        view = self.mock_view_builder.build()
        metric_view = self.mock_metric_view_builder.build()

        view_export_configs = [
            ExportBigQueryViewConfig(
                view=view,
                view_filter_clause=None,
                intermediate_table_name=f"{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket-without-state-codes"
                ),
                export_output_formats=[ExportOutputFormatType.JSON],
            ),
            ExportBigQueryViewConfig(
                view=metric_view,
                view_filter_clause=None,
                intermediate_table_name=f"{view.view_id}_table",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    f"gs://{self.mock_project_id}-bucket-without-state-codes"
                ),
                export_output_formats=[
                    ExportOutputFormatType.JSON,
                    ExportOutputFormatType.METRIC,
                ],
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
                self.mock_export_name, override_view_exporter=mock_view_exporter
            )

    @mock.patch(
        "recidiviz.big_query.export.big_query_view_exporter.BigQueryViewExporter"
    )
    def test_export_dashboard_data_to_cloud_storage_validation_error(
        self, mock_view_exporter: Mock
    ) -> None:
        """Tests the table is created from the view and then extracted."""

        mock_view_exporter.export_and_validate.side_effect = ViewExportValidationError

        # Should not throw
        view_export_manager.export_view_data_to_cloud_storage(
            self.mock_export_name, override_view_exporter=mock_view_exporter
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
            self.mock_export_name, override_view_exporter=mock_view_exporter
        )

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_metric_view_data_export_valid_request(
        self, mock_export_view_data_to_cloud_storage: Mock
    ) -> None:
        with self.app.test_request_context():
            mock_export_view_data_to_cloud_storage.return_value = None
            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=EXPORT&state_code=US_XX",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_export_view_data_to_cloud_storage.assert_called()

            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=export&state_code=us_xx",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_export_view_data_to_cloud_storage.assert_called()

    @mock.patch("recidiviz.utils.environment.get_gcp_environment")
    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_metric_view_data_export_not_launched_in_env(
        self,
        mock_export_view_data_to_cloud_storage: Mock,
        mock_get_gcp_environment: Mock,
    ) -> None:
        with self.app.test_request_context():
            mock_export_view_data_to_cloud_storage.return_value = None
            mock_get_gcp_environment.return_value = GCPEnvironment.PRODUCTION.value
            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=EXPORT&state_code=US_WW",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_export_view_data_to_cloud_storage.assert_not_called()

            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=export&state_code=us_ww",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_export_view_data_to_cloud_storage.assert_not_called()

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_metric_view_data_export_state_agnostic(
        self, mock_export_view_data_to_cloud_storage: Mock
    ) -> None:
        with self.app.test_request_context():
            mock_export_view_data_to_cloud_storage.return_value = None
            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=MOCK_EXPORT_NAME",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)

            # case insensitive
            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=mock_export_name",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_metric_view_data_export_missing_required_state_code(
        self, mock_export_view_data_to_cloud_storage: Mock
    ) -> None:
        with self.app.test_request_context():
            mock_export_view_data_to_cloud_storage.return_value = None
            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=EXPORT",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing required state_code parameter for export_job_name EXPORT",
                response.data,
            )

            # case insensitive
            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="export_job_name=export",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing required state_code parameter for export_job_name EXPORT",
                response.data,
            )

    @mock.patch(
        "recidiviz.metrics.export.view_export_manager.export_view_data_to_cloud_storage"
    )
    def test_metric_view_data_export_missing_export_job_name(
        self, mock_export_view_data_to_cloud_storage: Mock
    ) -> None:
        with self.app.test_request_context():
            mock_export_view_data_to_cloud_storage.return_value = None
            response = self.client.get(
                self.metric_view_data_export_url,
                headers=self.headers,
                query_string="state_code=US_XX",
            )
            self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
            self.assertEqual(
                b"Missing required export_job_name URL parameter", response.data
            )

    @mock.patch(
        "recidiviz.metrics.export.view_export_cloud_task_manager.ViewExportCloudTaskManager.create_metric_view_data_export_task"
    )
    def test_create_metric_view_data_export_tasks_state_code_filter(
        self, mock_create_metric_view_data_export_task: Mock
    ) -> None:
        with self.app.test_request_context():
            mock_create_metric_view_data_export_task.return_value = None
            response = self.client.get(
                self.create_metric_view_data_export_tasks_url,
                headers=self.headers,
                query_string="export_job_filter=US_XX",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_create_metric_view_data_export_task.assert_has_calls(
                [
                    mock.call(export_job_name="EXPORT", state_code="US_XX"),
                    mock.call(export_job_name="OTHER_EXPORT", state_code="US_XX"),
                ],
                any_order=True,
            )

            response = self.client.get(
                self.create_metric_view_data_export_tasks_url,
                headers=self.headers,
                query_string="export_job_filter=us_xx",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_create_metric_view_data_export_task.assert_has_calls(
                [
                    mock.call(export_job_name="EXPORT", state_code="US_XX"),
                    mock.call(export_job_name="OTHER_EXPORT", state_code="US_XX"),
                ],
                any_order=True,
            )

    @mock.patch(
        "recidiviz.metrics.export.view_export_cloud_task_manager.ViewExportCloudTaskManager.create_metric_view_data_export_task"
    )
    def test_create_metric_view_data_export_tasks_export_name_filter_state_agnostic(
        self, mock_create_metric_view_data_export_task: Mock
    ) -> None:
        with self.app.test_request_context():
            mock_create_metric_view_data_export_task.return_value = None
            response = self.client.get(
                self.create_metric_view_data_export_tasks_url,
                headers=self.headers,
                query_string="export_job_filter=MOCK_EXPORT_NAME",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_create_metric_view_data_export_task.assert_has_calls(
                [
                    mock.call(export_job_name="MOCK_EXPORT_NAME", state_code=None),
                ],
                any_order=True,
            )

            # case insensitive
            response = self.client.get(
                self.create_metric_view_data_export_tasks_url,
                headers=self.headers,
                query_string="export_job_filter=mock_export_name",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_create_metric_view_data_export_task.assert_has_calls(
                [
                    mock.call(export_job_name="MOCK_EXPORT_NAME", state_code=None),
                ],
                any_order=True,
            )

    @mock.patch(
        "recidiviz.metrics.export.view_export_cloud_task_manager.ViewExportCloudTaskManager.create_metric_view_data_export_task"
    )
    def test_create_metric_view_data_export_tasks_export_name_filter(
        self, mock_create_metric_view_data_export_task: Mock
    ) -> None:
        with self.app.test_request_context():
            mock_create_metric_view_data_export_task.return_value = None
            response = self.client.get(
                self.create_metric_view_data_export_tasks_url,
                headers=self.headers,
                query_string="export_job_filter=EXPORT",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_create_metric_view_data_export_task.assert_has_calls(
                [
                    mock.call(export_job_name="EXPORT", state_code="US_XX"),
                    mock.call(export_job_name="EXPORT", state_code="US_WW"),
                ],
                any_order=True,
            )

            # case insensitive
            response = self.client.get(
                self.create_metric_view_data_export_tasks_url,
                headers=self.headers,
                query_string="export_job_filter=export",
            )
            self.assertEqual(HTTPStatus.OK, response.status_code)
            mock_create_metric_view_data_export_task.assert_has_calls(
                [
                    mock.call(export_job_name="EXPORT", state_code="US_XX"),
                    mock.call(export_job_name="EXPORT", state_code="US_WW"),
                ],
                any_order=True,
            )
