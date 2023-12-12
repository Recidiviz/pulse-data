# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Test export timeliness functionality."""

import unittest
from datetime import datetime, timezone
from unittest.mock import PropertyMock, patch

import mock
from opentelemetry.metrics import Observation

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.metrics.export.export_config import (
    VIEW_COLLECTION_EXPORT_INDEX,
    ExportViewCollectionConfig,
)
from recidiviz.metrics.export.products.product_configs import (
    ProductConfig,
    ProductConfigs,
)
from recidiviz.monitoring.export_timeliness import (
    MISSING_FILE_CREATION_TIMESTAMP,
    UTC_EPOCH,
    build_blob_recent_reading_query,
    generate_expected_file_uris,
    get_export_timeliness_metrics,
    seconds_since_epoch,
)
from recidiviz.monitoring.keys import AttributeKey
from recidiviz.tests.utils.monitoring_test_utils import OTLMock

product_configs_fixture = ProductConfigs(
    products=[
        ProductConfig(
            name="test_product",
            description="Test Product",
            exports=[
                "TEST_EXPORT",
            ],
            environment=None,
            is_state_agnostic=True,
            states=None,
        )
    ]
)

TEST_EXPORT_CONFIG = ExportViewCollectionConfig(
    export_name="TEST_EXPORT",
    output_directory_uri_template="{project_id}-test-export",
    view_builders_to_export=[
        SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="test_view",
            description="test view",
            view_query_template="select 1",
        ),
        SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="other_test_view",
            description="other test view",
            view_query_template="select 2",
        ),
        SimpleBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="newly_added_test_view",
            description="other test view",
            view_query_template="select 2",
        ),
    ],
)


def build_mock_obj(attrs: dict) -> mock.Mock:
    mock_blob = mock.Mock()

    for key, value in attrs.items():
        setattr(type(mock_blob), key, PropertyMock(return_value=value))

    return mock_blob


mock_test_view_blob = build_mock_obj(
    {
        "bucket": build_mock_obj({"name": "test-project-test-export"}),
        "name": "test_view.json",
        "generation": "123",
        "time_created": datetime(1970, 1, 2, tzinfo=timezone.utc),
        "id": "test-project-test-export/test_view.json#123",
    }
)

blob_uris = {
    "test_view": "gs://test-project-test-export/test_view.json",
    "other_test_view": "gs://test-project-test-export/other_test_view.json",
    "newly_added_export": "gs://test-project-test-export/newly_added_test_view.json",
}


class TestExportTimeliness(unittest.TestCase):
    """Tests for export timeliness"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "test-project"

        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

        self.recent_blob_readings_by_query = {
            build_blob_recent_reading_query(blob_uris["test_view"]): [1],
            build_blob_recent_reading_query(blob_uris["other_test_view"]): [1],
            build_blob_recent_reading_query(blob_uris["newly_added_export"]): [],
        }
        self.client_patcher = patch("google.cloud.monitoring_v3.QueryServiceClient")
        self.client_patcher.start().return_value.query_time_series = (
            lambda request: self.recent_blob_readings_by_query[request.query]
        )

        self.gcs_patcher = patch("recidiviz.monitoring.export_timeliness.Client")
        gcs_client_mock = self.gcs_patcher.start()
        gcs_client_mock.return_value.list_blobs.return_value = [mock_test_view_blob]

        self.products_patcher = patch(
            "recidiviz.monitoring.export_timeliness.ProductConfigs.from_file"
        )
        self.products_patcher.start().return_value = product_configs_fixture

        self.view_collection_patcher = patch.dict(
            VIEW_COLLECTION_EXPORT_INDEX,
            {TEST_EXPORT_CONFIG.export_name: TEST_EXPORT_CONFIG},
        )
        self.view_collection_patcher.start()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.client_patcher.stop()
        self.gcs_patcher.stop()
        self.products_patcher.stop()
        self.view_collection_patcher.stop()
        self.otl_mock.tear_down()

    def test_generate_expected_file_uris(self) -> None:
        result = generate_expected_file_uris(TEST_EXPORT_CONFIG)

        self.assertEqual(
            result,
            {
                blob_uris["test_view"],
                blob_uris["other_test_view"],
                blob_uris["newly_added_export"],
            },
        )

    def test_seconds_since_epoch(self) -> None:
        self.assertEqual(seconds_since_epoch(UTC_EPOCH.replace(second=3)), 3)

    def test_get_export_timeliness_metrics(self) -> None:
        results = list(get_export_timeliness_metrics())

        self.assertCountEqual(
            results,
            [
                Observation(
                    value=86400,
                    attributes={
                        AttributeKey.METRIC_VIEW_EXPORT_NAME: TEST_EXPORT_CONFIG.export_name,
                        AttributeKey.EXPORT_FILE: blob_uris["test_view"],
                    },
                ),
                Observation(
                    value=MISSING_FILE_CREATION_TIMESTAMP,
                    attributes={
                        AttributeKey.METRIC_VIEW_EXPORT_NAME: TEST_EXPORT_CONFIG.export_name,
                        AttributeKey.EXPORT_FILE: blob_uris["other_test_view"],
                    },
                ),
            ],
        )
