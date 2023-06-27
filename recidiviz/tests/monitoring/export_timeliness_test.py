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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.metrics.export.export_config import ExportViewCollectionConfig
from recidiviz.metrics.export.products.product_configs import (
    ProductConfig,
    ProductConfigs,
    ProductStateConfig,
)
from recidiviz.monitoring.export_timeliness import (
    MISSING_FILE_CREATION_TIMESTAMP,
    UTC_EPOCH,
    build_blob_recent_reading_query,
    generate_expected_file_uris,
    produce_export_timeliness_metrics,
    seconds_since_epoch,
)

product_configs_fixture = ProductConfigs(
    products=[
        ProductConfig(
            name="test_product",
            description="Test Product",
            exports=[
                "TEST_EXPORT",
            ],
            environment=None,
            states=[ProductStateConfig(state_code="US_XX", environment="prod")],
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

        self.recent_blob_readings_by_query = {
            build_blob_recent_reading_query(blob_uris["test_view"]): [1],
            build_blob_recent_reading_query(blob_uris["other_test_view"]): [1],
            build_blob_recent_reading_query(blob_uris["newly_added_export"]): [],
        }
        self.client_patcher = patch("google.cloud.monitoring_v3.QueryServiceClient")
        self.client_patcher.start().return_value.query_time_series = (
            lambda request: self.recent_blob_readings_by_query[request.query]
        )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.client_patcher.stop()

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

    def test_produce_export_timeliness_metrics(self) -> None:
        results = produce_export_timeliness_metrics(
            set(blob_uris.values()),
            [mock_test_view_blob],
        )

        self.assertCountEqual(
            results,
            [
                (blob_uris["test_view"], 86400),
                (blob_uris["other_test_view"], MISSING_FILE_CREATION_TIMESTAMP),
            ],
        )
