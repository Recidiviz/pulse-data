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
"""Tests for DirectIngestRawDataUpdateController."""
import unittest
from unittest import mock

from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference
from mock import create_autospec, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.controllers import (
    direct_ingest_raw_data_table_latest_view_updater as latest_view_updater_module,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_data_table_latest_view_updater import (
    DirectIngestRawDataTableLatestViewUpdater,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestRawDataTableLatestView,
)
from recidiviz.tests.ingest.direct.direct_ingest_util import (
    FakeDirectIngestRegionRawFileConfig,
)
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.metadata import local_project_id_override


class DirectIngestRawDataUpdateControllerTest(unittest.TestCase):
    """Tests for DirectIngestRawDataUpdateController."""

    def setUp(self) -> None:
        self.test_region = fake_region(
            region_code="us_xx",
        )

        self.project_id = "recidiviz-456"
        self.mock_big_query_client = create_autospec(BigQueryClient)

        def fake_get_dataset_ref(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference(
                project=self.project_id, dataset_id=dataset_id
            )

        self.mock_big_query_client.dataset_ref_for_id = fake_get_dataset_ref
        self.mock_big_query_client.table_exists.side_effect = [
            True,  # tagA
            False,  # tagB
            True,  # tagC
            True,  # tagWeDoNotIngest
        ]

        self.mock_region_config = FakeDirectIngestRegionRawFileConfig(
            self.test_region.region_code
        )
        self.region_file_config_patcher = patch(
            f"{latest_view_updater_module.__name__}.DirectIngestRegionRawFileConfig",
            return_value=self.mock_region_config,
        )
        self.region_file_config_patcher.start()

    def tearDown(self) -> None:
        self.region_file_config_patcher.stop()

    def test_update_tables_for_state(self) -> None:
        self.mock_raw_file_configs = self.mock_region_config.raw_file_configs

        self.update_controller = DirectIngestRawDataTableLatestViewUpdater(
            state_code=self.test_region.region_code,
            project_id=self.project_id,
            bq_client=self.mock_big_query_client,
            sandbox_dataset_prefix=None,
        )

        with local_project_id_override(self.project_id):
            self.update_controller.update_views_for_state()

            self.assertEqual(
                self.mock_big_query_client.create_or_update_view.call_count, 2
            )

            raw_data_dataset = DatasetReference(self.project_id, "us_xx_raw_data")
            self.mock_big_query_client.table_exists.assert_has_calls(
                [
                    mock.call(raw_data_dataset, "tagA"),
                    mock.call(raw_data_dataset, "tagB"),
                    mock.call(raw_data_dataset, "tagC"),
                    mock.call(raw_data_dataset, "tagWeDoNotIngest"),
                ]
            )

            expected_views = [
                DirectIngestRawDataTableLatestView(
                    region_code=self.test_region.region_code,
                    raw_file_config=self.mock_raw_file_configs["tagA"],
                    dataset_overrides=None,
                ),
                DirectIngestRawDataTableLatestView(
                    region_code=self.test_region.region_code,
                    raw_file_config=self.mock_raw_file_configs["tagC"],
                    dataset_overrides=None,
                ),
            ]
            views_dataset = DatasetReference(
                self.project_id, "us_xx_raw_data_up_to_date_views"
            )

            self.mock_big_query_client.create_or_update_view.assert_has_calls(
                [mock.call(x) for x in expected_views]
            )

            self.mock_big_query_client.create_dataset_if_necessary.assert_called_once()
            self.mock_big_query_client.create_dataset_if_necessary.assert_has_calls(
                [mock.call(views_dataset, default_table_expiration_ms=None)]
            )

    def test_update_tables_for_state_with_prefix(self) -> None:
        self.mock_raw_file_configs = self.mock_region_config.raw_file_configs

        self.update_controller = DirectIngestRawDataTableLatestViewUpdater(
            state_code=self.test_region.region_code,
            project_id=self.project_id,
            bq_client=self.mock_big_query_client,
            sandbox_dataset_prefix="my_prefix",
        )

        with local_project_id_override(self.project_id):
            self.update_controller.update_views_for_state()

            self.assertEqual(
                self.mock_big_query_client.create_or_update_view.call_count, 2
            )

            raw_data_dataset = DatasetReference(self.project_id, "us_xx_raw_data")
            self.mock_big_query_client.table_exists.assert_has_calls(
                [
                    mock.call(raw_data_dataset, "tagA"),
                    mock.call(raw_data_dataset, "tagB"),
                    mock.call(raw_data_dataset, "tagC"),
                    mock.call(raw_data_dataset, "tagWeDoNotIngest"),
                ]
            )

            expected_dataset_overrides = {
                "us_xx_raw_data_up_to_date_views": "my_prefix_us_xx_raw_data_up_to_date_views"
            }

            expected_views = [
                DirectIngestRawDataTableLatestView(
                    region_code=self.test_region.region_code,
                    raw_file_config=self.mock_raw_file_configs["tagA"],
                    dataset_overrides=expected_dataset_overrides,
                ),
                DirectIngestRawDataTableLatestView(
                    region_code=self.test_region.region_code,
                    raw_file_config=self.mock_raw_file_configs["tagC"],
                    dataset_overrides=expected_dataset_overrides,
                ),
            ]

            self.mock_big_query_client.create_or_update_view.assert_has_calls(
                [mock.call(x) for x in expected_views]
            )

            expected_views_dataset = DatasetReference(
                self.project_id, "my_prefix_us_xx_raw_data_up_to_date_views"
            )

            self.mock_big_query_client.create_dataset_if_necessary.assert_called_once()
            self.mock_big_query_client.create_dataset_if_necessary.assert_has_calls(
                [
                    mock.call(
                        expected_views_dataset, default_table_expiration_ms=86400000
                    )
                ]
            )

    def test_failed_view_update(self) -> None:
        self.mock_raw_file_configs = self.mock_region_config.raw_file_configs

        self.update_controller = DirectIngestRawDataTableLatestViewUpdater(
            state_code=self.test_region.region_code,
            project_id=self.project_id,
            bq_client=self.mock_big_query_client,
            sandbox_dataset_prefix=None,
        )

        self.mock_big_query_client.create_or_update_view.side_effect = Exception

        with local_project_id_override(self.project_id):
            with self.assertRaisesRegex(
                ValueError, r"^Couldn't create/update views for file \[tagA\]$"
            ):
                self.update_controller.update_views_for_state()
