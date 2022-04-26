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
from unittest.mock import Mock

from google.cloud import bigquery
from mock import create_autospec, patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.raw_data import (
    direct_ingest_raw_data_table_latest_view_updater as latest_view_updater_module,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_data_table_latest_view_updater import (
    DirectIngestRawDataTableLatestViewUpdater,
)
from recidiviz.ingest.direct.views import direct_ingest_latest_view_collector
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewBuilder,
)
from recidiviz.tests.ingest.direct.fakes.fake_direct_ingest_controller import (
    FakeDirectIngestRegionRawFileConfig,
)
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


class DirectIngestRawDataUpdateControllerTest(unittest.TestCase):
    """Tests for DirectIngestRawDataUpdateController."""

    def setUp(self) -> None:
        self.test_region = fake_region(
            region_code="us_xx",
        )

        self.project_id = "recidiviz-456"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        self.mock_big_query_client = create_autospec(BigQueryClient)
        self.client_patcher = mock.patch(
            "recidiviz.big_query.big_query_table_checker.BigQueryClientImpl"
        )
        self.client_fn = self.client_patcher.start()
        self.client_fn.return_value = self.mock_big_query_client

        def table_exists_side_effect(
            # pylint: disable=unused-argument
            dataset_ref: bigquery.DatasetReference,
            table_id: str,
        ) -> bool:
            return table_id != "tagB"

        self.mock_big_query_client.table_exists.side_effect = table_exists_side_effect

        self.view_update_manager_patcher = patch(
            "recidiviz.big_query.view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders"
        )
        self.mock_view_update_manager = self.view_update_manager_patcher.start()

        self.mock_region_config = FakeDirectIngestRegionRawFileConfig(
            self.test_region.region_code
        )
        self.region_file_config_patcher = patch(
            f"{direct_ingest_latest_view_collector.__name__}.DirectIngestRegionRawFileConfig",
            return_value=self.mock_region_config,
        )
        self.region_file_config_patcher.start()

    def tearDown(self) -> None:
        self.region_file_config_patcher.stop()
        self.project_id_patcher.stop()
        self.client_patcher.stop()
        self.view_update_manager_patcher.stop()

    @mock.patch(
        f"{latest_view_updater_module.__name__}.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    def test_update_tables_for_state(self, mock_view_exporter: Mock) -> None:
        self.mock_raw_file_configs = self.mock_region_config.raw_file_configs

        self.update_controller = DirectIngestRawDataTableLatestViewUpdater(
            state_code=self.test_region.region_code,
            bq_client=self.mock_big_query_client,
            views_sandbox_dataset_prefix=None,
            raw_tables_sandbox_dataset_prefix=None,
        )

        self.update_controller.update_views_for_state()

        mock_view_exporter.assert_called_with(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            # These are verified below
            view_builders_to_update=mock.ANY,
            historically_managed_datasets_to_clean=None,
            address_overrides=None,
        )
        self._assert_has_valid_view_builders(
            mock_view_exporter,
            expected_input_dataset="us_xx_raw_data",
            expected_output_dataset="us_xx_raw_data_up_to_date_views",
        )

    @mock.patch(
        f"{latest_view_updater_module.__name__}.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    def test_update_tables_for_state_with_sandbox_views_dataset(
        self, mock_view_exporter: Mock
    ) -> None:
        self.mock_raw_file_configs = self.mock_region_config.raw_file_configs

        self.update_controller = DirectIngestRawDataTableLatestViewUpdater(
            state_code=self.test_region.region_code,
            bq_client=self.mock_big_query_client,
            views_sandbox_dataset_prefix="my_prefix",
            raw_tables_sandbox_dataset_prefix=None,
        )

        self.update_controller.update_views_for_state()
        mock_view_exporter.assert_called_with(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            # These are verified below
            view_builders_to_update=mock.ANY,
            historically_managed_datasets_to_clean=None,
            # These are verified below
            address_overrides=mock.ANY,
        )
        self._assert_has_valid_view_builders(
            mock_view_exporter,
            expected_input_dataset="us_xx_raw_data",
            expected_output_dataset="my_prefix_us_xx_raw_data_up_to_date_views",
        )
        self._assert_has_address_overrides(mock_view_exporter)

    @mock.patch(
        f"{latest_view_updater_module.__name__}.create_managed_dataset_and_deploy_views_for_view_builders"
    )
    def test_update_tables_for_state_with_sandbox_views_and_raw_datasets(
        self, mock_view_exporter: Mock
    ) -> None:
        self.mock_raw_file_configs = self.mock_region_config.raw_file_configs

        self.update_controller = DirectIngestRawDataTableLatestViewUpdater(
            state_code=self.test_region.region_code,
            bq_client=self.mock_big_query_client,
            views_sandbox_dataset_prefix="my_prefix",
            raw_tables_sandbox_dataset_prefix="my_other_prefix",
        )

        self.update_controller.update_views_for_state()

        mock_view_exporter.assert_called_with(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            # These are verified below
            view_builders_to_update=mock.ANY,
            historically_managed_datasets_to_clean=None,
            # These are verified below
            address_overrides=mock.ANY,
        )

        self._assert_has_valid_view_builders(
            mock_view_exporter,
            expected_input_dataset="my_other_prefix_us_xx_raw_data",
            expected_output_dataset="my_prefix_us_xx_raw_data_up_to_date_views",
        )
        self._assert_has_address_overrides(mock_view_exporter)

    def _assert_has_address_overrides(self, mock_view_exporter: Mock) -> None:
        actual_address_overrides = mock_view_exporter.mock_calls[0].kwargs[
            "address_overrides"
        ]
        if not isinstance(actual_address_overrides, BigQueryAddressOverrides):
            raise ValueError(
                f"Found bad type for address overrides {actual_address_overrides}"
            )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_us_xx_raw_data_up_to_date_views", table_id="tagA"
            ),
            actual_address_overrides.get_sandbox_address(
                BigQueryAddress(
                    dataset_id="us_xx_raw_data_up_to_date_views", table_id="tagA"
                )
            ),
        )

    def _assert_has_valid_view_builders(
        self,
        mock_view_exporter: Mock,
        *,
        expected_input_dataset: str,
        expected_output_dataset: str,
    ) -> None:
        """Checks the view builders that were generated and confirms that they are what
        we expect.
        """
        actual_address_overrides = mock_view_exporter.mock_calls[0].kwargs[
            "address_overrides"
        ]
        if actual_address_overrides and not isinstance(
            actual_address_overrides, BigQueryAddressOverrides
        ):
            raise ValueError(
                f"Found bad type for address overrides {actual_address_overrides}"
            )
        view_builders_to_update = mock_view_exporter.mock_calls[0].kwargs[
            "view_builders_to_update"
        ]
        found_view_ids = []
        for view_builder in view_builders_to_update:
            if not isinstance(view_builder, DirectIngestRawDataTableLatestViewBuilder):
                raise ValueError(f"Found bad type for view builder: {view_builder}")
            view = view_builder.build(address_overrides=actual_address_overrides)
            found_view_ids.append(view.view_id)
            if view_builder.view_id in ("tagB_latest", "tagWeDoNotIngest_latest"):
                self.assertFalse(view.should_deploy())
            else:
                self.assertTrue(view.should_deploy(), view_builder.view_id)
                tag = view.raw_file_config.file_tag
                self.assertEqual(
                    {BigQueryAddress(dataset_id=expected_input_dataset, table_id=tag)},
                    view.parent_tables,
                )
                self.assertEqual(expected_output_dataset, view.dataset_id)

        self.assertEqual(
            [
                "tagFullyEmptyFile_latest",
                "tagHeadersNoContents_latest",
                "tagBasicData_latest",
                "tagMoreBasicData_latest",
                "tagWeDoNotIngest_latest",
            ],
            found_view_ids,
        )
