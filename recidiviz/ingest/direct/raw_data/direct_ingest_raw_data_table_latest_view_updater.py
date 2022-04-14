# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Controller for updating raw state data latest views in BQ."""

from typing import Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.view_update_manager import (
    create_managed_dataset_and_deploy_views_for_view_builders,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    DirectIngestRawDataTableLatestViewCollector,
)
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS


class DirectIngestRawDataTableLatestViewUpdater:
    """Controller for updating raw state data latest views in BQ."""

    def __init__(
        self,
        state_code: str,
        bq_client: BigQueryClient,
        raw_tables_sandbox_dataset_prefix: Optional[str],
        # Sandbox prefix to write raw data views to
        views_sandbox_dataset_prefix: Optional[str],
    ):
        self.state_code = state_code
        self.bq_client = bq_client
        # Sandbox prefix for dataset to read raw data files from
        self.raw_tables_sandbox_dataset_prefix = raw_tables_sandbox_dataset_prefix
        self.views_sandbox_dataset_prefix = views_sandbox_dataset_prefix

    def update_views_for_state(self) -> None:
        """Create or update the up to date views dataset for a state with latest views"""
        latest_view_builders_to_update = DirectIngestRawDataTableLatestViewCollector(
            region_code=self.state_code,
            src_raw_tables_sandbox_dataset_prefix=self.raw_tables_sandbox_dataset_prefix,
        ).collect_view_builders()

        address_overrides: Optional[BigQueryAddressOverrides] = None
        if self.views_sandbox_dataset_prefix:
            address_overrides_builder = BigQueryAddressOverrides.Builder(
                sandbox_prefix=self.views_sandbox_dataset_prefix
            )
            address_overrides_builder.register_sandbox_override_for_entire_dataset(
                dataset_id=raw_latest_views_dataset_for_region(self.state_code)
            )
            address_overrides = address_overrides_builder.build()

        if self.raw_tables_sandbox_dataset_prefix:
            if address_overrides:
                address_overrides_builder = address_overrides.to_builder(
                    sandbox_prefix=self.raw_tables_sandbox_dataset_prefix
                )
            else:
                address_overrides_builder = BigQueryAddressOverrides.Builder(
                    sandbox_prefix=self.raw_tables_sandbox_dataset_prefix
                )
            address_overrides_builder.register_sandbox_override_for_entire_dataset(
                dataset_id=raw_tables_dataset_for_region(self.state_code)
            )
            address_overrides = address_overrides_builder.build()

        create_managed_dataset_and_deploy_views_for_view_builders(
            view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
            view_builders_to_update=latest_view_builders_to_update,
            # Don't clean up datasets when just updating latest views
            historically_managed_datasets_to_clean=None,
            address_overrides=address_overrides,
        )
