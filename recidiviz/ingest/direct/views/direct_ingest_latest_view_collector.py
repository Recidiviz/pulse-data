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
"""Collector and Builder for the DirectIngestRawDataTableLatestView class"""
from typing import List, Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.raw_table_query_builder import RawTableQueryBuilder
from recidiviz.utils import metadata


class DirectIngestRawDataTableLatestViewBuilder(BigQueryViewBuilder):
    """Factory class for building DirectIngestRawDataTableLatestView"""

    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        region_code: str,
        raw_data_source_instance: DirectIngestInstance,
        raw_file_config: DirectIngestRawFileConfig,
    ):
        self.project_id = project_id or metadata.project_id()
        self.raw_data_source_instance = raw_data_source_instance
        self.region_code = region_code
        self.raw_file_config = raw_file_config
        self.view_id = f"{raw_file_config.file_tag}_latest"
        self.description = f"{raw_file_config.file_tag} latest view"
        self.dataset_id = raw_latest_views_dataset_for_region(
            state_code=StateCode(region_code.upper()),
            instance=self.raw_data_source_instance,
            sandbox_dataset_prefix=None,
        )
        self.projects_to_deploy = None
        self.materialized_address = None

        self.dataset_id = raw_latest_views_dataset_for_region(
            state_code=StateCode(region_code.upper()),
            instance=self.raw_data_source_instance,
            sandbox_dataset_prefix=None,
        )

    def _build(
        self, *, address_overrides: Optional[BigQueryAddressOverrides] = None
    ) -> BigQueryView:
        query = RawTableQueryBuilder(
            project_id=self.project_id,
            region_code=self.region_code,
            raw_data_source_instance=self.raw_data_source_instance,
        ).build_query(
            raw_file_config=self.raw_file_config,
            address_overrides=address_overrides,
            normalized_column_values=True,
            raw_data_datetime_upper_bound=None,
        )
        return BigQueryView(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            view_query_template=query,
            description=self.description,
            address_overrides=address_overrides,
        )


class DirectIngestRawDataTableLatestViewCollector(
    BigQueryViewCollector[DirectIngestRawDataTableLatestViewBuilder]
):
    """Collects all raw data `*_latest` views for a given region and ingest instance."""

    def __init__(
        self,
        region_code: str,
        raw_data_source_instance: DirectIngestInstance,
    ):
        self.region_code = region_code
        self.raw_data_source_instance = raw_data_source_instance

    def collect_view_builders(self) -> List[DirectIngestRawDataTableLatestViewBuilder]:
        region_raw_file_config = DirectIngestRegionRawFileConfig(self.region_code)
        raw_file_configs = region_raw_file_config.raw_file_configs

        return [
            self._builder_for_config(config)
            for config in raw_file_configs.values()
            if not config.is_undocumented
        ]

    def _builder_for_config(
        self, config: DirectIngestRawFileConfig
    ) -> DirectIngestRawDataTableLatestViewBuilder:
        return DirectIngestRawDataTableLatestViewBuilder(
            region_code=self.region_code,
            raw_file_config=config,
            raw_data_source_instance=self.raw_data_source_instance,
        )
