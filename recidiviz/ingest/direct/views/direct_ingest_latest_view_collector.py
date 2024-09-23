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
from types import ModuleType
from typing import List

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions, regions
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.raw_table_query_builder import RawTableQueryBuilder
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING

RAW_DATA_LATEST_VIEW_ID_SUFFIX = "_latest"


class DirectIngestRawDataTableLatestViewBuilder(BigQueryViewBuilder):
    """Factory class for building DirectIngestRawDataTableLatestView"""

    def __init__(
        self,
        *,
        region_code: str,
        raw_data_source_instance: DirectIngestInstance,
        raw_file_config: DirectIngestRawFileConfig,
        regions_module: ModuleType,
    ):
        self.raw_data_source_instance = raw_data_source_instance
        self.region_code = region_code
        self.raw_file_config = raw_file_config
        self.view_id = f"{raw_file_config.file_tag}{RAW_DATA_LATEST_VIEW_ID_SUFFIX}"
        self.description = f"{raw_file_config.file_tag} latest view"
        self.dataset_id = raw_latest_views_dataset_for_region(
            state_code=StateCode(region_code.upper()),
            instance=self.raw_data_source_instance,
            sandbox_dataset_prefix=None,
        )
        region = direct_ingest_regions.get_direct_ingest_region(
            region_code.lower(), region_module_override=regions_module
        )
        self.projects_to_deploy = {GCP_PROJECT_STAGING} if region.playground else None
        self.materialized_address = None

        self.dataset_id = raw_latest_views_dataset_for_region(
            state_code=StateCode(region_code.upper()),
            instance=self.raw_data_source_instance,
            sandbox_dataset_prefix=None,
        )

    def _build(
        self, *, sandbox_context: BigQueryViewSandboxContext | None
    ) -> BigQueryView:
        project_id = metadata.project_id()
        query = (
            RawTableQueryBuilder(
                project_id=project_id,
                region_code=self.region_code,
                raw_data_source_instance=self.raw_data_source_instance,
            ).build_query(
                raw_file_config=self.raw_file_config,
                parent_address_overrides=(
                    sandbox_context.parent_address_overrides
                    if sandbox_context
                    else None
                ),
                normalized_column_values=True,
                raw_data_datetime_upper_bound=None,
                filter_to_latest=True,
                filter_to_only_documented_columns=True,
            )
            # Remove the project id values from the query and replace with template
            # params so we can enforce that no BigQueryView view_query_template have
            # project_id values already hydrated.
            .replace(project_id, "{project_id}")
        )
        return BigQueryView(
            project_id=project_id,
            dataset_id=self.dataset_id,
            view_id=self.view_id,
            view_query_template=query,
            bq_description=self.description,
            description=self.description,
            sandbox_context=sandbox_context,
        )


class DirectIngestRawDataTableLatestViewCollector(
    BigQueryViewCollector[DirectIngestRawDataTableLatestViewBuilder]
):
    """Collects all raw data `*_latest` views for a given region and ingest instance."""

    def __init__(
        self,
        region_code: str,
        raw_data_source_instance: DirectIngestInstance,
        regions_module: ModuleType = regions,
    ):
        self.region_code = region_code
        self.raw_data_source_instance = raw_data_source_instance
        self.regions_module = regions_module

    def collect_view_builders(self) -> List[DirectIngestRawDataTableLatestViewBuilder]:
        region_raw_file_config = DirectIngestRegionRawFileConfig(
            self.region_code, region_module=self.regions_module
        )
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
            regions_module=self.regions_module,
        )
