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
from typing import Callable, List, Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_dir_names,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestRawDataTableLatestView,
)


class DirectIngestRawDataTableLatestViewBuilder(
    BigQueryViewBuilder[DirectIngestRawDataTableLatestView]
):
    """Factory class for building DirectIngestRawDataTableLatestView"""

    def __init__(
        self,
        *,
        project_id: str = None,
        region_code: str,
        raw_file_config: DirectIngestRawFileConfig,
        should_build_predicate: Optional[Callable[[], bool]] = None,
    ):
        self.project_id = project_id
        self.region_code = region_code
        self.raw_file_config = raw_file_config
        self.should_build_predicate = should_build_predicate
        self.view_id = f"{raw_file_config.file_tag}_latest"
        self.dataset_id = raw_latest_views_dataset_for_region(
            region_code=self.region_code.lower(), sandbox_dataset_prefix=None
        )
        self.projects_to_deploy = None
        self.materialized_address = None

    def _build(
        self, *, address_overrides: Optional[BigQueryAddressOverrides] = None
    ) -> DirectIngestRawDataTableLatestView:
        return DirectIngestRawDataTableLatestView(
            project_id=self.project_id,
            region_code=self.region_code,
            raw_file_config=self.raw_file_config,
            address_overrides=address_overrides,
        )

    def should_build(self) -> bool:
        return not self.should_build_predicate or self.should_build_predicate()


class DirectIngestRawDataTableLatestViewCollector(
    BigQueryViewCollector[DirectIngestRawDataTableLatestViewBuilder]
):
    """Collects all raw data `*_latest` views for a given region."""

    def __init__(self, src_raw_tables_sandbox_dataset_prefix: Optional[str]):
        self.src_raw_tables_sandbox_dataset_prefix = (
            src_raw_tables_sandbox_dataset_prefix
        )

    def collect_view_builders(self) -> List[DirectIngestRawDataTableLatestViewBuilder]:
        builder_list = []
        for region_code in get_existing_region_dir_names():
            region_raw_file_config = DirectIngestRegionRawFileConfig(region_code)
            raw_file_configs = region_raw_file_config.raw_file_configs
            src_raw_tables_dataset = raw_tables_dataset_for_region(
                region_raw_file_config.region_code,
                sandbox_dataset_prefix=self.src_raw_tables_sandbox_dataset_prefix,
            )
            builder_list.extend(
                [
                    DirectIngestRawDataTableLatestViewBuilder(
                        region_code=region_code,
                        raw_file_config=config,
                        should_build_predicate=BigQueryTableChecker(
                            src_raw_tables_dataset,
                            config.file_tag,
                        ).get_table_exists_predicate(),
                    )
                    for config in raw_file_configs.values()
                    if not config.is_undocumented and config.primary_key_cols
                ]
            )
        return builder_list
