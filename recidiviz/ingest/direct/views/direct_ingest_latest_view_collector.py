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
from typing import List, Optional, Dict, Callable

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestRawDataTableLatestView,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_region_dir_names,
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
        self.dataset_id = f"{self.region_code.lower()}_raw_data_up_to_date_views"
        self.materialized_address_override = None

    def _build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> DirectIngestRawDataTableLatestView:
        return DirectIngestRawDataTableLatestView(
            project_id=self.project_id,
            region_code=self.region_code,
            raw_file_config=self.raw_file_config,
            dataset_overrides=dataset_overrides,
        )

    def should_build(self) -> bool:
        return not self.should_build_predicate or self.should_build_predicate()


class DirectIngestRawDataTableLatestViewCollector(
    BigQueryViewCollector[DirectIngestRawDataTableLatestViewBuilder]
):
    def collect_view_builders(self) -> List[DirectIngestRawDataTableLatestViewBuilder]:
        builder_list = []
        for region_code in get_existing_region_dir_names():
            region_raw_file_config = DirectIngestRegionRawFileConfig(region_code)
            raw_file_configs = region_raw_file_config.raw_file_configs
            builder_list.extend(
                [
                    DirectIngestRawDataTableLatestViewBuilder(
                        region_code=region_code,
                        raw_file_config=config,
                        should_build_predicate=BigQueryTableChecker(
                            f"{region_raw_file_config.region_code.lower()}_raw_data",
                            config.file_tag,
                        ).get_table_exists_predicate(),
                    )
                    for config in raw_file_configs.values()
                    if not config.is_undocumented and config.primary_key_cols
                ]
            )
        return builder_list
