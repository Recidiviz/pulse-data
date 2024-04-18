# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Collection of utilities for reasoning about raw file configs."""

from functools import lru_cache
from types import ModuleType
from typing import Optional, Set

from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)


# TODO(#28560): replace w/ full look @ dag, not just ingest views
@lru_cache
def raw_file_tags_referenced_downstream(
    region_code: str, region_module_override: Optional[ModuleType] = None
) -> Set[str]:
    """Given a region code, this funciton will return a set of all raw file tags that
    are referenced by downstream ingest views
    """
    region = get_direct_ingest_region(
        region_code,
        region_module_override=region_module_override,
    )

    view_collector = DirectIngestViewQueryBuilderCollector(region, [])
    return {
        raw_file_dependency.raw_file_config.file_tag
        for ingest_view in view_collector.collect_query_builders()
        for raw_file_dependency in ingest_view.raw_table_dependency_configs
    }
