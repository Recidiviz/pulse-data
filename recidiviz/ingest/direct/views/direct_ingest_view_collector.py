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
"""A class that collects all defined ingest view query builders for a given region."""

from typing import List

from more_itertools import one

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestViewQueryBuilder,
)

_INGEST_VIEWS_SUBDIR_NAME = "ingest_views"
_INGEST_VIEW_FILE_PREFIX = "view_"
_VIEW_BUILDER_EXPECTED_NAME = "VIEW_BUILDER"


class DirectIngestViewQueryBuilderCollector(ModuleCollectorMixin):
    """A class that collects all defined ingest view query builders for a given region."""

    def __init__(
        self, region: DirectIngestRegion, controller_ingest_view_rank_list: List[str]
    ):
        self.region = region
        self.controller_ingest_view_rank_list = controller_ingest_view_rank_list

    def collect_query_builders(self) -> List[DirectIngestViewQueryBuilder]:
        if self.region.region_module.__file__ is None:
            raise ValueError(f"No file associated with {self.region.region_module}.")

        view_dir_module = self.get_relative_module(
            self.region.region_module,
            [self.region.region_code, _INGEST_VIEWS_SUBDIR_NAME],
        )
        ingest_view_builders = self.collect_top_level_attributes_in_module(
            attribute_type=DirectIngestViewQueryBuilder,
            dir_module=view_dir_module,
            recurse=False,
            file_prefix_filter=_INGEST_VIEW_FILE_PREFIX,
            attribute_name_regex=_VIEW_BUILDER_EXPECTED_NAME,
            expect_match_in_all_files=True,
        )

        self._validate_query_builders(ingest_view_builders)

        return ingest_view_builders

    def get_query_builder_by_view_name(
        self, ingest_view_name: str
    ) -> DirectIngestViewQueryBuilder:
        return one(
            builder
            for builder in self.collect_query_builders()
            if builder.ingest_view_name == ingest_view_name
        )

    def _validate_query_builders(
        self, ingest_view_builders: List[DirectIngestViewQueryBuilder]
    ) -> None:
        found_ingest_view_names = [
            builder.ingest_view_name for builder in ingest_view_builders
        ]
        found_ingest_view_names_set = set(found_ingest_view_names)
        if len(found_ingest_view_names) > len(found_ingest_view_names_set):
            raise ValueError(
                f"Found duplicate ingest view names in the view directory. Found: "
                f"[{found_ingest_view_names}]."
            )

        controller_view_names_set = set(self.controller_ingest_view_rank_list)
        controller_names_no_view_defined = controller_view_names_set.difference(
            found_ingest_view_names_set
        )
        if self.region.is_ingest_launched_in_env() and controller_names_no_view_defined:
            raise ValueError(
                f"Found controller ingest view names with no corresponding view "
                f"defined: {controller_names_no_view_defined}"
            )
