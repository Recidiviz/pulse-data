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

from types import ModuleType
from typing import List, Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct.direct_ingest_regions import (
    DirectIngestRegion,
    get_direct_ingest_region,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)

_INGEST_VIEWS_SUBDIR_NAME = "ingest_views"
_INGEST_VIEW_FILE_PREFIX = "view_"
_VIEW_BUILDER_EXPECTED_NAME = "VIEW_BUILDER"


class DirectIngestViewQueryBuilderCollector(ModuleCollectorMixin):
    """A class that collects all defined ingest view query builders for a given region."""

    def __init__(
        self, region: DirectIngestRegion, expected_ingest_views: List[str] | None = None
    ):
        self.region = region

        # We do a set difference against these views and raise
        # a ValueError if there is one. If it is None we skip the check.
        self.expected_ingest_views = expected_ingest_views

        self._builders_by_view_name: dict[
            str, DirectIngestViewQueryBuilder
        ] | None = None

    @classmethod
    def from_state_code(
        cls,
        state_code: StateCode,
        region_module_override: Optional[ModuleType] = None,
        expected_ingest_views: Optional[List[str]] = None,
    ) -> "DirectIngestViewQueryBuilderCollector":
        return DirectIngestViewQueryBuilderCollector(
            get_direct_ingest_region(state_code.value, region_module_override),
            expected_ingest_views,
        )

    def _collect_query_builders(self) -> List[DirectIngestViewQueryBuilder]:
        """Collect the ingest view query builders for this state from the file system."""
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

    def _query_builders_by_name(
        self,
    ) -> dict[str, DirectIngestViewQueryBuilder]:
        if not self._builders_by_view_name:
            ingest_view_builders = self._collect_query_builders()

            self._builders_by_view_name = {
                builder.ingest_view_name: builder for builder in ingest_view_builders
            }
        return self._builders_by_view_name

    def get_query_builders(self) -> List[DirectIngestViewQueryBuilder]:
        """Returns a list of ingest view query builders for the state, collecting them
        via a file system read if we have not already.
        """
        return list(self._query_builders_by_name().values())

    def get_query_builder_by_view_name(
        self, ingest_view_name: str
    ) -> DirectIngestViewQueryBuilder:
        """Returns the ingest view query builder with the given ingest_view_name. Will
        do a file system read to load builders if they have not been loaded already.
        """
        return self._query_builders_by_name()[ingest_view_name]

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
        if self.expected_ingest_views:
            expected_names_no_view_defined = set(self.expected_ingest_views).difference(
                found_ingest_view_names_set
            )
            if expected_names_no_view_defined:
                raise ValueError(
                    f"Found expected ingest view names with no corresponding view "
                    f"defined: {expected_names_no_view_defined}"
                )

    def get_ingest_views_referencing(self, file_tag: str) -> set[str]:
        """Returns all ingest views names that reference |file_tag|."""
        return {
            ingest_view.ingest_view_name
            for ingest_view in self._query_builders_by_name().values()
            if file_tag in ingest_view.raw_data_table_dependency_file_tags
        }
