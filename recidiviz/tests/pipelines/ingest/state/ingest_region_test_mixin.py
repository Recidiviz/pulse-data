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
"""Defines a helper mixin for accessing information about a given ingest region and its
associated test fixture files.
"""
import abc
from datetime import datetime
from types import ModuleType
from typing import Any, Dict, Iterable, Optional

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.direct_ingest_regions import (
    DirectIngestRegion,
    get_direct_ingest_region,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestTestFixturePath,
    load_dataframe_from_path,
)
from recidiviz.tests.ingest.direct.regions.state_ingest_view_parser_test_base import (
    DEFAULT_UPDATE_DATETIME,
)


class IngestRegionTestMixin(abc.ABC):
    """A helper mixin for accessing information about a given ingest region and its
    associated test fixture files.
    """

    @classmethod
    @abc.abstractmethod
    def state_code(cls) -> StateCode:
        """Must be implemented by subclasses to return the StateCode for the test."""

    @classmethod
    @abc.abstractmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        """Must be implemented by subclasses to return the region module override."""

    @classmethod
    def region(cls) -> DirectIngestRegion:
        return get_direct_ingest_region(
            region_code=cls.state_code().value.lower(),
            region_module_override=cls.region_module_override() or regions,
        )

    @classmethod
    def state_code_str_upper(cls) -> str:
        return cls.state_code().value.upper()

    @classmethod
    def ingest_view_manifest_collector(cls) -> IngestViewManifestCollector:
        return IngestViewManifestCollector(
            cls.region(),
            delegate=StateSchemaIngestViewManifestCompilerDelegate(cls.region()),
        )

    @classmethod
    def launchable_ingest_views(cls) -> list[str]:
        return cls.ingest_view_manifest_collector().launchable_ingest_views(
            IngestViewContentsContext.build_for_tests()
        )

    @classmethod
    def ingest_view_collector(cls) -> DirectIngestViewQueryBuilderCollector:
        return DirectIngestViewQueryBuilderCollector(
            cls.region(),
            cls.launchable_ingest_views(),
        )

    def get_ingest_view_results_from_fixture(
        self,
        *,
        ingest_view_name: str,
        test_name: str,
        fixture_has_metadata_columns: bool = True,
        generate_default_metadata: bool = False,
        use_results_fixture: bool = True,
    ) -> Iterable[Dict[str, Any]]:
        """Reads in an ingest view fixture to be used in tests.

        If the ingest view fixture has metadata in the CSV,
        fixture_has_metadata_columns should be True.

        If the fixture CSV does not have metadata, but we would
        like to generate default values, then generate_default_metadata
        should be True.

        If we're loading an ingest view *result* fixture, then
        use_results_fixture should be True. If we are using
        an "extract and merge" fixture, it should be False.

        TODO(#22059): Standardize ingest view fixtures and simplify
                      this method. Ideally a pipeline test will only
                      need to read ingest view result fixtures, and
                      we can deprecate "extract and merge" fixtures.
        """
        if fixture_has_metadata_columns and generate_default_metadata:
            raise ValueError(
                "Can't read metadata from fixture and also generate default values!"
            )
        fixture_columns = (
            self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .input_columns
        )
        if fixture_has_metadata_columns:
            fixture_columns.extend(
                [MATERIALIZATION_TIME_COL_NAME, UPPER_BOUND_DATETIME_COL_NAME]
            )

        if use_results_fixture:
            fixture_path = (
                DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                    region_code=self.region().region_code,
                    ingest_view_name=ingest_view_name,
                    file_name=f"{test_name}.csv",
                ).full_path()
            )
        else:
            fixture_path = DirectIngestTestFixturePath.for_extract_and_merge_fixture(
                region_code=self.state_code().value,
                file_name=f"{test_name}.csv",
            ).full_path()

        df = load_dataframe_from_path(
            fixture_path,
            fixture_columns,
        )
        if generate_default_metadata:
            df[MATERIALIZATION_TIME_COL_NAME] = datetime.now().isoformat()
            df[UPPER_BOUND_DATETIME_COL_NAME] = DEFAULT_UPDATE_DATETIME.isoformat()
        return df.to_dict("records")
