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
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.ingest.direct.legacy_fixture_path import (
    DirectIngestTestFixturePath,
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

    # TODO(#38321): Delete this when all ingest view and mapping tests are migrated.
    # TODO(#22059): Remove this method and replace with the implementation on
    # StateIngestPipelineTestCase when fixture formats and data loading is standardized.
    def read_legacy_extract_and_merge_fixture(
        self,
        *,
        ingest_view_name: str,
        test_name: str,
    ) -> Iterable[Dict[str, Any]]:
        """
        Reads in an "extract and merge fixture" to be used in
        legacy integration tests.
        """
        fixture_columns = (
            self.ingest_view_manifest_collector()
            .ingest_view_to_manifest[ingest_view_name]
            .input_columns
        )
        fixture_path = DirectIngestTestFixturePath.for_extract_and_merge_fixture(
            region_code=self.state_code().value,
            file_name=f"{test_name}.csv",
        ).full_path()
        try:
            df = load_dataframe_from_path(
                fixture_path,
                fixture_columns,
            )
            df[MATERIALIZATION_TIME_COL_NAME] = datetime.now().isoformat()
            df[UPPER_BOUND_DATETIME_COL_NAME] = DEFAULT_UPDATE_DATETIME.isoformat()
            return df.to_dict("records")
        except Exception as ex:
            raise ValueError(f"Failed to read fixture file at {fixture_path}") from ex
