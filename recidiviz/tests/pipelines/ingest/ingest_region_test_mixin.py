# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared base for ingest pipeline region test mixins."""
import abc
from types import ModuleType

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
    IngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)


class IngestRegionTestMixin(abc.ABC):
    """Shared base for ingest pipeline region test mixins.

    Subclasses must implement the pipeline-type-specific bits
    (``ingest_pipeline_type`` and ``manifest_compiler_delegate``) as well as
    the test-specific ``state_code`` and ``region_module_override``.
    """

    @classmethod
    @abc.abstractmethod
    def ingest_pipeline_type(cls) -> IngestPipelineType:
        """Must be implemented by subclasses to return the pipeline type
        (activity or identity) the test exercises."""

    @classmethod
    @abc.abstractmethod
    def manifest_compiler_delegate(cls) -> IngestViewManifestCompilerDelegate:
        """Must be implemented by subclasses to return the manifest-compiler
        delegate for the pipeline type the test exercises."""

    @classmethod
    @abc.abstractmethod
    def state_code(cls) -> StateCode:
        """Must be implemented by subclasses to return the StateCode for the test."""

    @classmethod
    @abc.abstractmethod
    def region_module_override(cls) -> ModuleType | None:
        """Must be implemented by subclasses to return the region module override."""

    @classmethod
    def region(cls) -> DirectIngestRegion:
        return get_direct_ingest_region(
            region_code=cls.state_code().value.lower(),
            region_module_override=cls.region_module_override() or regions,
        )

    @classmethod
    def ingest_view_manifest_collector(cls) -> IngestViewManifestCollector:
        return IngestViewManifestCollector(
            region=cls.region(),
            delegate=cls.manifest_compiler_delegate(),
            ingest_pipeline_type=cls.ingest_pipeline_type(),
        )

    @classmethod
    def launchable_ingest_views(cls) -> list[str]:
        return cls.ingest_view_manifest_collector().launchable_ingest_views(
            IngestViewContentsContext.build_for_tests(state_code=cls.state_code())
        )

    @classmethod
    def ingest_view_collector(cls) -> DirectIngestViewQueryBuilderCollector:
        return DirectIngestViewQueryBuilderCollector(
            region=cls.region(),
            ingest_pipeline_type=cls.ingest_pipeline_type(),
            expected_ingest_views=cls.launchable_ingest_views(),
        )
