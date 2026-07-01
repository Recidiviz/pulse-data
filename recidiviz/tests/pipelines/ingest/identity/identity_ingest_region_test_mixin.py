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
"""Helper mixin for tests that run the identity ingest pipeline against a
specific region."""
import abc

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.tests.pipelines.ingest.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)


class IdentityIngestRegionTestMixin(IngestRegionTestMixin):
    """Helper mixin for tests that run the identity ingest pipeline against a
    specific region.

    Identity is tenant-keyed (not state-keyed). Subclasses specify ``tenant()``;
    ``state_code()`` is derived from the tenant.
    """

    @classmethod
    @abc.abstractmethod
    def tenant(cls) -> Tenant:
        """Returns the Tenant the test exercises."""

    @classmethod
    def state_code(cls) -> StateCode:
        return cls.tenant().to_state_code()

    @classmethod
    def ingest_pipeline_type(cls) -> IngestPipelineType:
        return IngestPipelineType.IDENTITY

    @classmethod
    def manifest_compiler_delegate(cls) -> IngestViewManifestCompilerDelegate:
        return IdentityIngestViewManifestCompilerDelegate(cls.region())
