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
"""Base class for testing identity mapping YAMLs against sample input rows.

Compiles a mapping via `IngestViewManifestCompiler` and exposes a helper that
parses dict rows into `IdentityFragment` entities. Test methods then assert
the produced entity trees match expectations.
"""
import abc
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifestCompiler,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.utils.types import assert_type


class IdentityMappingTestCase(unittest.TestCase, abc.ABC):
    """Base class for compiler-only tests of identity mapping YAMLs."""

    # Prevent pytest from collecting this abstract base class as a test class.
    __test__ = False

    @classmethod
    @abc.abstractmethod
    def tenant(cls) -> Tenant:
        """Returns the tenant whose mapping YAML is under test (e.g.,
        `Tenant.US_OZ`)."""

    @classmethod
    @abc.abstractmethod
    def ingest_view_name(cls) -> str:
        """Returns the ingest view name whose mapping YAML is under test
        (e.g., `person`)."""

    def parse_rows(self, rows: list[dict]) -> list[IdentityFragment]:
        """Returns the IdentityFragment entities produced by feeding `rows`
        through this view's mapping YAML."""
        delegate = IdentityIngestViewManifestCompilerDelegate(
            region=get_direct_ingest_region(self.tenant().value)
        )
        compiler = IngestViewManifestCompiler(delegate)
        manifest = compiler.compile_manifest(ingest_view_name=self.ingest_view_name())
        parsed = manifest.parse_contents(
            contents_iterator=iter(rows),
            context=IngestViewContentsContext.build_for_tests(
                StateCode(self.tenant().value)
            ),
        )
        return [assert_type(entity, IdentityFragment) for entity in parsed]
