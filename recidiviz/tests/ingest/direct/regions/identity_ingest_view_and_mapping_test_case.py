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
"""SQL-executing test harness for identity ingest views.

Reuses StateIngestViewAndMappingTestCase (BigQuery emulator + view-query
execution) but compiles mappings with the identity manifest delegate. This
gives identity views the test_validate_view_output_schema check: the view SQL
runs against empty raw tables and its output columns must exactly match the
input_columns of its identity mapping, catching column drift between a view
and its mapping.

This harness does not yet support run_ingest_view_test (running an identity
view against raw-data fixtures): that requires overriding both the fixture
dataset (identity view results live in a separate dataset from the activity
views of the same name) and the entity-tree comparison (identity's root is
IdentityFragment, not StatePerson/StateStaff).
"""
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifestCompiler,
)
from recidiviz.tests.ingest.direct.regions.ingest_view_query_test_case import (
    StateIngestViewAndMappingTestCase,
)


# pylint: disable=abstract-method
class IdentityIngestViewAndMappingTestCase(StateIngestViewAndMappingTestCase):
    """Base test class for identity ingest views: runs the view SQL and checks
    its output schema against the identity mapping's declared input columns."""

    __test__ = False

    @classmethod
    def build_ingest_view_manifest_compiler(cls) -> IngestViewManifestCompiler:
        """Returns a manifest compiler wired to the identity manifest delegate."""
        return IngestViewManifestCompiler(
            delegate=IdentityIngestViewManifestCompilerDelegate(
                region=get_direct_ingest_region(cls.state_code().value)
            )
        )
