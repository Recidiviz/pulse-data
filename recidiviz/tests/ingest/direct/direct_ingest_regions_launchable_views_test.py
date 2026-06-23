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
"""Tests that the has_launchable_ingest_views_in_* flags in each state's
manifest.yaml are consistent with the actual IngestViewManifestCollector
results.
"""
from unittest import TestCase

from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class TestHasLaunchableIngestViewsFlagsMatchReality(TestCase):
    """Asserts the static has_launchable_ingest_views_in_* flags on each
    state's manifest.yaml match the result of actually running the
    IngestViewManifestCollector for that state in each environment.
    """

    def test_flags_match_collector_results(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            region = get_direct_ingest_region(region_code=state_code.value.lower())
            collector = IngestViewManifestCollector(
                region=region,
                delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
                ingest_pipeline_type=IngestPipelineType.ACTIVITY,
            )

            for project_id in [GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION]:
                context = IngestViewContentsContext.build_for_project(
                    project_id=project_id,
                    is_sandbox=False,
                    state_code=state_code,
                )
                actual_has_launchable = (
                    len(collector.launchable_ingest_views(context)) > 0
                )
                flag_value = region.has_launchable_ingest_views(project_id=project_id)
                self.assertEqual(
                    flag_value,
                    actual_has_launchable,
                    f"has_launchable_ingest_views({project_id}) for "
                    f"{state_code.value} is {flag_value} but "
                    f"launchable_ingest_views() returns "
                    f"{'non-empty' if actual_has_launchable else 'empty'}",
                )
