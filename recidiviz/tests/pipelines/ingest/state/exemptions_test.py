# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for exemptions.py."""
import unittest
from collections import defaultdict
from typing import Dict, Set

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContextImpl,
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
from recidiviz.pipelines.ingest.state.exemptions import (
    INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS,
)
from recidiviz.utils.environment import GCP_PROJECTS


class TestExemptions(unittest.TestCase):
    """Unit tests for exemptions.py"""

    def test_ingest_view_tree_merger_names_match_launched_ingest_views(self) -> None:
        state_codes = get_existing_direct_ingest_states()
        state_code_to_launchable_views: Dict[StateCode, Set[str]] = defaultdict(set)
        for state_code in state_codes:
            for project_id in GCP_PROJECTS:
                region = get_direct_ingest_region(region_code=state_code.value)
                ingest_manifest_collector = IngestViewManifestCollector(
                    region=region,
                    delegate=StateSchemaIngestViewManifestCompilerDelegate(
                        region=region
                    ),
                )
                all_launchable_views = (
                    ingest_manifest_collector.launchable_ingest_views(
                        IngestViewContentsContextImpl.build_for_project(project_id)
                    )
                )
                state_code_to_launchable_views[state_code].update(
                    set(all_launchable_views)
                )

        for (
            state_code,
            exempted_views,
        ) in INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS.items():
            self.assertTrue(state_code in state_code_to_launchable_views)
            exempted_difference = exempted_views.difference(
                state_code_to_launchable_views[state_code]
            )
            self.assertEqual(0, len(exempted_difference))
