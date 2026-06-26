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
"""End-to-end tests for `StateSpecificIngestViewDataLineageBuilder`."""

import unittest
from unittest.mock import patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tools.mcp.data_lineage.ingest_view_lineage_builder import (
    StateSpecificIngestViewDataLineageBuilder,
)


class TestBuildLineageTreeForStateEntity(unittest.TestCase):
    """Exercises `build_lineage_tree_for_state_entity` against the fake `US_XX`
    region.
    """

    def setUp(self) -> None:
        self.direct_ingest_regions_patcher = patch(
            "recidiviz.tools.mcp.data_lineage.ingest_view_lineage_builder."
            "direct_ingest_regions",
            autospec=True,
        )
        mock_direct_ingest_regions = self.direct_ingest_regions_patcher.start()
        mock_direct_ingest_regions.get_direct_ingest_region.side_effect = (
            lambda region_code: get_direct_ingest_region(
                region_code, region_module_override=fake_regions
            )
        )

        self.project_id_patcher = patch(
            "recidiviz.tools.mcp.data_lineage.ingest_view_lineage_builder."
            "metadata.project_id",
            return_value="recidiviz-staging",
        )
        self.project_id_patcher.start()

    def tearDown(self) -> None:
        self.direct_ingest_regions_patcher.stop()
        self.project_id_patcher.stop()

    def test_state_person_lineage_includes_activity_views_only(self) -> None:
        builder = StateSpecificIngestViewDataLineageBuilder("US_XX")
        tree = builder.build_lineage_tree_for_state_entity(
            normalized_address=BigQueryAddress(
                dataset_id="us_xx_normalized_state",
                table_id="state_person",
            ),
        )

        # Root node points to the requested normalized address.
        self.assertEqual(
            BigQueryAddress(
                dataset_id="us_xx_normalized_state",
                table_id="state_person",
            ),
            tree.address,
        )

        # Activity ingest views that hydrate StatePerson in fake_regions are
        # basic, tagBasicData, and tagMoreBasicData.
        ingest_view_addresses = sorted(
            (dep.address.dataset_id, dep.address.table_id) for dep in tree.dependencies
        )
        self.assertEqual(
            [
                ("us_xx_ingest_view_results", "basic"),
                ("us_xx_ingest_view_results", "tagBasicData"),
                ("us_xx_ingest_view_results", "tagMoreBasicData"),
            ],
            ingest_view_addresses,
        )

        # Each ingest-view dependency has the raw-data table it reads from
        # nested under it.
        for ingest_view_subtree in tree.dependencies:
            self.assertTrue(
                ingest_view_subtree.dependencies,
                f"Expected raw-data dependencies under ingest view "
                f"[{ingest_view_subtree.address.table_id}]",
            )
            for raw_data_node in ingest_view_subtree.dependencies:
                self.assertTrue(
                    raw_data_node.address.dataset_id.endswith("_raw_data"),
                    f"Expected raw-data dataset, got "
                    f"[{raw_data_node.address.dataset_id}]",
                )

    def test_unmapped_entity_returns_as_source_table(self) -> None:
        """An entity not hydrated by any fake_regions ingest view is returned
        as a leaf (no dependencies)."""
        builder = StateSpecificIngestViewDataLineageBuilder("US_XX")
        tree = builder.build_lineage_tree_for_state_entity(
            normalized_address=BigQueryAddress(
                dataset_id="us_xx_normalized_state",
                table_id="state_supervision_period",
            ),
        )
        self.assertEqual([], tree.dependencies)
