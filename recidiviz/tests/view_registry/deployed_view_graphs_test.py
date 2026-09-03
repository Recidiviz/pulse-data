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
"""Tests for deployed_view_graphs.py"""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.utils.environment import (
    DATA_PLATFORM_GCP_PROJECTS,
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.types import assert_type
from recidiviz.view_registry.deployed_view_graphs import (
    CALCULATION_VIEW_GRAPH_NAME,
    builders_for_all_deployed_view_graphs,
    builders_for_all_view_graphs_across_projects,
    deployed_view_graph_registry,
)


class TestDeployedViewGraphRegistry(unittest.TestCase):
    """Tests for deployed_view_graph_registry()."""

    def test_registry_builds(self) -> None:
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with self.subTest(project_id=project_id), local_project_id_override(
                project_id
            ):
                registry = deployed_view_graph_registry(project_id)
                self.assertEqual(project_id, registry.project_id)
                calculation_graph = registry.graph_for_name(CALCULATION_VIEW_GRAPH_NAME)
                deployed_addresses = [
                    b.address
                    for b in builders_for_all_view_graphs_across_projects()
                    if b.should_deploy_in_project(project_id)
                ]
                graph_addresses = [b.address for b in calculation_graph.view_builders]
                # Compare as sets because view builder collection order is not
                # deterministic across calls.
                self.assertEqual(len(deployed_addresses), len(graph_addresses))
                self.assertEqual(set(deployed_addresses), set(graph_addresses))
                # With only the calculation graph registered, the all-graphs union
                # matches the calculation graph exactly.
                self.assertEqual(
                    set(graph_addresses),
                    {b.address for b in builders_for_all_deployed_view_graphs()},
                )
                self.assertTrue(calculation_graph.input_source_table_collections)
                self.assertEqual(
                    SourceTableUpdateGroup.CALC,
                    calculation_graph.input_source_table_update_group,
                )
                for collection in calculation_graph.input_source_table_collections:
                    self.assertIn(
                        SourceTableUpdateGroup.CALC,
                        assert_type(collection.update_groups, set),
                    )

    def test_project_id_mismatch_raises(self) -> None:
        with local_project_id_override(GCP_PROJECT_STAGING):
            with self.assertRaisesRegex(
                ValueError,
                rf"^Expected project_id \[{GCP_PROJECT_PRODUCTION}\] to match the "
                rf"current project \[{GCP_PROJECT_STAGING}\]\.$",
            ):
                deployed_view_graph_registry(GCP_PROJECT_PRODUCTION)


class TestBuildersForAllViewGraphsAcrossProjects(unittest.TestCase):
    """Tests for builders_for_all_view_graphs_across_projects()."""

    def test_deployed_builders_are_drawn_from_the_cross_project_roster(self) -> None:
        """Ensure each graph's project-filtered builders come from its cross-project
        roster."""
        for project_id in DATA_PLATFORM_GCP_PROJECTS:
            with self.subTest(project_id=project_id), local_project_id_override(
                project_id
            ):
                roster_addresses = {
                    b.address for b in builders_for_all_view_graphs_across_projects()
                }
                deployed_addresses = {
                    b.address
                    for graph in deployed_view_graph_registry(project_id).view_graphs
                    for b in graph.view_builders
                }
                unrostered_addresses = deployed_addresses - roster_addresses
                self.assertEqual(
                    set(),
                    unrostered_addresses,
                    f"Views deployed to [{project_id}] that no view graph's "
                    f"cross-project roster contains: "
                    f"{BigQueryAddress.addresses_to_str(unrostered_addresses)}",
                )

    @patch("recidiviz.utils.environment.in_gcp", MagicMock(return_value=True))
    def test_cross_project_rosters_unavailable_in_gcp(self) -> None:
        with self.assertRaisesRegex(
            RuntimeError, r"^Not available, see service logs\.$"
        ):
            builders_for_all_view_graphs_across_projects()
