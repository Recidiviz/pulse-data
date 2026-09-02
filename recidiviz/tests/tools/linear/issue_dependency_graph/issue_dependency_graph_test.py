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
"""Tests for building an issue dependency graph out of raw Linear responses."""
import datetime
import os
import tempfile
import unittest

from recidiviz.tools.linear.issue_dependency_graph.issue_dependency_graph import (
    GraphIssue,
    GraphMilestone,
    IssueDependencyGraph,
    build_graph,
    graph_milestones_for_config,
)
from recidiviz.tools.linear.issue_dependency_graph.project_display_config import (
    MilestoneDisplayConfig,
    ProjectDisplayConfig,
)

GENERATED_AT = datetime.datetime(2026, 8, 28, 17, 5, 3, tzinfo=datetime.timezone.utc)
GENERATED_AT_STR = "2026-08-28T17:05:03+00:00"

PHASE_1_ID = "11111111-1111-1111-1111-111111111111"
PHASE_2_ID = "22222222-2222-2222-2222-222222222222"


def make_config(*, exclude_parent_issues: bool = True) -> ProjectDisplayConfig:
    return ProjectDisplayConfig(
        project_name="Test Project",
        milestones=[
            MilestoneDisplayConfig(name="Phase 1", short_name="P1"),
            MilestoneDisplayConfig(name="Phase 2", short_name="P2"),
        ],
        exclude_parent_issues=exclude_parent_issues,
        labels_excluded_by_default=["Region: US_XX"],
    )


PHASE_1 = GraphMilestone(
    milestone_id=PHASE_1_ID, name="Phase 1", short_name="P1", sort_order=100.0
)
PHASE_2 = GraphMilestone(
    milestone_id=PHASE_2_ID, name="Phase 2", short_name="P2", sort_order=200.0
)


def linear_issue_node(
    *,
    identifier: str,
    title: str,
    state_name: str = "Todo",
    state_type: str = "unstarted",
    assignee_name: str | None = None,
    label_names: list[str] | None = None,
    child_ids: list[str] | None = None,
    blocks: list[str] | None = None,
) -> dict:
    """Returns a raw Linear issue node in the shape the API hands back."""
    return {
        "identifier": identifier,
        "title": title,
        "state": {"name": state_name, "type": state_type},
        "assignee": {"name": assignee_name} if assignee_name else None,
        "labels": {"nodes": [{"name": name} for name in label_names or []]},
        "children": {"nodes": [{"id": child_id} for child_id in child_ids or []]},
        "relations": {
            "nodes": [
                {
                    "type": "blocks",
                    "issue": {"identifier": identifier},
                    "relatedIssue": {"identifier": blocked},
                }
                for blocked in blocks or []
            ]
        },
    }


class TestBuildGraph(unittest.TestCase):
    """Tests for build_graph."""

    def test_build_graph(self) -> None:
        graph = build_graph(
            config=make_config(),
            milestones=[PHASE_1, PHASE_2],
            issue_nodes_by_milestone_id={
                PHASE_1_ID: [
                    linear_issue_node(
                        identifier="OBT-4",
                        title="[CNI][Extractor Config] Parse the config",
                        state_name="Done",
                        state_type="completed",
                        assignee_name="Anna Geiduschek",
                        label_names=["Team: Data Platform"],
                        blocks=["OBT-31"],
                    ),
                    linear_issue_node(
                        identifier="OBT-31",
                        title="[CNI] Build the extractor",
                        state_name="In Review",
                        state_type="started",
                        label_names=["Region: US_XX", "Team: Data Platform"],
                        blocks=["OBT-200"],
                    ),
                ],
                PHASE_2_ID: [
                    linear_issue_node(
                        identifier="OBT-200",
                        title="Launch the extractor",
                        state_name="In Progress",
                        state_type="started",
                        assignee_name="Kim Rodriguez",
                    ),
                ],
            },
            generated_at=GENERATED_AT,
        )

        self.assertEqual(
            IssueDependencyGraph(
                project_name="Test Project",
                generated_at=GENERATED_AT_STR,
                milestones=[PHASE_1, PHASE_2],
                issues=[
                    GraphIssue(
                        issue_id="OBT-4",
                        milestone_short_name="P1",
                        status="done",
                        assignee="Anna Geiduschek",
                        labels=["Team: Data Platform"],
                        title="Parse the config",
                    ),
                    GraphIssue(
                        issue_id="OBT-31",
                        milestone_short_name="P1",
                        status="review",
                        assignee=None,
                        labels=["Region: US_XX", "Team: Data Platform"],
                        title="Build the extractor",
                    ),
                    GraphIssue(
                        issue_id="OBT-200",
                        milestone_short_name="P2",
                        status="progress",
                        assignee="Kim Rodriguez",
                        labels=[],
                        title="Launch the extractor",
                    ),
                ],
                edges=[("OBT-4", "OBT-31"), ("OBT-31", "OBT-200")],
                labels_excluded_by_default=["Region: US_XX"],
            ),
            graph,
        )

    def test_build_graph_drops_canceled_issues_and_their_edges(self) -> None:
        graph = build_graph(
            config=make_config(),
            milestones=[PHASE_1],
            issue_nodes_by_milestone_id={
                PHASE_1_ID: [
                    linear_issue_node(
                        identifier="OBT-1",
                        title="Keep me",
                        blocks=["OBT-2"],
                    ),
                    linear_issue_node(
                        identifier="OBT-2",
                        title="Cancel me",
                        state_name="Canceled",
                        state_type="canceled",
                    ),
                ]
            },
            generated_at=GENERATED_AT,
        )

        self.assertEqual(["OBT-1"], [issue.issue_id for issue in graph.issues])
        self.assertEqual([], graph.edges)

    def test_build_graph_drops_parent_issues_when_configured(self) -> None:
        nodes = {
            PHASE_1_ID: [
                linear_issue_node(
                    identifier="OBT-1",
                    title="Container issue",
                    child_ids=["child-uuid"],
                    blocks=["OBT-2"],
                ),
                linear_issue_node(identifier="OBT-2", title="Leaf issue"),
            ]
        }

        with_parents_dropped = build_graph(
            config=make_config(exclude_parent_issues=True),
            milestones=[PHASE_1],
            issue_nodes_by_milestone_id=nodes,
            generated_at=GENERATED_AT,
        )
        self.assertEqual(
            ["OBT-2"], [issue.issue_id for issue in with_parents_dropped.issues]
        )
        self.assertEqual([], with_parents_dropped.edges)

        with_parents_kept = build_graph(
            config=make_config(exclude_parent_issues=False),
            milestones=[PHASE_1],
            issue_nodes_by_milestone_id=nodes,
            generated_at=GENERATED_AT,
        )
        self.assertEqual(
            ["OBT-1", "OBT-2"], [issue.issue_id for issue in with_parents_kept.issues]
        )
        self.assertEqual([("OBT-1", "OBT-2")], with_parents_kept.edges)

    def test_build_graph_drops_edges_to_issues_outside_the_graph(self) -> None:
        graph = build_graph(
            config=make_config(),
            milestones=[PHASE_1],
            issue_nodes_by_milestone_id={
                PHASE_1_ID: [
                    linear_issue_node(
                        identifier="OBT-1",
                        title="Blocks an issue in another project",
                        blocks=["OBT-999"],
                    )
                ]
            },
            generated_at=GENERATED_AT,
        )

        self.assertEqual(["OBT-1"], [issue.issue_id for issue in graph.issues])
        self.assertEqual([], graph.edges)

    def test_build_graph_ignores_non_blocking_relations(self) -> None:
        node = linear_issue_node(identifier="OBT-1", title="Related, not blocking")
        node["relations"]["nodes"] = [
            {
                "type": "related",
                "issue": {"identifier": "OBT-1"},
                "relatedIssue": {"identifier": "OBT-2"},
            }
        ]

        graph = build_graph(
            config=make_config(),
            milestones=[PHASE_1],
            issue_nodes_by_milestone_id={
                PHASE_1_ID: [node, linear_issue_node(identifier="OBT-2", title="Other")]
            },
            generated_at=GENERATED_AT,
        )

        self.assertEqual([], graph.edges)

    def test_build_graph_sorts_issue_numbers_numerically(self) -> None:
        graph = build_graph(
            config=make_config(),
            milestones=[PHASE_1],
            issue_nodes_by_milestone_id={
                PHASE_1_ID: [
                    linear_issue_node(identifier="OBT-31", title="Thirty one"),
                    linear_issue_node(identifier="OBT-4", title="Four"),
                    linear_issue_node(identifier="OBT-3", title="Three"),
                ]
            },
            generated_at=GENERATED_AT,
        )

        self.assertEqual(
            ["OBT-3", "OBT-4", "OBT-31"], [issue.issue_id for issue in graph.issues]
        )

    def test_build_graph_maps_every_workflow_state(self) -> None:
        graph = build_graph(
            config=make_config(),
            milestones=[PHASE_1],
            issue_nodes_by_milestone_id={
                PHASE_1_ID: [
                    linear_issue_node(
                        identifier="OBT-1",
                        title="Triaged",
                        state_name="Triage",
                        state_type="triage",
                    ),
                    linear_issue_node(
                        identifier="OBT-2",
                        title="Backlogged",
                        state_name="Backlog",
                        state_type="backlog",
                    ),
                    linear_issue_node(
                        identifier="OBT-3",
                        title="Not started",
                        state_name="Todo",
                        state_type="unstarted",
                    ),
                    linear_issue_node(
                        identifier="OBT-4",
                        title="Underway",
                        state_name="In Progress",
                        state_type="started",
                    ),
                    linear_issue_node(
                        identifier="OBT-5",
                        title="Up for review",
                        state_name="In Review",
                        state_type="started",
                    ),
                    linear_issue_node(
                        identifier="OBT-6",
                        title="Finished",
                        state_name="Done",
                        state_type="completed",
                    ),
                ]
            },
            generated_at=GENERATED_AT,
        )

        self.assertEqual(
            ["triage", "backlog", "todo", "progress", "review", "done"],
            [issue.status for issue in graph.issues],
        )


class TestGraphMilestonesForConfig(unittest.TestCase):
    """Tests for resolving a config's milestone names against Linear."""

    def test_orders_milestones_by_linear_sort_order(self) -> None:
        milestones = graph_milestones_for_config(
            config=make_config(),
            linear_milestone_nodes=[
                {"id": PHASE_2_ID, "name": "Phase 2", "sortOrder": 200.0},
                {"id": "other-uuid", "name": "Not in the config", "sortOrder": 50.0},
                {"id": PHASE_1_ID, "name": "Phase 1", "sortOrder": 100.0},
            ],
        )

        self.assertEqual([PHASE_1, PHASE_2], milestones)

    def test_raises_when_the_config_names_a_missing_milestone(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Project display config for \[Test Project\] names milestones that the "
            r"project does not have: \['Phase 2'\]\. Project milestones are: "
            r"\['Phase 1'\]\.$",
        ):
            graph_milestones_for_config(
                config=make_config(),
                linear_milestone_nodes=[
                    {"id": PHASE_1_ID, "name": "Phase 1", "sortOrder": 100.0}
                ],
            )


class TestIssueDependencyGraphYaml(unittest.TestCase):
    """Tests for writing a graph to YAML and reading it back."""

    def test_yaml_round_trip(self) -> None:
        graph = IssueDependencyGraph(
            project_name="Test Project",
            generated_at=GENERATED_AT_STR,
            milestones=[PHASE_1, PHASE_2],
            issues=[
                GraphIssue(
                    issue_id="OBT-4",
                    milestone_short_name="P1",
                    status="done",
                    assignee="Anna Geiduschek",
                    labels=["Region: US_XX"],
                    title="Parse the config",
                ),
                GraphIssue(
                    issue_id="OBT-200",
                    milestone_short_name="P2",
                    status="todo",
                    assignee=None,
                    labels=[],
                    title="Launch the extractor",
                ),
            ],
            edges=[("OBT-4", "OBT-200")],
            labels_excluded_by_default=["Region: US_XX"],
        )

        with tempfile.TemporaryDirectory() as output_dir:
            yaml_path = os.path.join(output_dir, "graph.yaml")
            graph.write_yaml(yaml_path)

            self.assertEqual(graph, IssueDependencyGraph.from_yaml(yaml_path))

    def test_all_labels_collects_every_label_in_the_graph(self) -> None:
        graph = IssueDependencyGraph(
            project_name="Test Project",
            generated_at=GENERATED_AT_STR,
            milestones=[PHASE_1],
            issues=[
                GraphIssue(
                    issue_id="OBT-1",
                    milestone_short_name="P1",
                    status="todo",
                    assignee=None,
                    labels=["Team: DSI", "Region: US_XX"],
                    title="One",
                ),
                GraphIssue(
                    issue_id="OBT-2",
                    milestone_short_name="P1",
                    status="todo",
                    assignee=None,
                    labels=["Team: DSI"],
                    title="Two",
                ),
            ],
            edges=[],
            labels_excluded_by_default=[],
        )

        self.assertEqual(["Region: US_XX", "Team: DSI"], graph.all_labels)

    def test_raises_on_an_edge_endpoint_outside_the_graph(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found edge endpoints that are not issues in the graph: \['OBT-999'\]\.$",
        ):
            IssueDependencyGraph(
                project_name="Test Project",
                generated_at=GENERATED_AT_STR,
                milestones=[PHASE_1],
                issues=[
                    GraphIssue(
                        issue_id="OBT-1",
                        milestone_short_name="P1",
                        status="todo",
                        assignee=None,
                        labels=[],
                        title="One",
                    )
                ],
                edges=[("OBT-1", "OBT-999")],
                labels_excluded_by_default=[],
            )

    def test_raises_on_an_issue_in_an_unknown_milestone(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Found issues in milestones missing from the graph: \['P2'\]\.$",
        ):
            IssueDependencyGraph(
                project_name="Test Project",
                generated_at=GENERATED_AT_STR,
                milestones=[PHASE_1],
                issues=[
                    GraphIssue(
                        issue_id="OBT-1",
                        milestone_short_name="P2",
                        status="todo",
                        assignee=None,
                        labels=[],
                        title="One",
                    )
                ],
                edges=[],
                labels_excluded_by_default=[],
            )
