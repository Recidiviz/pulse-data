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
"""Tests for rendering an issue dependency graph as HTML."""
import re
import unittest

from recidiviz.tools.linear.issue_dependency_graph.issue_dependency_graph import (
    GraphIssue,
    GraphMilestone,
    IssueDependencyGraph,
)
from recidiviz.tools.linear.issue_dependency_graph.issue_dependency_graph_html import (
    render_html,
)

GRAPH = IssueDependencyGraph(
    project_name="Test Project",
    generated_at="2026-08-28T17:05:03+00:00",
    milestones=[
        GraphMilestone(
            milestone_id="11111111-1111-1111-1111-111111111111",
            name="Phase 1",
            short_name="P1",
            sort_order=100.0,
        )
    ],
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
            milestone_short_name="P1",
            status="todo",
            assignee=None,
            labels=[],
            title="Launch the extractor",
        ),
    ],
    edges=[("OBT-4", "OBT-200")],
    labels_excluded_by_default=["Region: US_XX"],
)


class TestRenderHtml(unittest.TestCase):
    """Tests for render_html."""

    def test_render_fills_in_every_placeholder(self) -> None:
        html = render_html(GRAPH)

        self.assertEqual([], re.findall(r"__[A-Z_]+__", html))

    def test_render_inlines_the_graph_data(self) -> None:
        html = render_html(GRAPH)

        self.assertIn('const PROJECT_NAME = "Test Project";', html)
        self.assertIn(
            'const MILESTONES = [{"shortName": "P1", "name": "Phase 1"}];', html
        )
        self.assertIn('const EDGES = [["OBT-4", "OBT-200"]];', html)
        self.assertIn('const ALL_LABELS = ["Region: US_XX"];', html)
        self.assertIn('const LABELS_EXCLUDED_BY_DEFAULT = ["Region: US_XX"];', html)
        self.assertIn(
            '{"id": "OBT-4", "milestone": "P1", "status": "done", '
            '"assignee": "Anna Geiduschek", "labels": ["Region: US_XX"], '
            '"title": "Parse the config"}',
            html,
        )
        self.assertIn(
            '{"id": "OBT-200", "milestone": "P1", "status": "todo", '
            '"assignee": null, "labels": [], "title": "Launch the extractor"}',
            html,
        )
        self.assertIn("2026-08-28T17:05:03+00:00", html)
