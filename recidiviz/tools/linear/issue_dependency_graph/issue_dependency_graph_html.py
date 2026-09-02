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
"""Renders an issue dependency graph as a single self-contained HTML page."""
import json
import os
from pathlib import Path

from recidiviz.tools.linear import (
    issue_dependency_graph as issue_dependency_graph_module,
)
from recidiviz.tools.linear.issue_dependency_graph.issue_dependency_graph import (
    IssueDependencyGraph,
)

TEMPLATE_FILE_NAME = "issue_dependency_graph_template.html"

# Placeholders the template declares, each replaced with a JSON literal. The
# render fails if any placeholder survives, so a renamed one cannot ship a page
# with a syntax error in its inlined data.
_PROJECT_NAME_PLACEHOLDER = "__PROJECT_NAME__"
_PROJECT_NAME_JSON_PLACEHOLDER = "__PROJECT_NAME_JSON__"
_MILESTONES_JSON_PLACEHOLDER = "__MILESTONES_JSON__"
_ISSUES_JSON_PLACEHOLDER = "__ISSUES_JSON__"
_EDGES_JSON_PLACEHOLDER = "__EDGES_JSON__"
_ALL_LABELS_JSON_PLACEHOLDER = "__ALL_LABELS_JSON__"
_LABELS_EXCLUDED_BY_DEFAULT_JSON_PLACEHOLDER = "__LABELS_EXCLUDED_BY_DEFAULT_JSON__"
_GENERATED_AT_PLACEHOLDER = "__GENERATED_AT__"


def template_path() -> str:
    """Returns the path of the HTML template this module fills in."""
    return os.path.join(
        os.path.dirname(issue_dependency_graph_module.__file__), TEMPLATE_FILE_NAME
    )


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False)


def render_html(graph: IssueDependencyGraph) -> str:
    """Returns the self-contained HTML page for |graph|.

    The page carries the whole graph inline as JSON, so it opens from the local
    filesystem with no server and no network access.
    """
    with open(template_path(), encoding="utf-8") as f:
        template = f.read()

    replacements = {
        _PROJECT_NAME_PLACEHOLDER: graph.project_name,
        _PROJECT_NAME_JSON_PLACEHOLDER: _to_json(graph.project_name),
        _MILESTONES_JSON_PLACEHOLDER: _to_json(
            [
                {"shortName": milestone.short_name, "name": milestone.name}
                for milestone in graph.milestones
            ]
        ),
        _ISSUES_JSON_PLACEHOLDER: _to_json(
            [
                {
                    "id": issue.issue_id,
                    "milestone": issue.milestone_short_name,
                    "status": issue.status,
                    "assignee": issue.assignee,
                    "labels": issue.labels,
                    "title": issue.title,
                }
                for issue in graph.issues
            ]
        ),
        _EDGES_JSON_PLACEHOLDER: _to_json(
            [[blocker, blocked] for blocker, blocked in graph.edges]
        ),
        _ALL_LABELS_JSON_PLACEHOLDER: _to_json(graph.all_labels),
        _LABELS_EXCLUDED_BY_DEFAULT_JSON_PLACEHOLDER: _to_json(
            graph.labels_excluded_by_default
        ),
        _GENERATED_AT_PLACEHOLDER: graph.generated_at,
    }
    for placeholder, value in replacements.items():
        if placeholder not in template:
            raise ValueError(
                f"Template [{template_path()}] does not contain placeholder "
                f"[{placeholder}]."
            )
        template = template.replace(placeholder, value)
    return template


def write_html(graph: IssueDependencyGraph, html_path: str | Path) -> None:
    """Writes the rendered page for |graph| to |html_path|."""
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(render_html(graph))
