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
"""Draws a Linear project's issue dependency graph as a browsable HTML page.

Reads a checked-in project display config, pulls each included milestone's issues
and blocking relations from Linear, writes a human-readable YAML file, then
renders that YAML into a self-contained HTML page and opens it in a browser.

The page draws one box per milestone, one node per issue, and one arrow per
blocking relation. It offers a "Labels to exclude" dropdown, a "Show Labels"
toggle, a "Hide Done" toggle, and a "Treat In Review as Done" toggle.

Usage:
    # Fetch from Linear, write the YAML and the HTML, then open the page.
    uv run python -m recidiviz.tools.linear.generate_issue_dependency_graph_html \\
        --config case_note_insights

    # Re-render the page from an already-written (or hand-edited) YAML file.
    # Makes no Linear call.
    uv run python -m recidiviz.tools.linear.generate_issue_dependency_graph_html \\
        --config case_note_insights --from-yaml

Configs live in recidiviz/tools/linear/issue_dependency_graph/
project_display_configs/. Name one on the command line without its `.yaml`
suffix. Add a config there to graph another project.

Authenticates with the Linear API key in Secret Manager, which needs Google
secrets access. Pass --api-key to use a personal Linear API key instead.
"""
import argparse
import datetime
import logging
import os
import tempfile
import webbrowser
from pathlib import Path

from recidiviz.issue_tracking.linear.linear_client import (
    LinearClient,
    linear_client_from_secret,
)
from recidiviz.tools.linear.issue_dependency_graph.issue_dependency_graph import (
    IssueDependencyGraph,
    build_graph,
    graph_milestones_for_config,
)
from recidiviz.tools.linear.issue_dependency_graph.issue_dependency_graph_html import (
    write_html,
)
from recidiviz.tools.linear.issue_dependency_graph.project_display_config import (
    CONFIG_FILE_SUFFIX,
    ProjectDisplayConfig,
    config_path_for_name,
    project_display_configs_dir,
)
from recidiviz.tools.utils.script_helpers import requires_google_adc
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DEFAULT_OUTPUT_DIR = Path(tempfile.gettempdir()) / "linear_issue_dependency_graph"

# The Linear API key is the same in both projects, and this tool only reads, so it
# always pulls the staging copy.
SECRET_PROJECT_ID = GCP_PROJECT_STAGING

logger = logging.getLogger(__name__)


def available_config_names() -> list[str]:
    """Returns the name of every checked-in project display config, sorted."""
    return sorted(
        file_name[: -len(CONFIG_FILE_SUFFIX)]
        for file_name in os.listdir(project_display_configs_dir())
        if file_name.endswith(CONFIG_FILE_SUFFIX)
    )


def fetch_graph_from_linear(
    *, client: LinearClient, config: ProjectDisplayConfig
) -> IssueDependencyGraph:
    """Returns the dependency graph for the project the config names, read live
    from Linear."""
    project_id = client.get_project_id_by_name(project_name=config.project_name)
    milestones = graph_milestones_for_config(
        config=config,
        linear_milestone_nodes=client.get_project_milestones(project_id=project_id),
    )
    issue_nodes_by_milestone_id = {}
    for milestone in milestones:
        nodes = client.get_issues_for_milestone(milestone_id=milestone.milestone_id)
        logger.info("Fetched %d issues for milestone [%s]", len(nodes), milestone.name)
        issue_nodes_by_milestone_id[milestone.milestone_id] = nodes

    return build_graph(
        config=config,
        milestones=milestones,
        issue_nodes_by_milestone_id=issue_nodes_by_milestone_id,
        generated_at=datetime.datetime.now(datetime.timezone.utc),
    )


@requires_google_adc
def generate_issue_dependency_graph_html(
    *,
    config_name: str,
    output_dir: Path,
    from_yaml: bool,
    open_in_browser: bool,
    api_key: str | None,
) -> Path:
    """Writes the YAML and HTML files for |config_name| into |output_dir| and
    returns the path of the HTML file.

    Reads the graph from Linear unless |from_yaml| is set, in which case it
    re-renders the HTML from the YAML file already in |output_dir|.
    """
    config = ProjectDisplayConfig.from_yaml(config_path_for_name(config_name))
    output_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = output_dir / f"{config_name}.yaml"
    html_path = output_dir / f"{config_name}.html"

    if from_yaml:
        graph = IssueDependencyGraph.from_yaml(yaml_path)
        print(f"Read graph from {yaml_path} (generated {graph.generated_at})")
    else:
        if api_key:
            client = LinearClient(api_key)
        else:
            with local_project_id_override(SECRET_PROJECT_ID):
                client = linear_client_from_secret()
        print(f"Fetching [{config.project_name}] issues from Linear…")
        graph = fetch_graph_from_linear(client=client, config=config)
        graph.write_yaml(yaml_path)
        print(
            f"Wrote {yaml_path} ({len(graph.issues)} issues, {len(graph.edges)} "
            f"dependencies)"
        )

    write_html(graph, html_path)
    print(f"Wrote {html_path}")
    if open_in_browser:
        webbrowser.open_new_tab(html_path.as_uri())
    return html_path


def _parse_arguments() -> argparse.Namespace:
    """Returns the parsed command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--config",
        required=True,
        choices=available_config_names(),
        help="Name of the project display config to render, without its .yaml suffix.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write the YAML and HTML files to. Defaults to "
        f"{DEFAULT_OUTPUT_DIR}.",
    )
    parser.add_argument(
        "--from-yaml",
        action="store_true",
        help="Re-render the HTML from the YAML file already in the output "
        "directory. Makes no Linear call, so hand-edits to that file survive.",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Write the files without opening the page in a browser.",
    )
    parser.add_argument(
        "--api-key",
        help=f"A personal Linear API key. Defaults to the key in the "
        f"{SECRET_PROJECT_ID} Secret Manager, which needs Google secrets access.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_arguments()
    generate_issue_dependency_graph_html(
        config_name=args.config,
        output_dir=args.output_dir,
        from_yaml=args.from_yaml,
        open_in_browser=not args.no_open,
        api_key=args.api_key,
    )
