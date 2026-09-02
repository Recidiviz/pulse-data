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
"""The registry of view graphs deployed to our GCP projects."""
from functools import cache

from recidiviz.big_query.big_query_view_graph import (
    BigQueryViewGraph,
    BigQueryViewGraphRegistry,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    collect_source_table_collections_hydrated_outside_view_graphs,
)
from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.utils import metadata

# TODO(OBT-47830): Move the calculation graph's view builder roster into this module
#  so that it no longer needs the private import.
from recidiviz.view_registry.deployed_views import (  # pylint: disable=protected-access
    _all_view_builders_across_projects,
)

CALCULATION_VIEW_GRAPH_NAME = "calculation"


@cache
def deployed_view_graph_registry(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> BigQueryViewGraphRegistry:
    """Returns the registry of all view graphs deployed to the given project."""
    if project_id != metadata.project_id():
        raise ValueError(
            f"Expected project_id [{project_id}] to match the current project "
            f"[{metadata.project_id()}]."
        )
    return BigQueryViewGraphRegistry.build(
        project_id=project_id,
        view_graphs=[_calculation_view_graph()],
        candidate_collections=collect_source_table_collections_hydrated_outside_view_graphs(
            project_id
        ),
    )


def _calculation_view_graph() -> BigQueryViewGraph:
    """Returns the view graph whose views are updated by the calculation DAG."""
    return BigQueryViewGraph(
        name=CALCULATION_VIEW_GRAPH_NAME,
        input_source_table_update_group=SourceTableUpdateGroup.CALC,
        view_builder_candidates=_all_view_builders_across_projects(),
    )
