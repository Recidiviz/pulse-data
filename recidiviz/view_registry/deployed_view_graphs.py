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
import itertools
import logging
from functools import cache

from recidiviz.aggregated_metrics.view_config import (
    get_aggregated_metrics_view_builders,
)
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_graph import (
    BigQueryViewGraph,
    BigQueryViewGraphRegistry,
)
from recidiviz.calculator.query.experiments_metadata.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as EXPERIMENTS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.externally_shared_views.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as EXTERNALLY_SHARED_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.view_config import (
    VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE as STATE_VIEW_BUILDERS,
)
from recidiviz.datasets.static_data.views.view_config import (
    get_static_data_view_builders,
)
from recidiviz.ingest.direct.views.view_config import (
    get_view_builders_for_views_to_update as get_direct_ingest_view_builders,
)
from recidiviz.ingest.views.view_config import (
    get_view_builders_for_views_to_update as get_ingest_infra_view_builders,
)
from recidiviz.llm_eval.label_studio.views.view_config import (
    get_view_builders_for_views_to_update as get_label_studio_view_builders,
)
from recidiviz.llm_eval.llmaj.views.view_config import (
    get_view_builders_for_views_to_update as get_llmaj_view_builders,
)
from recidiviz.monitoring.platform_kpis.view_config import (
    get_platform_kpi_views_to_update,
)
from recidiviz.observations.view_config import (
    get_view_builders_for_views_to_update as get_observations_view_builders,
)
from recidiviz.outcome_metrics.view_config import (
    get_transitions_view_builders_for_views_to_update as get_transitions_view_builders,
)
from recidiviz.segment.view_config import (
    get_view_builders_for_views_to_update as get_segment_view_builders,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    collect_source_table_collections_hydrated_outside_view_graphs,
)
from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.task_eligibility.view_config import (
    get_view_builders_for_views_to_update as get_task_eligibility_view_builders,
)
from recidiviz.utils import environment, metadata
from recidiviz.validation.views.view_config import (
    build_validation_metadata_view_builders,
)
from recidiviz.validation.views.view_config import (
    get_view_builders_for_views_to_update as get_validation_view_builders,
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
        view_graphs=_all_view_graphs(),
        candidate_collections=collect_source_table_collections_hydrated_outside_view_graphs(
            project_id
        ),
    )


def _all_view_graphs() -> list[BigQueryViewGraph]:
    """Returns every deployed view graph, unscoped to a project."""
    return [_calculation_view_graph()]


def _calculation_view_graph() -> BigQueryViewGraph:
    """Returns the view graph whose views are updated by the calculation DAG."""
    return BigQueryViewGraph(
        name=CALCULATION_VIEW_GRAPH_NAME,
        input_source_table_update_group=SourceTableUpdateGroup.CALC,
        view_builder_candidates=_calculation_view_builders_across_projects(),
    )


def _calculation_view_builders_across_projects() -> list[BigQueryViewBuilder]:
    """Returns every view builder that is a candidate for the calculation view graph,
    across all projects.
    """
    logging.info("Gathering all calculation view graph builders...")
    return list(
        itertools.chain(
            get_aggregated_metrics_view_builders(),
            get_direct_ingest_view_builders(),
            EXPERIMENTS_VIEW_BUILDERS,
            EXTERNALLY_SHARED_VIEW_BUILDERS,
            get_ingest_infra_view_builders(),
            get_observations_view_builders(),
            get_segment_view_builders(),
            STATE_VIEW_BUILDERS,
            get_task_eligibility_view_builders(),
            get_validation_view_builders(),
            build_validation_metadata_view_builders(),
            get_platform_kpi_views_to_update(),
            get_transitions_view_builders(),
            get_static_data_view_builders(),
            get_label_studio_view_builders(),
            get_llmaj_view_builders(),
        )
    )


@environment.local_only
def builders_for_all_view_graphs_across_projects() -> list[BigQueryViewBuilder]:
    """Returns the view builders for every view that is a candidate for any view graph,
    across all projects. Some of these views do not deploy to a given project based on
    the builder configuration.
    """
    return [b for g in _all_view_graphs() for b in g.view_builder_candidates]


def builders_for_all_deployed_view_graphs() -> list[BigQueryViewBuilder]:
    """Returns the view builders for every view in every view graph deployed to the
    current project.
    """
    return [
        b
        for g in deployed_view_graph_registry(metadata.project_id()).view_graphs
        for b in g.view_builders
    ]


def build_dag_walker_for_all_deployed_view_graphs() -> BigQueryViewDagWalker:
    """Builds a BigQueryViewDagWalker over every view in every view graph deployed
    to the current project.
    """
    return BigQueryViewDagWalker.union_dags(
        *(
            g.build_dag_walker()
            for g in deployed_view_graph_registry(metadata.project_id()).view_graphs
        )
    )
