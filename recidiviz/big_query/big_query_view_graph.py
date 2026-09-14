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
"""Defines BigQueryViewGraph, a project-agnostic named set of views that are
updated together, and ResolvedBigQueryViewGraph, that graph resolved to a project
and set of input source table collections.
"""

from collections import defaultdict

import attr

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_utils import build_views_to_update
from recidiviz.common import attr_validators
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableConfig,
    SourceTableUpdateGroup,
)


@attr.define(frozen=True, kw_only=True)
class BigQueryViewGraph:
    """A named set of candidate view builders, across all projects, plus the
    update group of the DAG that updates them.
    """

    name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name that uniquely identifies this graph across all view graphs."""

    view_builder_candidates: list[BigQueryViewBuilder] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(BigQueryViewBuilder),
        ]
    )
    """Builders for every view in this graph in any project. A builder may be
    configured to deploy only in some projects.
    """

    input_source_table_update_group: SourceTableUpdateGroup = attr.ib(
        validator=attr.validators.in_(SourceTableUpdateGroup)
    )
    """The update group this graph's derived input collections carry: the group
    of the DAG whose task updates this graph's views.
    """


@attr.define(frozen=True, kw_only=True)
class ResolvedBigQueryViewGraph:
    """A BigQueryViewGraph scoped to one project, with its view builders filtered
    to that project and its input source table collections resolved. Only
    obtainable from BigQueryViewGraphRegistry.
    """

    project_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The project this graph is resolved for."""

    _view_graph: BigQueryViewGraph = attr.ib(
        validator=attr.validators.instance_of(BigQueryViewGraph)
    )
    """The project-agnostic graph this was resolved from."""

    input_source_table_collections: list[SourceTableCollection] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(SourceTableCollection),
        ]
    )
    """Source table collections whose tables provide inputs to this graph's
    views. A collection may provide inputs to more than one graph.
    """

    view_builders: list[BigQueryViewBuilder] = attr.ib(init=False)
    """Builders for every view in this graph that deploys in project_id."""

    @view_builders.default
    def _filter_view_builders_to_project(self) -> list[BigQueryViewBuilder]:
        """Returns the candidate builders that deploy in project_id, raising if
        there are none.
        """
        view_builders = [
            b
            for b in self._view_graph.view_builder_candidates
            if b.should_deploy_in_project(self.project_id)
        ]
        if not view_builders:
            raise ValueError(
                f"View graph [{self.name}] has no view builders deployed in project "
                f"[{self.project_id}]"
            )
        return view_builders

    @property
    def name(self) -> str:
        return self._view_graph.name

    @property
    def input_source_table_update_group(self) -> SourceTableUpdateGroup:
        return self._view_graph.input_source_table_update_group

    def build_dag_walker(self) -> BigQueryViewDagWalker:
        """Builds a BigQueryViewDagWalker over this graph's views."""
        return BigQueryViewDagWalker(
            list(
                build_views_to_update(
                    candidate_view_builders=self.view_builders, sandbox_context=None
                )
            )
        )

    def build_output_source_table_configs_by_dataset(
        self,
    ) -> dict[str, list[SourceTableConfig]]:
        """Returns this graph's materialized outputs grouped by dataset, as
        SourceTableConfigs. Raises on a partitioned materialized view.
        """
        configs_by_dataset: dict[str, list[SourceTableConfig]] = defaultdict(list)
        for builder in self.view_builders:
            view = builder.build()
            if view.materialized_address is None:
                continue
            # TODO(OBT-47853): Support deriving source tables from time-partitioned
            # materialized views by plumbing time_partitioning into the config.
            if view.time_partitioning is not None:
                raise ValueError(
                    f"Graph [{self.name}] materializes partitioned view "
                    f"[{view.address.to_str()}]; partitioned outputs cannot be "
                    f"derived as source tables."
                )
            configs_by_dataset[view.materialized_address.dataset_id].append(
                SourceTableConfig(
                    address=view.materialized_address,
                    description=view.materialized_table_bq_description,
                    schema_fields=view.bq_schema,
                    clustering_fields=view.clustering_fields or [],
                )
            )
        return dict(configs_by_dataset)
