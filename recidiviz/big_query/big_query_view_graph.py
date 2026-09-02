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
updated together, ResolvedBigQueryViewGraph, that graph resolved to a project
and set of input source table collections, and BigQueryViewGraphRegistry, the collection
of all view graphs in one project.
"""
import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_utils import build_views_to_update
from recidiviz.common import attr_validators
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableUpdateGroup,
)
from recidiviz.utils.types import assert_type


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


@attr.define(frozen=True, kw_only=True)
class BigQueryViewGraphRegistry:
    """The collection of view graphs defined within a single project. Enforces
    that graph names are unique and that no view or materialized address belongs
    to more than one graph.
    """

    project_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The project every registered graph is resolved for."""

    view_graphs: list[ResolvedBigQueryViewGraph] = attr.ib(
        validator=attr_validators.is_list_of(ResolvedBigQueryViewGraph)
    )
    """All registered view graphs."""

    _view_graphs_by_name: dict[str, ResolvedBigQueryViewGraph] = attr.ib(init=False)
    """View graphs keyed by graph name."""

    @_view_graphs_by_name.default
    def _build_view_graphs_by_name(self) -> dict[str, ResolvedBigQueryViewGraph]:
        """Returns view_graphs keyed by name, raising if two graphs share a name."""
        view_graphs_by_name: dict[str, ResolvedBigQueryViewGraph] = {}
        for graph in self.view_graphs:
            if graph.name in view_graphs_by_name:
                raise ValueError(
                    f"Found more than one view graph with name [{graph.name}]"
                )
            view_graphs_by_name[graph.name] = graph
        return view_graphs_by_name

    def __attrs_post_init__(self) -> None:
        """Raises if any graph is resolved for a different project, or if any view
        or materialized address appears in more than one graph.
        """
        for graph in self.view_graphs:
            if graph.project_id != self.project_id:
                raise ValueError(
                    f"View graph [{graph.name}] is resolved for project "
                    f"[{graph.project_id}] but the registry is for project "
                    f"[{self.project_id}]"
                )

        graph_name_by_address: dict[BigQueryAddress, str] = {}
        for graph in self.view_graphs:
            for builder in graph.view_builders:
                addresses = [builder.address]
                if builder.materialized_address:
                    addresses.append(builder.materialized_address)
                for address in addresses:
                    if address in graph_name_by_address:
                        raise ValueError(
                            f"Address [{address.to_str()}] in view graph "
                            f"[{graph.name}] already belongs to view graph "
                            f"[{graph_name_by_address[address]}]"
                        )
                    graph_name_by_address[address] = graph.name

    @classmethod
    def build(
        cls,
        *,
        project_id: str,
        view_graphs: list[BigQueryViewGraph],
        candidate_collections: list[SourceTableCollection],
    ) -> "BigQueryViewGraphRegistry":
        """Resolves each graph for project_id and registers the results. A graph's
        inputs are the candidate collections whose update groups include the
        graph's input_source_table_update_group.
        """
        # TODO(OBT-44681): Resolve each graph's inputs from the datasets its views
        #  actually reference, and derive the collections for tables that one
        #  graph materializes and another reads.
        return cls(
            project_id=project_id,
            view_graphs=[
                ResolvedBigQueryViewGraph(
                    project_id=project_id,
                    view_graph=graph,
                    input_source_table_collections=[
                        c
                        for c in candidate_collections
                        if graph.input_source_table_update_group
                        in assert_type(c.update_groups, set)
                    ],
                )
                for graph in view_graphs
            ],
        )

    def graph_for_name(self, name: str) -> ResolvedBigQueryViewGraph:
        """Returns the graph with this name, raising if none exists."""
        if name not in self._view_graphs_by_name:
            raise ValueError(f"Found no view graph with name [{name}]")
        return self._view_graphs_by_name[name]
