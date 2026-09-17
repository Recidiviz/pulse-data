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
"""Defines BigQueryViewGraphRegistry, the collection of all view graphs in one
project.
"""

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_graph import (
    BigQueryViewGraph,
    ResolvedBigQueryViewGraph,
)
from recidiviz.common import attr_validators
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.utils.types import assert_type


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

    _graph_name_by_address: dict[BigQueryAddress, str] = attr.ib(init=False)
    """Maps each view or materialized address to the name of the graph that owns
    it."""

    def __attrs_post_init__(self) -> None:
        """Raises if any graph is resolved for a different project."""
        for graph in self.view_graphs:
            if graph.project_id != self.project_id:
                raise ValueError(
                    f"View graph [{graph.name}] is resolved for project "
                    f"[{graph.project_id}] but the registry is for project "
                    f"[{self.project_id}]"
                )

    @_graph_name_by_address.default
    def _build_graph_name_by_address(self) -> dict[BigQueryAddress, str]:
        """Returns every output address mapped to its owning graph's name, raising
        if any address belongs to more than one graph.
        """
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
        return graph_name_by_address

    @classmethod
    def build(
        cls,
        *,
        project_id: str,
        view_graphs: list[BigQueryViewGraph],
        candidate_source_table_collections: list[SourceTableCollection],
    ) -> "BigQueryViewGraphRegistry":
        """Resolves each graph for project_id and registers the results. A graph's
        inputs are the candidate collections whose update groups include the
        graph's input_source_table_update_group.
        """
        materialized_table_datasets = {
            ds for graph in view_graphs for ds in graph.output_datasets
        }
        externally_hydrated_datasets = {
            c.dataset_id for c in candidate_source_table_collections
        }
        if collisions := materialized_table_datasets & externally_hydrated_datasets:
            raise ValueError(
                f"Datasets {sorted(collisions)} are both materialized into by a view "
                f"graph and present as plain source table collections. A dataset must "
                f"hold either view-derived or plain source tables, not both."
            )
        # TODO(OBT-44681): Resolve each graph's inputs from the datasets its views
        #  actually reference, and derive the collections for tables that one
        #  graph materializes and another reads.
        return cls(
            project_id=project_id,
            view_graphs=[
                ResolvedBigQueryViewGraph(
                    view_graph=graph,
                    input_source_table_collections=[
                        c
                        for c in candidate_source_table_collections
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

    def graph_name_for_address(self, address: BigQueryAddress) -> str | None:
        """Returns the name of the graph that owns this view or materialized
        address, or None if no registered graph owns it.
        """
        return self._graph_name_by_address.get(address)
