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

from collections import defaultdict

import attr
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_graph import (
    BigQueryViewGraph,
    ResolvedBigQueryViewGraph,
)
from recidiviz.common import attr_validators
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
    SourceTableUpdateGroup,
)
from recidiviz.utils.graph_algorithms import find_cycle
from recidiviz.utils.list_helpers import flatten_values_for_keys, group_by


def view_derived_source_table_collection(
    dataset_id: str,
    writer_update_group: SourceTableUpdateGroup,
    reader_update_groups: set[SourceTableUpdateGroup],
    source_tables_by_address: dict[BigQueryAddress, SourceTableConfig],
) -> SourceTableCollection:
    """Returns a source table collection for a boundary dataset, whose tables are
    derived from the views the graph in |writer_update_group| materializes into it.
    """
    is_read_across_dags = bool(reader_update_groups - {writer_update_group})
    # All view materialized collections are technically regenerable. However, a schema
    # update task will regenerate a materialized table if the schema changes. When the
    # table is only read and written by a single DAG, this is fine because the re-materialization
    # will happen before any downstream readers access it. If it is read by one
    # (or more) DAGs and written by another, there is no guarantee that the table will be regenerated
    # before the downstream readers access it, so the table could still be empty when read. Instead,
    # such tables are protected() to prevent accidental data loss.
    update_config = (
        SourceTableCollectionUpdateConfig.protected()
        if is_read_across_dags
        else SourceTableCollectionUpdateConfig.regenerable()
    )
    return SourceTableCollection(
        dataset_id=dataset_id,
        description=f"View-derived source tables materialized into [{dataset_id}].",
        update_config=update_config,
        # The update group of every graph that reads this dataset: the DAGs whose
        # tasks consume these tables, so this source table collection must be updated
        # whenever a reading graph's DAG runs.
        update_groups=reader_update_groups,
        source_tables_by_address=source_tables_by_address,
    )


def _map_datasets_to_writer_reader_graphs(
    view_graphs: list[BigQueryViewGraph],
) -> tuple[dict[str, list[BigQueryViewGraph]], dict[str, list[BigQueryViewGraph]]]:
    """Returns a tuple of two dictionaries:
    - The first maps dataset IDs to the graphs that materialize into them.
    - The second maps dataset IDs to the graphs that read from them.
    """
    writer_graphs_by_dataset: dict[str, list[BigQueryViewGraph]] = defaultdict(list)
    reader_graphs_by_dataset: dict[str, list[BigQueryViewGraph]] = defaultdict(list)
    for graph in view_graphs:
        for dataset_id in graph.output_datasets:
            writer_graphs_by_dataset[dataset_id].append(graph)
        for dataset_id in graph.root_datasets:
            reader_graphs_by_dataset[dataset_id].append(graph)
    return dict(writer_graphs_by_dataset), dict(reader_graphs_by_dataset)


def _assert_single_writer_for_datasets(
    writer_graphs_by_dataset: dict[str, list[BigQueryViewGraph]],
) -> None:
    """Raises if any materialized dataset is written by graphs spanning more than
    one update group. Two DAGs co-owning a dataset's contents is not a
    configuration we support, since it becomes impossible to reason about when the
    dataset is updated.
    """
    for dataset_id, writer_graphs in writer_graphs_by_dataset.items():
        writer_update_groups = {
            graph.input_source_table_update_group for graph in writer_graphs
        }
        if len(writer_update_groups) > 1:
            raise ValueError(
                f"Dataset [{dataset_id}] is materialized into by graphs in "
                f"multiple update groups {sorted(g.value for g in writer_update_groups)}. "
                f"A dataset must be written by graphs in a single update group."
            )


def _writer_update_group_for_boundary_dataset(
    writer_graphs: list[BigQueryViewGraph],
) -> SourceTableUpdateGroup:
    """Returns the update group shared by every graph that materializes into the
    boundary dataset. Assumes _assert_single_writer_for_datasets has already
    verified the writers share a single update group.
    """
    return one({graph.input_source_table_update_group for graph in writer_graphs})


_VIEW_GRAPH_NODE_PREFIX = "view_graph:"
_DATASET_NODE_PREFIX = "dataset:"


def _assert_view_graphs_acyclic(
    writer_graphs_by_dataset: dict[str, list[BigQueryViewGraph]],
    reader_graphs_by_dataset: dict[str, list[BigQueryViewGraph]],
) -> None:
    """Raises if the graph-of-graphs has a dataset-level cycle. An inter-graph edge exists when
    one graph reads a dataset another graph materializes into. The graph-of-graphs is modeled
    as graph of view graphs and datasets. Ex:

    "view_graph:llm" -> dataset:llm_outputs -> "view_graph:calc"

    A real cycle is a table-level property, so this dataset-level check is stricter than necessary.
    It can reject for example:

    graph_a: source_dataset.table_a -> target_dataset.table_b
    graph_b: target_dataset.table_b -> source_dataset.table_b

    which is not a true cycle.

    That over-strictness is fine because a graph resolves its inputs at dataset granularity, so we
    never want two graphs to depend on each other's outputs in the same dataset regardless of
    which individual tables are involved.
    """
    edges: dict[str, set[str]] = defaultdict(set)
    for dataset_id, readers in reader_graphs_by_dataset.items():
        dataset_node = f"{_DATASET_NODE_PREFIX}[{dataset_id}]"
        for reader in readers:
            for writer in writer_graphs_by_dataset.get(dataset_id, []):
                # A graph that reads a dataset it also materializes into is not a
                # cross-graph dependency, so it never forms a cycle; skip the
                # self-edge that would otherwise trip find_cycle.
                if reader.name != writer.name:
                    edges[f"{_VIEW_GRAPH_NODE_PREFIX}[{reader.name}]"].add(dataset_node)
                    edges[dataset_node].add(f"{_VIEW_GRAPH_NODE_PREFIX}[{writer.name}]")
    if cycle := find_cycle(edges):
        raise ValueError(f"View graphs form a cycle: {' -> '.join(cycle)}.")


@attr.define(frozen=True, kw_only=True)
class BigQueryViewGraphRegistry:
    """The collection of view graphs defined within a single project."""

    project_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The project every registered graph is resolved for."""

    view_graphs: list[ResolvedBigQueryViewGraph] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(ResolvedBigQueryViewGraph),
        ]
    )
    """All registered view graphs."""

    view_derived_source_table_collections: list[SourceTableCollection] = attr.ib(
        validator=attr_validators.is_list_of(SourceTableCollection)
    )
    """Source table collections for the boundary datasets one graph materializes
    into and another reads from, whose tables are derived from the writing graph's
    outputs. Empty when no dataset is both written and read across graphs.
    """

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
        """Resolves each view graph's input source table collections and returns
        the registry.

        A graph's input source tables come from two places: the
        |candidate_source_table_collections| (plain source tables hydrated outside
        any graph, e.g. raw data or ingest output),
        and boundary datasets — datasets one graph materializes into and another
        reads from, whose tables are derived from the writing graph's outputs.

        For each boundary dataset, builds a view-derived source table collection
        that is then attached to each graph that reads from that dataset.
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

        (
            writer_graphs_by_dataset,
            reader_graphs_by_dataset,
        ) = _map_datasets_to_writer_reader_graphs(view_graphs)
        # TODO(OBT-51674): Re-enable cross-graph cycle detection. This check is
        # dataset-granular, so it reports a cycle now that the calculation
        # graph both writes normalized_state (read by the extraction graphs) and
        # reads us_xx_document_extraction_results (written by the extraction graphs) —
        # two different datasets, so there is no real table-level cycle (the combined
        # view DAG is acyclic). Restore this once the calculation graph is split so
        # cross-graph edges no longer loop at dataset granularity.
        # _assert_view_graphs_acyclic(writer_graphs_by_dataset, reader_graphs_by_dataset)
        _assert_single_writer_for_datasets(writer_graphs_by_dataset)

        # A boundary dataset is materialized into by some graph and read by some graph
        # (possibly the same graph). Sorted for test determinism.
        boundary_datasets = sorted(
            set(writer_graphs_by_dataset) & set(reader_graphs_by_dataset)
        )
        derived_source_table_collections = [
            view_derived_source_table_collection(
                dataset_id=dataset_id,
                writer_update_group=_writer_update_group_for_boundary_dataset(
                    writer_graphs_by_dataset[dataset_id]
                ),
                reader_update_groups={
                    graph.input_source_table_update_group
                    for graph in reader_graphs_by_dataset[dataset_id]
                },
                # One source table config per materialized view that any graph writes to this dataset
                source_tables_by_address={
                    config.address: config
                    for writer in writer_graphs_by_dataset[dataset_id]
                    for config in writer.output_source_table_configs_by_dataset[
                        dataset_id
                    ]
                },
            )
            for dataset_id in boundary_datasets
        ]
        # TODO(OBT-50580) One we remove all _DATASETS_WITH_MULTIPLE_COLLECTIONS this can
        # be a dict[str, SourceTableCollection]
        source_table_collections_by_dataset: dict[
            str, list[SourceTableCollection]
        ] = group_by(
            items=[
                *candidate_source_table_collections,
                *derived_source_table_collections,
            ],
            key_fn=lambda c: c.dataset_id,
        )

        resolved_graphs = [
            ResolvedBigQueryViewGraph(
                view_graph=graph,
                input_source_table_collections=cls._resolve_input_collections(
                    graph, source_table_collections_by_dataset
                ),
            )
            for graph in view_graphs
        ]

        return cls(
            project_id=project_id,
            view_graphs=resolved_graphs,
            view_derived_source_table_collections=derived_source_table_collections,
        )

    @staticmethod
    def _resolve_input_collections(
        graph: BigQueryViewGraph,
        source_table_collections_by_dataset: dict[str, list[SourceTableCollection]],
    ) -> list[SourceTableCollection]:
        """Returns the source table collections for the datasets |graph| reads,
        raising if any read dataset has neither a plain candidate collection nor a
        graph that materializes into it.
        """
        if unregistered_datasets := graph.root_datasets - set(
            source_table_collections_by_dataset
        ):
            raise ValueError(
                f"Graph [{graph.name}] reads datasets {sorted(unregistered_datasets)} "
                f"that no source table collection provides and no view graph "
                f"materializes into. Add a source table collection for them to "
                f"collect_source_table_collections_hydrated_outside_view_graphs, or "
                f"remove the views that reference them."
            )
        return flatten_values_for_keys(
            # Sort for test determinism
            keys=sorted(graph.root_datasets),
            values_by_key=source_table_collections_by_dataset,
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
