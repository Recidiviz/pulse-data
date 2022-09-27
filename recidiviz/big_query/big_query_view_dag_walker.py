# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements a class that allows us to walk across a DAG of BigQueryViews
and perform actions on each of them in some order."""
import heapq
import logging
import time
from concurrent import futures
from concurrent.futures import Future
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.utils import structured_logging, trace


@attr.s(frozen=True, kw_only=True)
class DagKey:
    """Dataclass representing a node in the view DAG. Can represent a source data table
    or a view, but will never refer directly to the materialized table associated with
    a BigQueryView. A materialized table is considered part of the same abstract node
    in the dependency graph.
    """

    view_address: BigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddress)
    )

    @property
    def dataset_id(self) -> str:
        return self.view_address.dataset_id

    @property
    def table_id(self) -> str:
        return self.view_address.table_id

    def as_tuple(self) -> Tuple[str, str]:
        """Transforms the key to a Tuple for display purposes."""
        return self.dataset_id, self.table_id

    @classmethod
    def for_view(cls, view: BigQueryView) -> "DagKey":
        return DagKey(view_address=view.address)


ViewResultT = TypeVar("ViewResultT")
ParentResultsT = Dict[BigQueryView, ViewResultT]


@attr.s(auto_attribs=True, kw_only=True)
class ViewProcessingMetadata:
    node_processing_runtime_seconds: float
    total_time_in_queue_seconds: float
    graph_depth: int


@attr.s(auto_attribs=True, kw_only=True)
class ProcessDagResult(Generic[ViewResultT]):
    """Stores results and metadata about a single call to
    BigQueryDagWalker.process_dag().
    """

    view_results: Dict[BigQueryView, ViewResultT]
    view_processing_stats: Dict[BigQueryView, ViewProcessingMetadata]
    total_runtime: float

    def log_processing_stats(self, n_slowest: int) -> None:
        """Logs various stats about a DAG processing run.

        Args:
            n_slowest: (int) The number of slowest nodes to print out.
        """
        if not self.view_processing_stats:
            logging.info("No views processed - no stats to show.")
            return
        nodes_processed = len(self.view_results)
        processing_runtimes = []
        queued_wait_times = []
        for view, metadata in self.view_processing_stats.items():
            processing_time = metadata.node_processing_runtime_seconds
            total_queue_time = metadata.total_time_in_queue_seconds
            processing_runtimes.append((processing_time, view.address))
            queued_wait_times.append(total_queue_time - processing_time)

        avg_wait_time = max(
            0.0, round(sum(queued_wait_times) / len(queued_wait_times), 2)
        )
        max_wait_time = max(0.0, max(queued_wait_times))

        slowest_to_process = heapq.nlargest(n_slowest, processing_runtimes)
        slowest_list = "\n".join(
            [
                f"  {i+1}) {seconds:.2f} sec: {address.dataset_id}.{address.table_id}"
                for i, (seconds, address) in enumerate(slowest_to_process)
            ]
        )

        logging.info(
            "### BQ DAG PROCESSING STATS ###\n"
            "Total processing time: %s sec\n"
            "Nodes processed: %s\n"
            "Average queue wait time: %s seconds\n"
            "Max queue wait time: %s seconds\n"
            "Top [%s] most expensive nodes in DAG: \n%s",
            round(self.total_runtime, 2),
            nodes_processed,
            avg_wait_time,
            max_wait_time,
            n_slowest,
            slowest_list,
        )


class BigQueryViewDagNode:
    """A single node in a BigQuery view DAG, i.e. a single view with relationships to other views."""

    def __init__(self, view: BigQueryView, is_root: bool = False):
        self.view = view
        # Note: Must use add_child_key() to populate this member variable before using.
        self.child_node_keys: Set[DagKey] = set()
        self.materialized_addresss: Optional[Dict[BigQueryAddress, DagKey]] = None
        self.is_root = is_root
        # Note: Must call populate_node_family_for_node() on a node before using this member variable.
        self._node_family: Optional[BigQueryViewDagNodeFamily] = None

        self._view_builder: Optional[BigQueryViewBuilder] = None

    @property
    def node_family(self) -> "BigQueryViewDagNodeFamily":
        if not self._node_family:
            raise ValueError("Must set node_family via set_node_family().")
        return self._node_family

    @property
    def dag_key(self) -> DagKey:
        return DagKey.for_view(self.view)

    @property
    def parent_tables(self) -> Set[BigQueryAddress]:
        """The set of actual tables/views referenced by this view."""
        return self.view.parent_tables

    @property
    def parent_keys(self) -> Set[DagKey]:
        """The set of actual keys to parent DAG nodes for this view."""
        if self.materialized_addresss is None:
            raise ValueError(
                "Must call set_materialized_addresss() before calling parent_keys()."
            )

        parent_keys: Set[DagKey] = set()

        for parent_table_address in self.parent_tables:
            if parent_table_address in self.materialized_addresss:
                parent_keys.add(self.materialized_addresss[parent_table_address])
            else:
                parent_keys.add(DagKey(view_address=parent_table_address))

        return parent_keys

    def add_child_key(self, dag_key: DagKey) -> None:
        self.child_node_keys.add(dag_key)

    def set_materialized_addresss(
        self, materialized_addresss: Dict[BigQueryAddress, DagKey]
    ) -> None:
        self.materialized_addresss = materialized_addresss

    def set_node_family(self, node_family: "BigQueryViewDagNodeFamily") -> None:
        self._node_family = node_family

    @property
    def child_keys(self) -> Set[DagKey]:
        return self.child_node_keys

    @property
    def view_builder(self) -> BigQueryViewBuilder:
        """The view builder associated with this view."""
        if self._view_builder is None:
            raise ValueError("Must set view_builder via set_view_builder().")
        return self._view_builder

    def set_view_builder(self, view_builder: BigQueryViewBuilder) -> None:
        self._view_builder = view_builder


@attr.s
class BigQueryViewDagNodeFamily:
    """Class that holds a DAG node's full child and parent dependencies, as well as string
    representations of those dependency trees.
    Attributes can be populated using the DAG walker's populate_node_family_for_node()"""

    # The set of DagKeys for all views this view relies on (parents, grandparents, etc...)
    full_parentage: Set[DagKey] = attr.ib(default=None)
    # The set of DagKeys for all views that depend on this view (children, grandchildren, etc...)
    full_descendants: Set[DagKey] = attr.ib(default=None)
    # The visual string representation of the full parentage dependency tree.
    parent_dfs_tree_str: str = attr.ib(default=None)
    # The visual string representation of the full descendants dependency tree.
    child_dfs_tree_str: str = attr.ib(default=None)


class BigQueryViewDagWalker:
    """Class implementation that walks a DAG of BigQueryViews."""

    def __init__(self, views: List[BigQueryView]):
        self.views = views
        dag_nodes = [BigQueryViewDagNode(view) for view in views]
        self.nodes_by_key: Dict[DagKey, BigQueryViewDagNode] = {
            node.dag_key: node for node in dag_nodes
        }
        self.materialized_addresss = self._get_materialized_addresss_map()

        self._prepare_dag()
        self.roots = [node for node in self.nodes_by_key.values() if node.is_root]
        self._check_for_cycles()

    def _get_materialized_addresss_map(self) -> Dict[BigQueryAddress, DagKey]:
        """For every view, if it has an associated materialized table, returns a
        dictionary the addresss of those tables to the DagKey for the original view.

        If a view is not materialized, it will not have any entry in this map.
        """
        materialized_addresss: Dict[BigQueryAddress, DagKey] = {}
        for key, node in self.nodes_by_key.items():
            if node.view.materialized_address:
                if node.view.materialized_address in materialized_addresss:
                    raise ValueError(
                        f"Found materialized view address for view [{key.as_tuple()}] "
                        f"that matches materialized_address of another view: "
                        f"[{materialized_addresss[node.view.materialized_address].as_tuple()}]. "
                        f"Two BigQueryViews cannot share the same "
                        f"materialized_address."
                    )
                materialized_address_key = DagKey(
                    view_address=node.view.materialized_address
                )
                if materialized_address_key in self.nodes_by_key:
                    raise ValueError(
                        f"Found materialized view address for view [{key.as_tuple()}] "
                        f"that matches the view address of another view "
                        f"[{materialized_address_key.as_tuple()}]. The "
                        f"materialized_address of a view cannot be the address "
                        f"of another BigQueryView."
                    )

                materialized_addresss[node.view.materialized_address] = key
        return materialized_addresss

    def _prepare_dag(self) -> None:
        """Prepares for processing the full DAG by identifying root nodes and
        associating nodes with their children.
        """
        for key, node in self.nodes_by_key.items():
            node.set_materialized_addresss(self.materialized_addresss)
            node.is_root = True
            for parent_key in node.parent_keys:
                if parent_key in self.nodes_by_key:
                    node.is_root = False
                    self.nodes_by_key[parent_key].add_child_key(key)

    def _check_for_cycles(self) -> None:
        """Raises a ValueError if there are any cycles in the provided DAG."""
        if not self.nodes_by_key:
            return

        if not self.roots:
            raise ValueError("No roots detected. Input views contain a cycle.")

        for node_key in self.nodes_by_key:
            self._check_for_cycles_reachable_from_node(node_key)

    def _check_for_cycles_reachable_from_node(self, start_key: DagKey) -> None:
        """Throws if there is a cycle that can be reached from the provided start node."""
        paths_to_explore: List[Tuple[DagKey, List[DagKey]]] = [(start_key, [])]
        while paths_to_explore:
            key, path = paths_to_explore.pop()

            for child_key in self.nodes_by_key[key].child_node_keys:
                if child_key in path:
                    raise ValueError(
                        f"Detected cycle in graph reachable from "
                        f"{start_key.as_tuple()}: {[k.as_tuple() for k in path]}"
                    )

                paths_to_explore.append((child_key, path + [child_key]))

    def view_for_key(self, dag_key: DagKey) -> BigQueryView:
        return self.nodes_by_key[dag_key].view

    def node_for_view(self, view: BigQueryView) -> BigQueryViewDagNode:
        return self.nodes_by_key[DagKey.for_view(view)]

    def process_dag(
        self, view_process_fn: Callable[[BigQueryView, ParentResultsT], ViewResultT]
    ) -> ProcessDagResult[ViewResultT]:
        """This method provides a level-by-level "breadth-first" traversal of a DAG and executes
        view_process_fn on every node in level order."""
        processed: Set[DagKey] = set()
        queue: Set[BigQueryViewDagNode] = set(self.roots)
        view_results: Dict[BigQueryView, ViewResultT] = {}
        view_processing_stats: Dict[BigQueryView, ViewProcessingMetadata] = {}
        dag_processing_start = time.perf_counter()
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            future_to_context: Dict[
                Future[Tuple[float, ViewResultT]],
                Tuple[BigQueryViewDagNode, ParentResultsT, float],
            ] = {
                executor.submit(
                    _timed_func(structured_logging.with_context(view_process_fn)),
                    node.view,
                    {},
                ): (node, {}, time.perf_counter())
                for node in self.roots
            }
            processing = {node.dag_key for node, _, _ in future_to_context.values()}
            while processing:
                completed, _not_completed = futures.wait(
                    future_to_context.keys(), return_when="FIRST_COMPLETED"
                )
                end = time.perf_counter()
                for future in completed:
                    node, parent_results, entered_queue_time = future_to_context.pop(
                        future
                    )
                    try:
                        execution_sec, view_result = future.result()
                    except Exception as e:
                        # We log an error here rather than doing `raise Error() from e`
                        # so that we maintain the original error format.
                        logging.error(
                            "Exception found fetching results for for view_key: [%s]",
                            node.dag_key,
                        )
                        raise e
                    graph_depth = (
                        0
                        if not parent_results
                        else max(
                            {
                                view_processing_stats[p].graph_depth
                                for p in parent_results
                            }
                        )
                        + 1
                    )
                    view_stats = ViewProcessingMetadata(
                        node_processing_runtime_seconds=execution_sec,
                        total_time_in_queue_seconds=(end - entered_queue_time),
                        graph_depth=graph_depth,
                    )
                    view_results[node.view] = view_result
                    view_processing_stats[node.view] = view_stats
                    processing.remove(node.dag_key)
                    processed.add(node.dag_key)

                    for child_key in node.child_node_keys:
                        child_node = self.nodes_by_key[child_key]
                        if child_node in processed or child_node in queue:
                            raise ValueError(
                                f"Unexpected situation where child node has already been processed: {child_key}"
                            )
                        if child_node in processing:
                            continue

                        parents_all_processed = True
                        parent_results = {}
                        for parent_key in child_node.parent_keys:
                            if (
                                parent_key in self.nodes_by_key
                                and parent_key not in processed
                            ):
                                parents_all_processed = False
                                break
                            if parent_key in self.nodes_by_key:
                                parent_view = self.nodes_by_key[parent_key].view
                                parent_results[parent_view] = view_results[parent_view]
                        if parents_all_processed:
                            if not parent_results:
                                raise ValueError(
                                    f"Expected child node [{child_node.view.address}] "
                                    f"to have parents."
                                )
                            entered_queue_time = time.perf_counter()
                            future = executor.submit(
                                _timed_func(
                                    structured_logging.with_context(view_process_fn)
                                ),
                                child_node.view,
                                parent_results,
                            )
                            future_to_context[future] = (
                                child_node,
                                parent_results,
                                entered_queue_time,
                            )
                            processing.add(child_node.dag_key)
        return ProcessDagResult(
            view_results=view_results,
            view_processing_stats=view_processing_stats,
            total_runtime=(time.perf_counter() - dag_processing_start),
        )

    def _check_sub_dag_input_views(self, *, input_views: List[BigQueryView]) -> None:
        missing_views = set(input_views).difference(self.views)
        if missing_views:
            raise ValueError(f"Found input views not in source DAG: {missing_views}")

    @staticmethod
    def _check_sub_dag_views(
        *, input_views: List[BigQueryView], sub_dag_views: Set[BigQueryView]
    ) -> None:
        missing_views = set(input_views).difference(sub_dag_views)
        if missing_views:
            raise ValueError(
                f"Found input views not represented in the output DAG: {missing_views}"
            )

    def get_descendants_sub_dag(
        self, views: List[BigQueryView]
    ) -> "BigQueryViewDagWalker":
        """Returns a DAG containing only views that are descendants of the list of input
        views. Includes the input views themselves.
        """
        self._check_sub_dag_input_views(input_views=views)

        sub_dag_views: Set[BigQueryView] = set()

        def collect_descendants(
            v: BigQueryView, parent_results: Dict[BigQueryView, bool]
        ) -> bool:
            is_target_view_or_descendant = (
                any(
                    is_parent_target_view_or_descendant
                    for is_parent_target_view_or_descendant in parent_results.values()
                )
                or v in views
            )
            if is_target_view_or_descendant:
                sub_dag_views.add(v)

            return is_target_view_or_descendant

        self.process_dag(collect_descendants)
        self._check_sub_dag_views(input_views=views, sub_dag_views=sub_dag_views)
        sub_dag = BigQueryViewDagWalker(list(sub_dag_views))
        sub_dag.populate_node_view_builders(self.view_builders())
        return sub_dag

    def get_ancestors_sub_dag(
        self,
        views: List[BigQueryView],
    ) -> "BigQueryViewDagWalker":
        """Returns a DAG containing only views that are ancestors of the list of input
        views. Includes the input views themselves.
        """
        self._check_sub_dag_input_views(input_views=views)

        sub_dag_views: Set[BigQueryView] = set()

        def collect_ancestors(
            v: BigQueryView, parent_results: Dict[BigQueryView, Set[BigQueryView]]
        ) -> Set[BigQueryView]:
            # Ancestors of this view are comprised of all ancestors of each parent...
            all_ancestors: Set[BigQueryView] = set().union(*parent_results.values())

            # ...plus the parents themselves
            all_ancestors |= parent_results.keys()

            if v in views:
                sub_dag_views.add(v)
                for ancestor in all_ancestors:
                    sub_dag_views.add(ancestor)

            return all_ancestors

        self.process_dag(collect_ancestors)
        self._check_sub_dag_views(input_views=views, sub_dag_views=sub_dag_views)
        sub_dag = BigQueryViewDagWalker(list(sub_dag_views))
        sub_dag.populate_node_view_builders(self.view_builders())
        return sub_dag

    @staticmethod
    def union_dags(
        dag_1: "BigQueryViewDagWalker", dag_2: "BigQueryViewDagWalker"
    ) -> "BigQueryViewDagWalker":
        views: List[BigQueryView] = [*{*dag_1.views, *dag_2.views}]
        unioned_dag = BigQueryViewDagWalker(views=views)
        unioned_dag.populate_node_view_builders(
            [*dag_1.view_builders(), *dag_2.view_builders()]
        )
        return unioned_dag

    def get_sub_dag(
        self,
        *,
        views: List[BigQueryView],
        include_ancestors: bool,
        include_descendants: bool,
    ) -> "BigQueryViewDagWalker":
        """Returns a BigQueryDagWalker that represents the sub-portion of the view graph
         that includes all views in |views|.

        If |get_ancestors| is True, includes all ancestor views of |views|.
        If |get_descendants| is True, includes all views that are descendant from the
        |views|.
        """
        sub_dag_walker = BigQueryViewDagWalker(views)
        sub_dag_walker.populate_node_view_builders(self.view_builders())

        # If necessary, get descendants of views_in_sub_dag
        if include_descendants:
            sub_dag_walker = BigQueryViewDagWalker.union_dags(
                sub_dag_walker,
                self.get_descendants_sub_dag(views),
            )

        # If necessary, get ancestor views of views_in_sub_dag
        if include_ancestors:
            sub_dag_walker = BigQueryViewDagWalker.union_dags(
                sub_dag_walker, self.get_ancestors_sub_dag(views)
            )

        return sub_dag_walker

    def populate_node_family_for_node(
        self,
        node: BigQueryViewDagNode,
        datasets_to_skip: Optional[Set[str]] = None,
        custom_node_formatter: Optional[Callable[[DagKey], str]] = None,
        view_source_table_datasets: Optional[Set[str]] = None,
    ) -> None:
        """Populates the BigQueryViewDagNodeFamily for a given node. This includes the
         full set of all parent nodes, the full set of all child nodes, and the string
        representations of those dependency trees. If |view_source_table_datasets| are
        specified, we stop searching for parent nodes when we hit a source dataset.
        """

        def _get_one_way_dependencies(
            descendants: bool = False,
        ) -> Tuple[Set[DagKey], str]:
            """Returns a set of all dependent DagKeys in one direction, and a string
             representation of that tree.

            If |descendants| is True, returns info about the tree of views that are
            dependent on the view. If |descendants| is False, returns info about the
            tree of all views that this view depends on."""
            stack = [
                (
                    DagKey(
                        view_address=BigQueryAddress(
                            dataset_id=node.view.dataset_id, table_id=node.view.table_id
                        )
                    ),
                    0,
                )
            ]
            tree = ""
            full_dependencies: Set[DagKey] = set()
            while len(stack) > 0:
                dag_key, tabs = stack.pop()
                if not datasets_to_skip or dag_key.dataset_id not in datasets_to_skip:
                    table_name = (
                        custom_node_formatter(dag_key)
                        if custom_node_formatter
                        else f"{dag_key.dataset_id}.{dag_key.table_id}"
                    )
                    tree += ("|" if tabs else "") + ("--" * tabs) + table_name + "\n"

                # Stop if we reached a source view
                if (
                    view_source_table_datasets
                    and not descendants
                    and dag_key.dataset_id in view_source_table_datasets
                ):
                    continue

                curr_node = self.nodes_by_key.get(dag_key)
                if curr_node:
                    next_related_keys = (
                        curr_node.child_keys if descendants else curr_node.parent_keys
                    )

                    for related_key in sorted(
                        next_related_keys,
                        key=lambda key: (key.dataset_id, key.table_id),
                        reverse=descendants,
                    ):
                        full_dependencies.add(related_key)
                        stack.append(
                            (
                                related_key,
                                # We don't add a tab if we are skipping a view
                                tabs
                                if datasets_to_skip
                                and dag_key.dataset_id in datasets_to_skip
                                else tabs + 1,
                            )
                        )
            return full_dependencies, tree

        full_parentage, parent_tree = _get_one_way_dependencies()
        full_descendants, child_tree = _get_one_way_dependencies(descendants=True)

        node.set_node_family(
            BigQueryViewDagNodeFamily(
                full_parentage=full_parentage,
                parent_dfs_tree_str=parent_tree,
                full_descendants=full_descendants,
                child_dfs_tree_str=child_tree,
            )
        )

    def populate_node_view_builders(
        self, all_candidate_view_builders: Sequence[BigQueryViewBuilder]
    ) -> None:
        """Populates all view nodes on the DAG with their associated view builder.
        The provided list of view builders must be a superset of all builders for views
        in this DAG.
        """
        builders_by_key = {
            DagKey(view_address=b.address): b for b in all_candidate_view_builders
        }
        for key, node in self.nodes_by_key.items():
            if key not in builders_by_key:
                raise ValueError(f"Builder not found for view [{key.view_address}]")
            builder = builders_by_key[key]
            node.set_view_builder(builder)

    def view_builders(self) -> Sequence[BigQueryViewBuilder]:
        """Returns all view builders for the nodes in this views DAG. Can only be
        called after populate_node_view_builders() is called on this BigQueryDagWalker.
        """
        return [node.view_builder for node in self.nodes_by_key.values()]


def _timed_func(
    func: Callable[[BigQueryView, ParentResultsT], ViewResultT]
) -> Callable[[BigQueryView, ParentResultsT], Tuple[float, ViewResultT]]:
    """Wraps a DAG node processing function in a function that will return both the
    processing result and the execution time.
    """

    def _wrapper(
        view: BigQueryView, parent_results: ParentResultsT
    ) -> Tuple[float, ViewResultT]:
        start = time.perf_counter()
        res = trace.span(func)(view, parent_results)
        end = time.perf_counter()
        return (end - start), res

    return _wrapper
