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
from collections import deque
from concurrent import futures
from concurrent.futures import Future
from typing import (
    Callable,
    Deque,
    Dict,
    Generic,
    Iterator,
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
from recidiviz.utils import environment, structured_logging, trace


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
class ProcessDagPerfConfig:
    # The max allowed amount of time to process any given node in the DAG.
    node_max_processing_time_seconds: float

    # A mapping of BigQueryAddress to allowed processing runtime for nodes associated
    # with this address. If a value exists for a view in this mapping, it will take
    # precedence over the global |node_max_processing_time_seconds| value.
    node_allowed_process_time_overrides: Dict[BigQueryAddress, float]

    def allowed_processing_time(self, address: BigQueryAddress) -> float:
        return (
            self.node_allowed_process_time_overrides.get(address)
            or self.node_max_processing_time_seconds
        )


DEFAULT_PROCESS_DAG_PERF_CONFIG = ProcessDagPerfConfig(
    # By default, we expect DAG processing to take less than 3 minutes per node.
    node_max_processing_time_seconds=(3 * 60),
    node_allowed_process_time_overrides={},
)


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
        max_wait_time = max(0.0, round(max(queued_wait_times), 2))

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

    def __init__(self, view: BigQueryView, is_root: bool = False, is_leaf: bool = True):
        self.view = view
        # Note: Must use add_child_key() to populate this member variable before using.
        self.child_node_keys: Set[DagKey] = set()
        self.parent_node_keys: Set[DagKey] = set()
        self.materialized_addresss: Optional[Dict[BigQueryAddress, DagKey]] = None
        self.is_root = is_root
        self.is_leaf = is_leaf

        self._view_builder: Optional[BigQueryViewBuilder] = None
        self._ancestors_sub_dag: Optional[BigQueryViewDagWalker] = None
        self._ancestors_tree_num_edges: Optional[int] = None
        self._descendants_sub_dag: Optional[BigQueryViewDagWalker] = None
        self._descendants_tree_num_edges: Optional[int] = None

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

    def add_parent_key(self, dag_key: DagKey) -> None:
        self.parent_node_keys.add(dag_key)

    def set_materialized_addresss(
        self, materialized_addresss: Dict[BigQueryAddress, DagKey]
    ) -> None:
        self.materialized_addresss = materialized_addresss

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

    def set_ancestors_sub_dag(self, ancestor_sub_dag: "BigQueryViewDagWalker") -> None:
        self._ancestors_sub_dag = ancestor_sub_dag

    @property
    def ancestors_sub_dag(self) -> "BigQueryViewDagWalker":
        """A DAG that includes this node and all nodes that are an ancestor of this
        node.
        """
        if not self._ancestors_sub_dag:
            raise ValueError("Must set ancestors_sub_dag via set_ancestors_sub_dag().")
        return self._ancestors_sub_dag

    def set_descendants_sub_dag(
        self, descendants_sub_dag: "BigQueryViewDagWalker"
    ) -> None:
        self._descendants_sub_dag = descendants_sub_dag

    @property
    def descendants_sub_dag(self) -> "BigQueryViewDagWalker":
        """A DAG that includes this node and all nodes that are a descendant of this
        node.
        """
        if not self._descendants_sub_dag:
            raise ValueError(
                "Must set descendants_sub_dag via set_descendants_sub_dag()."
            )
        return self._descendants_sub_dag

    def set_ancestors_tree_num_edges(self, num_edges: int) -> None:
        # pylint: disable=anomalous-backslash-in-string
        """Returns the number of edges in the tree with this node at the root and all
          source tables in the ancestors_sub_dag as leaves. Consider the following
          ancestors DAG for node F:
               A
              /\
            B   C
           / \ /
          D   E
           \ /
            F

        The expanded tree representation of that graph is this tree, which has 8 edges:
         A  A       A
          \  \     /
           B  B   C
           \   \ /
            D   E
             \ /
              F

        This result can be used to interpret the number of lines in a printed DFS
        representation of the ancestors sub_dag.
        """
        self._ancestors_tree_num_edges = num_edges

    @property
    def ancestors_tree_num_edges(self) -> int:
        if self._ancestors_tree_num_edges is None:
            raise ValueError(
                "Must set descendants_sub_dag via set_ancestors_tree_num_edges()."
            )
        return self._ancestors_tree_num_edges

    def set_descendants_tree_num_edges(self, num_edges: int) -> None:
        # pylint: disable=anomalous-backslash-in-string
        """Returns the number of edges in the tree with this node at the root and all
          *leaf* descendant nodes in the ancestors_sub_dag as leaves. Consider the
          following descendants DAG for node A:
               A
              /\
            B   C
           / \ /
          D   E
           \ /
            F

        The expanded tree representation of that graph is this tree, which has 8 edges:
                   A
                  /\
                B   C
               / \   \
              D   E   E
            /      \   \
           F        F   F

        This result can be used to interpret the number of lines in a printed DFS
        representation of the ancestors sub_dag.
        """
        self._descendants_tree_num_edges = num_edges

    @property
    def descendants_tree_num_edges(self) -> int:
        if self._descendants_tree_num_edges is None:
            raise ValueError(
                "Must set descendants_sub_dag via set_descendants_tree_num_edges()."
            )
        return self._descendants_tree_num_edges


class BigQueryViewDagWalker:
    """Class implementation that walks a DAG of BigQueryViews."""

    def __init__(
        self,
        views: List[BigQueryView],
    ):
        self.views = views
        dag_nodes = [BigQueryViewDagNode(view) for view in views]
        self.nodes_by_key: Dict[DagKey, BigQueryViewDagNode] = {
            node.dag_key: node for node in dag_nodes
        }
        self.materialized_addresss = self._get_materialized_addresss_map()

        self._prepare_dag()
        self.roots = [node for node in self.nodes_by_key.values() if node.is_root]
        self.leaves = [node for node in self.nodes_by_key.values() if node.is_leaf]

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
                    parent_node = self.nodes_by_key[parent_key]
                    parent_node.is_leaf = False
                    parent_node.add_child_key(key)
                    node.is_root = False
                    node.add_parent_key(parent_key)

    def _check_for_cycles(self) -> None:
        """
        The textbook implementation of a DFS cycle check might look something like:

            def cycle_check(graph):
                def dfs(graph, vertex, visited, onpath):
                    visited[vertex] = True
                    onpath[vertex] = True
                    for child in graph.childrenof(vertex):
                        if not visited[child]:
                            dfs(graph, child, visited, onpath)
                        else if onpath[child]:
                            raise ValueError
                    onpath[vertex] = False

                visited = [False] * len(graph)
                onpath = [False] * len(graph)
                for vertex in graph:
                    if not visited[vertex]:
                        dfs(graph, vertex, visited, onpath)

        In our implementation, we use an explicit stack instead of implicit
        (recursion). We are interested in the vertex post-order
        (after the recursive calls in the text book example), because only
        after processing all children can we remove a vertex from the current
        path. Thus, with an explicit stack, as we process each child, we keep the parent
        on the stack until all children have been processed, at which point
        it is safe to remove the parent from the path.

        Because we are working with string-based keys we use hashing for
        fast indexing, instead of integer-indexed lists.

        Once a vertex has been explored in a DFS fashion, there is no need
        to process that vertex again when only looking for a cycle.
        """
        if not self.nodes_by_key:
            return

        if not self.roots:
            raise ValueError("No roots detected. Input views contain a cycle.")

        visited = set()
        # Mapping of nodes in the current path to the child for that node.
        current_path_edges: Dict[DagKey, DagKey] = {}
        for start_node in self.nodes_by_key:
            if start_node in visited:
                continue

            stack: Deque[Tuple[DagKey, Deque[DagKey]]] = deque()

            next_node = start_node
            while True:
                if next_node not in visited:
                    child_keys = self.nodes_by_key[next_node].child_node_keys
                    if child_keys:
                        stack.append((next_node, deque(child_keys)))
                    visited.add(next_node)

                if not stack:
                    break

                # Peek at the top of the stack to get current node
                current_node, current_node_children = stack[-1]

                if not current_node_children:
                    # We have explored all children of the node and found no
                    # cycles - pop this node.
                    stack.pop()
                    current_path_edges.pop(current_node)
                    continue

                next_node = current_node_children.pop()
                if next_node in current_path_edges:
                    raise ValueError(
                        f"Detected cycle in graph reachable from "
                        f"{start_node.as_tuple()}: "
                        f"{[e.as_tuple() for e in self._get_cycle_path(start_node, current_path_edges)]}"
                    )
                current_path_edges[current_node] = next_node

    def _get_cycle_path(
        self, start_node_key: DagKey, current_path_edges: Dict[DagKey, DagKey]
    ) -> Iterator[DagKey]:
        key = start_node_key
        while key in current_path_edges:
            key = current_path_edges[key]
            yield key

    def view_for_key(self, dag_key: DagKey) -> BigQueryView:
        return self.nodes_by_key[dag_key].view

    def node_for_view(self, view: BigQueryView) -> BigQueryViewDagNode:
        return self.nodes_by_key[DagKey.for_view(view)]

    def process_dag(
        self,
        view_process_fn: Callable[[BigQueryView, ParentResultsT], ViewResultT],
        perf_config: ProcessDagPerfConfig = DEFAULT_PROCESS_DAG_PERF_CONFIG,
        reverse: bool = False,
    ) -> ProcessDagResult[ViewResultT]:
        """This method provides a level-by-level "breadth-first" traversal of a DAG and
        executes |view_process_fn| on every node in level order.

        Will throw if a node execution time exceeds |max_node_process_time_sec|.
        """
        queue = set(self.leaves) if reverse else set(self.roots)
        processed: Set[DagKey] = set()
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
                    trace.time_and_trace(
                        structured_logging.with_context(view_process_fn)
                    ),
                    node.view,
                    {},
                ): (node, {}, time.perf_counter())
                for node in queue
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

                    allowed_processing_time = perf_config.allowed_processing_time(
                        node.view.address
                    )
                    if execution_sec > allowed_processing_time:
                        error_msg = (
                            f"[BigQueryViewDagWalker Node Failure] Processing for "
                            f"[{node.view.address}] took [{round(execution_sec, 2)}] "
                            f"seconds. Expected node to process in less than "
                            f"[{allowed_processing_time}] seconds."
                        )
                        if environment.in_gcp():
                            # Runtimes can be more unreliable in GCP due to resource
                            # contention with other running processes, so we do not
                            # throw in GCP. We instead emit an error log which can be
                            # used to fire an alert.
                            logging.error(error_msg)
                        else:
                            raise ValueError(error_msg)

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
                    adjacent_keys = (
                        node.parent_node_keys if reverse else node.child_node_keys
                    )
                    for adjacent_key in adjacent_keys:
                        adjacent_node = self.nodes_by_key[adjacent_key]
                        if adjacent_node in processed or adjacent_node in queue:
                            raise ValueError(
                                f"Unexpected situation where adjacent node has already been processed: {adjacent_key}"
                            )
                        if adjacent_node in processing:
                            continue

                        previous_level_all_processed = True
                        previous_level_results = {}
                        previous_level_keys = (
                            adjacent_node.child_keys
                            if reverse
                            else adjacent_node.parent_keys
                        )
                        for previous_level_key in previous_level_keys:
                            if (
                                previous_level_key in self.nodes_by_key
                                and previous_level_key not in processed
                            ):
                                previous_level_all_processed = False
                                break
                            if previous_level_key in self.nodes_by_key:
                                previous_level_view = self.nodes_by_key[
                                    previous_level_key
                                ].view
                                previous_level_results[
                                    previous_level_view
                                ] = view_results[previous_level_view]
                        if previous_level_all_processed:
                            if not previous_level_results:
                                raise ValueError(
                                    f"Expected adjacent node [{adjacent_node.view.address}] "
                                    f"to have previously processed ancesters."
                                )
                            entered_queue_time = time.perf_counter()
                            future = executor.submit(
                                trace.time_and_trace(
                                    structured_logging.with_context(view_process_fn)
                                ),
                                adjacent_node.view,
                                previous_level_results,
                            )
                            future_to_context[future] = (
                                adjacent_node,
                                previous_level_results,
                                entered_queue_time,
                            )
                            processing.add(adjacent_node.dag_key)
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
    def union_dags(*dags: "BigQueryViewDagWalker") -> "BigQueryViewDagWalker":
        found_view_addresses = set()
        views: List[BigQueryView] = []
        view_builders: List[BigQueryViewBuilder] = []
        for dag in dags:
            for view in dag.views:
                if view.address not in found_view_addresses:
                    views.append(view)
                    view_builders.append(dag.node_for_view(view).view_builder)
                    found_view_addresses.add(view.address)

        unioned_dag = BigQueryViewDagWalker(views=views)
        unioned_dag.populate_node_view_builders(view_builders)
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

    def populate_ancestor_sub_dags(self) -> None:
        """Builds and caches ancestor sub-DAGs on all nodes in this DAG."""

        def populate_node_ancestors_sub_dag(
            v: BigQueryView, parent_results: Dict[BigQueryView, None]
        ) -> None:
            """For a given view, calculates the ancestors sub-DAG by unioning together
            the parent ancestor sub-DAGs."""
            this_view_dag = BigQueryViewDagWalker([v])
            this_view_dag.populate_node_view_builders(
                [self.node_for_view(v).view_builder]
            )

            parent_nodes = [
                self.node_for_view(parent_view) for parent_view in parent_results
            ]
            ancestors_sub_dag = self.union_dags(
                this_view_dag,
                *[n.ancestors_sub_dag for n in parent_nodes],
            )

            node = self.node_for_view(view=v)
            node.set_ancestors_sub_dag(ancestors_sub_dag)

            # Include source tables in parent count in addition to parent views
            ancestors_tree_num_edges = len(node.parent_keys) + sum(
                p.ancestors_tree_num_edges for p in parent_nodes
            )
            node.set_ancestors_tree_num_edges(ancestors_tree_num_edges)

        self.process_dag(populate_node_ancestors_sub_dag)

    def populate_descendant_sub_dags(self) -> None:
        """Builds and caches descendant sub-DAGs on all nodes in this DAG."""

        def populate_node_descendants_sub_dag(
            v: BigQueryView, child_results: Dict[BigQueryView, None]
        ) -> None:
            """For a given view, calculates the descendants sub-DAG by unioning together
            the child descendant sub-DAGs."""
            this_view_dag = BigQueryViewDagWalker([v])
            this_view_dag.populate_node_view_builders(
                [self.node_for_view(v).view_builder]
            )

            child_nodes = [
                self.node_for_view(child_view) for child_view in child_results
            ]
            descendants_sub_dag = self.union_dags(
                this_view_dag,
                *[n.descendants_sub_dag for n in child_nodes],
            )

            node = self.node_for_view(view=v)
            node.set_descendants_sub_dag(descendants_sub_dag)

            descendants_tree_num_edges = len(child_results) + sum(
                c.descendants_tree_num_edges for c in child_nodes
            )
            node.set_descendants_tree_num_edges(descendants_tree_num_edges)

        # Process the DAG in the leaves -> roots direction so we process children
        # first.

        self.process_dag(populate_node_descendants_sub_dag, reverse=True)

    def ancestors_dfs_tree_str(
        self,
        view: BigQueryView,
        parent_sort_fn: Callable[[str], str] = lambda s: s,
        custom_node_formatter: Callable[
            [BigQueryAddress], str
        ] = lambda a: f"{a.dataset_id}.{a.table_id}",
        datasets_to_skip: Optional[Set[str]] = None,
    ) -> str:
        # pylint: disable=anomalous-backslash-in-string
        """
        Generate a string representing the dependency graph for ancestors.
        The graph begins at the given view, and ascends through ancestors
        in reverse sorted order with additional indentation at each level.
        Generally, the graph will terminate at a source view (which is not
        a node of the graph itself, but is calculated by taking the difference
        between all parent keys and the parent keys found from the
        process_dag parent results). For example, given node C in the graph
        A     B
         \   /
           C
           |
           D
           |
          ...
        the ancestor tree representation may look something like:
        - C
        - - B
        - - - Source
        - - A
        - - - Source
        The ancestors sub_dag used for the operation is cached in
        the node representing the view during populate_ancestor_sub_dags.
        """

        sub_dag = self.node_for_view(view).ancestors_sub_dag

        def _build_dfs_str(
            v: BigQueryView, parent_results: Dict[BigQueryView, str]
        ) -> str:
            view_parent_addresses = {p.address for p in parent_results}
            node = self.node_for_view(v)
            parent_keys = {key.view_address for key in node.parent_keys}
            source_table_addresses = parent_keys - view_parent_addresses
            return self._build_dfs_str(
                v=v,
                parent_view_dfs_strs=parent_results,
                parent_source_tables=source_table_addresses,
                parent_table_sort_fn=lambda *args: sorted(
                    *args, key=parent_sort_fn, reverse=True
                ),
                node_formatter_fn=custom_node_formatter,
                datasets_to_skip=datasets_to_skip,
            )

        return sub_dag.process_dag(_build_dfs_str).view_results[view] + "\n"

    def descendants_dfs_tree_str(
        self,
        view: BigQueryView,
        parent_sort_fn: Callable[[str], str] = lambda s: s,
        custom_node_formatter: Callable[
            [BigQueryAddress], str
        ] = lambda a: f"{a.dataset_id}.{a.table_id}",
        datasets_to_skip: Optional[Set[str]] = None,
    ) -> str:
        # pylint: disable=anomalous-backslash-in-string
        """
        Generate a string representing the dependency graph for descendants.
        The graph begins at the given view, and descends through descendants
        in sorted order with additional indentation at each level.
        Source views need not be considered when constructing the descendants
        tree. For example, given node C in the graph
        A     B
         \   /
           C
           |
           D
         /   \
        E     F
        the descendant tree representation may look something like:
        - C
        - - D
        - - - E
        - - - F
        The descendants sub_dag used for the operation is cached in
        the node representing the view during populate_descendant_sub_dags.
        """

        sub_dag = self.node_for_view(view).descendants_sub_dag

        def _build_dfs_str(
            v: BigQueryView, parent_results: Dict[BigQueryView, str]
        ) -> str:
            return self._build_dfs_str(
                v=v,
                parent_view_dfs_strs=parent_results,
                parent_source_tables=set(),
                parent_table_sort_fn=lambda *args: sorted(
                    *args, key=parent_sort_fn, reverse=False
                ),
                node_formatter_fn=custom_node_formatter,
                datasets_to_skip=datasets_to_skip,
            )

        return (
            sub_dag.process_dag(_build_dfs_str, reverse=True).view_results[view] + "\n"
        )

    def _build_dfs_str(
        self,
        v: BigQueryView,
        parent_view_dfs_strs: Dict[BigQueryView, str],
        parent_source_tables: Set[BigQueryAddress],
        parent_table_sort_fn: Callable,
        node_formatter_fn: Callable[[BigQueryAddress], str],
        datasets_to_skip: Optional[Set[str]],
    ) -> str:
        parent_source_tables = {
            k
            for k in parent_source_tables
            if not datasets_to_skip or k.dataset_id not in datasets_to_skip
        }
        formatted_source_table_names = [
            node_formatter_fn(a) for a in parent_source_tables
        ]
        # TODO(#17679): Remove unecessarily complex sorting logic
        all_parent_results = parent_table_sort_fn(
            [
                *parent_view_dfs_strs.values(),
                *formatted_source_table_names,
            ]
        )
        if datasets_to_skip and v.dataset_id in datasets_to_skip:
            return "\n".join(all_parent_results)
        table_name = node_formatter_fn(v.address)
        return "\n".join(
            [
                table_name,
                *("|--" + p.replace("|--", "|----") for p in all_parent_results),
            ]
        )

    def related_ancestor_keys(
        self, dag_key: DagKey, terminating_datasets: Optional[Set[str]] = None
    ) -> Set[DagKey]:
        if terminating_datasets is None:
            terminating_datasets = set()
        related_keys = set()
        node = self.nodes_by_key[dag_key]
        ancestors = set(node.ancestors_sub_dag.nodes_by_key)
        related_keys |= ancestors
        for key in ancestors:
            if not key.dataset_id in terminating_datasets:
                node = self.nodes_by_key[key]
                related_keys |= node.parent_keys
        return related_keys
