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
import os
import time
from collections import deque
from concurrent import futures
from concurrent.futures import Future
from types import TracebackType
from typing import (
    Callable,
    Deque,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.utils import environment, structured_logging, trace

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

        # Note: Must use add_child_node_address() to populate this member variable before using.
        self.child_node_addresses: Set[BigQueryAddress] = set()

        # The set of actual addresses to parent DAG nodes for the underlying view.
        # Note: Must use add_parent_node_address() to populate this member variable before using.
        self.parent_node_addresses: Set[BigQueryAddress] = set()

        # The set of addresses referenced by the underlying
        # view which are not also nodes in the DAG. When the
        # DAG contains all existing views, this result
        # gives the source tables referenced by the underlying
        # view.
        self.source_addresses: Set[BigQueryAddress] = set()

        self.is_root = is_root
        self.is_leaf = is_leaf

        self._ancestors_sub_dag: Optional[BigQueryViewDagWalker] = None
        self._ancestors_tree_num_edges: Optional[int] = None
        self._descendants_sub_dag: Optional[BigQueryViewDagWalker] = None
        self._descendants_tree_num_edges: Optional[int] = None

    @property
    def parent_tables(self) -> Set[BigQueryAddress]:
        """The set of actual parent tables/addresses referenced by the underlying view."""
        return self.view.parent_tables

    def add_child_node_address(self, address: BigQueryAddress) -> None:
        self.child_node_addresses.add(address)

    def add_parent_node_address(self, address: BigQueryAddress) -> None:
        self.parent_node_addresses.add(address)

    def add_source_address(self, source_address: BigQueryAddress) -> None:
        self.source_addresses.add(source_address)

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
        r"""Returns the number of edges in the tree with this node at the root and all
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
        r"""Returns the number of edges in the tree with this node at the root and all
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


_ProcessNodeQueueT = Union["_AsyncProcessNodeQueue", "_SyncProcessNodeQueue"]


class _AsyncProcessNodeQueue:
    """
    Internal queue implementation that enqueues for
    asynchronous execution with executor.submit
    """

    def __init__(
        self,
        view_process_fn: Callable[[BigQueryView, ParentResultsT], ViewResultT],
    ) -> None:
        self.executor = futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        )
        self.future_to_context: Dict[
            Future[Tuple[float, ViewResultT]],
            Tuple[BigQueryViewDagNode, ParentResultsT, float],
        ] = {}
        self.view_process_fn = view_process_fn

    def __enter__(self) -> _ProcessNodeQueueT:
        self.executor.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: TracebackType,
    ) -> None:
        self.executor.__exit__(exc_type, exc_val, exc_tb)

    def __len__(self) -> int:
        return len(self.future_to_context)

    def enqueue(
        self,
        item: Tuple[BigQueryViewDagNode, Dict[BigQueryView, ViewResultT], float],
    ) -> None:
        adjacent_node, previous_level_results, entered_queue_time = item
        self.future_to_context[
            self.executor.submit(
                trace.time_and_trace(
                    structured_logging.with_context(self.view_process_fn)
                ),
                adjacent_node.view,
                previous_level_results,
            )
        ] = (
            adjacent_node,
            previous_level_results,
            entered_queue_time,
        )

    def dequeue(
        self,
    ) -> Tuple[Callable, BigQueryViewDagNode, Dict[BigQueryView, ViewResultT], float,]:
        completed, _not_completed = futures.wait(
            self.future_to_context.keys(), return_when=futures.FIRST_COMPLETED
        )
        future = completed.pop()
        node, parent_results, entered_queue_time = self.future_to_context.pop(future)
        return future.result, node, parent_results, entered_queue_time


class _SyncProcessNodeQueue:
    """
    Internal queue implementation adapter for synchronous
    processing with deque
    """

    def __init__(
        self,
        view_process_fn: Callable[[BigQueryView, ParentResultsT], ViewResultT],
    ) -> None:
        self.queue: Deque[
            Tuple[BigQueryViewDagNode, Dict[BigQueryView, ViewResultT], float]
        ] = deque()
        self.view_process_fn = view_process_fn

    def __enter__(self) -> _ProcessNodeQueueT:
        return self

    def __exit__(
        self,
        _exc_type: Optional[type[BaseException]],
        _exc_val: Optional[BaseException],
        _exc_tb: TracebackType,
    ) -> None:
        return

    def __len__(self) -> int:
        return len(self.queue)

    def enqueue(
        self,
        item: Tuple[BigQueryViewDagNode, Dict[BigQueryView, ViewResultT], float],
    ) -> None:
        self.queue.appendleft(item)

    def dequeue(
        self,
    ) -> Tuple[Callable, BigQueryViewDagNode, Dict[BigQueryView, ViewResultT], float,]:
        node, parent_results, entered_queue_time = self.queue.pop()
        return (
            lambda: trace.time_and_trace(
                structured_logging.with_context(self.view_process_fn)
            )(node.view, parent_results),
            node,
            parent_results,
            entered_queue_time,
        )


class BigQueryViewDagWalker:
    """Class implementation that walks a DAG of BigQueryViews."""

    def __init__(
        self,
        views: Iterable[BigQueryView],
    ):
        self.views = list(views)
        dag_nodes = [BigQueryViewDagNode(view) for view in views]
        self.nodes_by_address: Dict[BigQueryAddress, BigQueryViewDagNode] = {
            node.view.address: node for node in dag_nodes
        }
        self._prepare_dag()
        self.roots = [node for node in self.nodes_by_address.values() if node.is_root]
        self.leaves = [node for node in self.nodes_by_address.values() if node.is_leaf]

        self._check_for_cycles()

    def _prepare_dag(self) -> None:
        """
        Prepares for processing the full DAG by identifying root nodes and
        associating nodes with their children.

        For every view, if it has an associated materialized table, map
        the addresss of those (parent) tables to the address for the original view.
        """

        materialized_addresses: Dict[BigQueryAddress, BigQueryAddress] = {}
        for address, node in self.nodes_by_address.items():
            if node.view.materialized_address:
                if node.view.materialized_address in materialized_addresses:
                    raise ValueError(
                        f"Found materialized view address for view [{address.to_str()}] "
                        f"that matches materialized_address of another view: "
                        f"[{materialized_addresses[node.view.materialized_address].to_str()}]. "
                        f"Two BigQueryViews cannot share the same "
                        f"materialized_address."
                    )
                materialized_address = node.view.materialized_address
                if materialized_address in self.nodes_by_address:
                    raise ValueError(
                        f"Found materialized view address for view [{address.to_str()}] "
                        f"that matches the view address of another view "
                        f"[{materialized_address.to_str()}]. The "
                        f"materialized_address of a view cannot be the address "
                        f"of another BigQueryView."
                    )

                materialized_addresses[node.view.materialized_address] = address

        for address, node in self.nodes_by_address.items():
            node.is_root = True
            for parent_address in node.parent_tables:
                parent_address = materialized_addresses.get(
                    parent_address, parent_address
                )
                if parent_address in self.nodes_by_address:
                    parent_node = self.nodes_by_address[parent_address]
                    parent_node.is_leaf = False
                    parent_node.add_child_node_address(address)
                    node.is_root = False
                    node.add_parent_node_address(parent_address)
                else:
                    node.add_source_address(parent_address)

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
        if not self.nodes_by_address:
            return

        if not self.roots:
            raise ValueError("No roots detected. Input views contain a cycle.")

        visited = set()
        # Mapping of nodes in the current path to the child for that node.
        current_path_edges: Dict[BigQueryAddress, BigQueryAddress] = {}
        for start_address in self.nodes_by_address:
            if start_address in visited:
                continue

            stack: Deque[Tuple[BigQueryAddress, Deque[BigQueryAddress]]] = deque()

            next_address = start_address
            while True:
                if next_address not in visited:
                    child_addresses = self.nodes_by_address[
                        next_address
                    ].child_node_addresses
                    if child_addresses:
                        stack.append((next_address, deque(child_addresses)))
                    visited.add(next_address)

                if not stack:
                    break

                # Peek at the top of the stack to get current node
                current_address, current_address_children = stack[-1]

                if not current_address_children:
                    # We have explored all children of the address and found no
                    # cycles - pop this address.
                    stack.pop()
                    current_path_edges.pop(current_address)
                    continue

                next_address = current_address_children.pop()
                if next_address in current_path_edges:
                    raise ValueError(
                        f"Detected cycle in graph reachable from "
                        f"{start_address.to_str()}: "
                        f"{[e.to_str() for e in self._get_cycle_path(start_address, current_path_edges)]}"
                    )
                current_path_edges[current_address] = next_address

    def _get_cycle_path(
        self,
        start_node_address: BigQueryAddress,
        current_path_edges: Dict[BigQueryAddress, BigQueryAddress],
    ) -> Iterator[BigQueryAddress]:
        address = start_node_address
        while address in current_path_edges:
            address = current_path_edges[address]
            yield address

    def view_for_address(self, view_address: BigQueryAddress) -> BigQueryView:
        return self.nodes_by_address[view_address].view

    def node_for_view(self, view: BigQueryView) -> BigQueryViewDagNode:
        return self.nodes_by_address[view.address]

    def _adjacent_dag_verticies(
        self,
        node: BigQueryViewDagNode,
        view_results: Dict[BigQueryView, ViewResultT],
        visited_set: Set[BigQueryAddress],
        reverse: bool,
    ) -> Generator[
        Tuple[BigQueryViewDagNode, Dict[BigQueryView, ViewResultT]], None, None
    ]:
        """
        Generates an iterable of tuples, adjacent nodes and parent results,
        which are adjacent to the given node
        """

        adjacent_addresses = (
            node.parent_node_addresses if reverse else node.child_node_addresses
        )
        for adjacent_address in adjacent_addresses:
            adjacent_node = self.nodes_by_address[adjacent_address]
            previous_level_all_processed = True
            previous_level_results = {}
            previous_level_addresses = (
                adjacent_node.child_node_addresses
                if reverse
                else adjacent_node.parent_node_addresses
            )
            for previous_level_address in previous_level_addresses:
                if (
                    previous_level_address in self.nodes_by_address
                    and previous_level_address not in visited_set
                ):
                    previous_level_all_processed = False
                    break
                if previous_level_address in self.nodes_by_address:
                    previous_level_view = self.nodes_by_address[
                        previous_level_address
                    ].view
                    previous_level_results[previous_level_view] = view_results[
                        previous_level_view
                    ]
            if previous_level_all_processed:
                if not previous_level_results:
                    raise ValueError(
                        f"Expected adjacent node [{adjacent_node.view.address}] "
                        f"to have previously processed ancesters."
                    )
                yield adjacent_node, previous_level_results

    @staticmethod
    def _check_processing_time(
        perf_config: ProcessDagPerfConfig,
        view: BigQueryView,
        processing_time: float,
    ) -> None:
        allowed_processing_time = perf_config.allowed_processing_time(view.address)
        if processing_time > allowed_processing_time:
            error_msg = (
                f"[BigQueryViewDagWalker Node Failure] Processing for "
                f"[{view.address}] took [{round(processing_time, 2)}] "
                f"seconds. Expected node to process in less than "
                f"[{allowed_processing_time}] seconds."
            )
            if (
                # TODO(#21394) Update this check properly to reflect environment is in Airflow
                environment.in_gcp()
                or os.environ.get("CONTAINER_NAME", None) is not None
            ):
                # Runtimes can be more unreliable in GCP due to resource
                # contention with other running processes, so we do not
                # throw in GCP. We instead emit an error log which can be
                # used to fire an alert.
                logging.error(error_msg)
            else:
                raise ValueError(error_msg)

    @staticmethod
    def _dag_view_process_fn_result(
        results_fn: Callable[[], Tuple[float, ViewResultT]],
        view: BigQueryView,
    ) -> Tuple[float, ViewResultT]:
        try:
            execution_sec, view_result = results_fn()
        except Exception as e:
            logging.error(
                "Exception found fetching results for for address: [%s]",
                view.address,
            )
            raise e
        return execution_sec, view_result

    @staticmethod
    def _view_processing_statistics(
        view_processing_stats: Dict[BigQueryView, ViewProcessingMetadata],
        parent_results: Dict[BigQueryView, ViewResultT],
        processing_time: float,
        queue_time: float,
    ) -> ViewProcessingMetadata:
        graph_depth = (
            0
            if not parent_results
            else max({view_processing_stats[p].graph_depth for p in parent_results}) + 1
        )
        return ViewProcessingMetadata(
            node_processing_runtime_seconds=processing_time,
            total_time_in_queue_seconds=queue_time,
            graph_depth=graph_depth,
        )

    def process_dag(
        self,
        view_process_fn: Callable[[BigQueryView, ParentResultsT], ViewResultT],
        synchronous: bool,
        perf_config: Optional[ProcessDagPerfConfig] = DEFAULT_PROCESS_DAG_PERF_CONFIG,
        reverse: bool = False,
    ) -> ProcessDagResult[ViewResultT]:
        """
        This method provides a level-by-level "breadth-first" traversal of a DAG and
        executes |view_process_fn| on every node in level order.

        Callers are required to choose between asynchronous/multi-threaded and
        synchronous/single-threaded processing. Asynchronous execution is likely
        to perform better with IO-intensive view processing functions, which sleep
        waiting for completion. CPU-intensive view processing functions may perform better
        with synchronous execution.

        Will throw if a node execution time exceeds |max_node_process_time_sec|.

        If a |perf_config| is provided, processing will fail if any node takes longer
        to process than is allowed by the config.
        """

        top_level_set = set(self.leaves) if reverse else set(self.roots)
        processed: Set[BigQueryAddress] = set()
        view_results: Dict[BigQueryView, ViewResultT] = {}
        view_processing_stats: Dict[BigQueryView, ViewProcessingMetadata] = {}
        dag_processing_start = time.perf_counter()
        queue: _ProcessNodeQueueT = (
            _SyncProcessNodeQueue(view_process_fn=view_process_fn)
            if synchronous
            else _AsyncProcessNodeQueue(view_process_fn=view_process_fn)
        )
        with queue:
            for node in top_level_set:
                queue.enqueue((node, {}, dag_processing_start))
            while queue:
                parent_results: Dict[BigQueryView, ViewResultT]
                (
                    view_result_fn,
                    node,
                    parent_results,
                    entered_queue_time,
                ) = queue.dequeue()
                if node.view.address in processed:
                    raise ValueError(
                        "Unexpected situation where node has already "
                        f"been processed: {node.view.address}"
                    )
                end = time.perf_counter()
                execution_sec, view_result = self._dag_view_process_fn_result(
                    results_fn=view_result_fn,
                    view=node.view,
                )
                if perf_config:
                    self._check_processing_time(
                        perf_config=perf_config,
                        view=node.view,
                        processing_time=execution_sec,
                    )
                view_stats = self._view_processing_statistics(
                    view_processing_stats=view_processing_stats,
                    parent_results=parent_results,
                    processing_time=execution_sec,
                    queue_time=end - entered_queue_time,
                )
                view_results[node.view] = view_result
                view_processing_stats[node.view] = view_stats
                processed.add(node.view.address)
                logging.info(
                    "Completed processing of [%s]. Duration: [%s] seconds.",
                    node.view.address.to_str(),
                    execution_sec,
                )
                for (
                    adjacent_node,
                    previous_level_results,
                ) in self._adjacent_dag_verticies(
                    node=node,
                    view_results=view_results,
                    visited_set=processed,
                    reverse=reverse,
                ):
                    entered_queue_time = time.perf_counter()
                    queue.enqueue(
                        (adjacent_node, previous_level_results, entered_queue_time)
                    )
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

        self.process_dag(collect_descendants, synchronous=True)
        self._check_sub_dag_views(input_views=views, sub_dag_views=sub_dag_views)
        sub_dag = BigQueryViewDagWalker(list(sub_dag_views))
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

        self.process_dag(collect_ancestors, synchronous=True)
        self._check_sub_dag_views(input_views=views, sub_dag_views=sub_dag_views)
        sub_dag = BigQueryViewDagWalker(list(sub_dag_views))
        return sub_dag

    @staticmethod
    def union_dags(*dags: "BigQueryViewDagWalker") -> "BigQueryViewDagWalker":
        found_view_addresses = set()
        views: List[BigQueryView] = []
        for dag in dags:
            for view in dag.views:
                if view.address not in found_view_addresses:
                    views.append(view)
                    found_view_addresses.add(view.address)

        unioned_dag = BigQueryViewDagWalker(views=views)
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

    def populate_ancestor_sub_dags(self) -> None:
        """Builds and caches ancestor sub-DAGs on all nodes in this DAG."""

        def populate_node_ancestors_sub_dag(
            v: BigQueryView, parent_results: Dict[BigQueryView, None]
        ) -> None:
            """For a given view, calculates the ancestors sub-DAG by unioning together
            the parent ancestor sub-DAGs."""
            this_view_dag = BigQueryViewDagWalker([v])
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
            ancestors_tree_num_edges = (
                len(node.source_addresses)
                + len(node.parent_node_addresses)
                + sum(p.ancestors_tree_num_edges for p in parent_nodes)
            )
            node.set_ancestors_tree_num_edges(ancestors_tree_num_edges)

        self.process_dag(populate_node_ancestors_sub_dag, synchronous=True)

    def populate_descendant_sub_dags(self) -> None:
        """Builds and caches descendant sub-DAGs on all nodes in this DAG."""

        def populate_node_descendants_sub_dag(
            v: BigQueryView, child_results: Dict[BigQueryView, None]
        ) -> None:
            """For a given view, calculates the descendants sub-DAG by unioning together
            the child descendant sub-DAGs."""
            this_view_dag = BigQueryViewDagWalker([v])
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

        self.process_dag(
            populate_node_descendants_sub_dag, synchronous=True, reverse=True
        )

    def ancestors_dfs_tree_str(
        self,
        view: BigQueryView,
        custom_node_formatter: Callable[
            [BigQueryAddress], str
        ] = lambda a: f"{a.dataset_id}.{a.table_id}",
        datasets_to_skip: Optional[Set[str]] = None,
    ) -> str:
        r"""
        Generate a string representing the dependency graph for ancestors.
        The graph begins at the given view, and ascends through ancestors
        in reverse sorted order with additional indentation at each level.
        Generally, the graph will terminate at a source view (which is not
        a node of the graph itself, but is calculated by taking the difference
        between all parent addresses and the parent addresses found from the
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
            node = self.node_for_view(v)
            source_table_addresses = node.source_addresses
            return self._build_dfs_str(
                v=v,
                parent_view_dfs_strs=parent_results,
                parent_source_tables=source_table_addresses,
                node_formatter_fn=custom_node_formatter,
                datasets_to_skip=datasets_to_skip,
            )

        return (
            sub_dag.process_dag(_build_dfs_str, synchronous=True).view_results[view]
            + "\n"
        )

    def descendants_dfs_tree_str(
        self,
        view: BigQueryView,
        custom_node_formatter: Callable[
            [BigQueryAddress], str
        ] = lambda a: f"{a.dataset_id}.{a.table_id}",
        datasets_to_skip: Optional[Set[str]] = None,
    ) -> str:
        r"""
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
                node_formatter_fn=custom_node_formatter,
                datasets_to_skip=datasets_to_skip,
            )

        return (
            sub_dag.process_dag(
                _build_dfs_str, synchronous=True, reverse=True
            ).view_results[view]
            + "\n"
        )

    def _build_dfs_str(
        self,
        v: BigQueryView,
        parent_view_dfs_strs: Dict[BigQueryView, str],
        parent_source_tables: Set[BigQueryAddress],
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
        all_parent_results = sorted(
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

    def related_ancestor_addresses(
        self, address: BigQueryAddress, terminating_datasets: Optional[Set[str]] = None
    ) -> Set[BigQueryAddress]:
        if terminating_datasets is None:
            terminating_datasets = set()
        related_addresses = set()
        node = self.nodes_by_address[address]
        ancestors = set(node.ancestors_sub_dag.nodes_by_address)
        related_addresses |= ancestors
        for ancestor in ancestors:
            if not ancestor.dataset_id in terminating_datasets:
                node = self.nodes_by_address[ancestor]
                related_addresses |= node.parent_node_addresses | node.source_addresses
        return related_addresses
