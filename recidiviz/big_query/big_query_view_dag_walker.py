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
from enum import Enum, auto
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
from recidiviz.monitoring import trace
from recidiviz.utils import environment, structured_logging

ViewResultT = TypeVar("ViewResultT")
ParentResultsT = Dict[BigQueryView, ViewResultT]


class TraversalDirection(Enum):
    ROOTS_TO_LEAVES = auto()
    LEAVES_TO_ROOTS = auto()


@attr.s(auto_attribs=True, kw_only=True)
class ViewProcessingMetadata:
    # The time it took to actually run the view_process_fn for this view.
    view_processing_runtime_sec: float
    # The total time between when this view was added to the queue for processing and
    # when it finishes processing. May include wait time, during which the job for this
    # view wasn't actually started yet.
    total_node_processing_time_sec: float
    graph_depth: int
    longest_path: List[BigQueryView]
    longest_path_runtime_seconds: float

    @property
    def queue_wait_time_sec(self) -> float:
        """The number of seconds between when a node is queued and when it actually
        starts view processing.
        """
        return self.total_node_processing_time_sec - self.view_processing_runtime_sec


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
    # The set of nodes that were processed last
    edge_nodes: Set["BigQueryViewDagNode"]

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
            processing_runtimes.append(
                (metadata.view_processing_runtime_sec, view.address)
            )
            queued_wait_times.append(metadata.queue_wait_time_sec)

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

        longest_path_runtime_seconds, longest_path = self._get_longest_path()

        path_strs = []
        for v in longest_path:
            runtime_seconds = self.view_processing_stats[v].view_processing_runtime_sec
            path_strs.append(f"  * {runtime_seconds:.2f} sec: {v.address.to_str()}")

        longest_path_str = "\n".join(path_strs)

        logging.info(
            "### BQ DAG PROCESSING STATS ###\n"
            "Total processing time: %s sec\n"
            "Nodes processed: %s\n"
            "Average queue wait time: %s seconds\n"
            "Max queue wait time: %s seconds\n"
            "Top [%s] most expensive nodes in DAG: \n%s\n"
            "Most expensive path [%s seconds total]: \n%s",
            round(self.total_runtime, 2),
            nodes_processed,
            avg_wait_time,
            max_wait_time,
            n_slowest,
            slowest_list,
            round(longest_path_runtime_seconds, 2),
            longest_path_str,
        )

    def _get_longest_path(self) -> Tuple[float, List[BigQueryView]]:
        edge_node_stats = [
            self.view_processing_stats[node.view] for node in self.edge_nodes
        ]

        end_of_longest_path_stats = sorted(
            edge_node_stats, key=lambda stats: stats.longest_path_runtime_seconds
        )[-1]
        return (
            end_of_longest_path_stats.longest_path_runtime_seconds,
            end_of_longest_path_stats.longest_path,
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
        self._descendants_sub_dag: Optional[BigQueryViewDagWalker] = None

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


_ProcessNodeQueueT = Union["_AsyncProcessNodeQueue", "_SyncProcessNodeQueue"]


class _AsyncProcessNodeQueue(Generic[ViewResultT]):
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
                trace.time_execution(
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


class _SyncProcessNodeQueue(Generic[ViewResultT]):
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

    def views_for_addresses(
        self, view_addresses: list[BigQueryAddress]
    ) -> List[BigQueryView]:
        """Returns the list of views associated with the provided list of addresses.
        Throws if any of the addresses do not have a corresponding view in this DAG.
        """
        views = []
        for view_address in view_addresses:
            if view_address not in self.nodes_by_address:
                raise ValueError(
                    f"Address [{view_address.to_str()}] is not a valid view address."
                )
            views.append(self.nodes_by_address[view_address].view)

        return views

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
        view: BigQueryView,
        parent_results: Dict[BigQueryView, ViewResultT],
        processing_time: float,
        queue_time: float,
    ) -> ViewProcessingMetadata:
        graph_depth = (
            0
            if not parent_results
            else max({view_processing_stats[p].graph_depth for p in parent_results}) + 1
        )

        if parent_results:
            parent_with_longest_path = max(
                (view_processing_stats[parent_view] for parent_view in parent_results),
                key=lambda parent_metadata: parent_metadata.longest_path_runtime_seconds,
            )
            parent_longest_path = parent_with_longest_path.longest_path
            parent_longest_path_runtime_seconds = (
                parent_with_longest_path.longest_path_runtime_seconds
            )
        else:
            parent_longest_path = []
            parent_longest_path_runtime_seconds = 0

        return ViewProcessingMetadata(
            view_processing_runtime_sec=processing_time,
            total_node_processing_time_sec=queue_time,
            graph_depth=graph_depth,
            longest_path=[*parent_longest_path, view],
            longest_path_runtime_seconds=(
                parent_longest_path_runtime_seconds + processing_time
            ),
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
        bottom_level_set = set(self.roots) if reverse else set(self.leaves)
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
                    view=node.view,
                    parent_results=parent_results,
                    processing_time=execution_sec,
                    queue_time=end - entered_queue_time,
                )
                view_results[node.view] = view_result
                view_processing_stats[node.view] = view_stats
                processed.add(node.view.address)
                logging.debug(
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
            edge_nodes=bottom_level_set,
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
            the parent ancestor sub-DAGs.
            """
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

        self.process_dag(populate_node_ancestors_sub_dag, synchronous=True)

    def populate_descendant_sub_dags(self) -> None:
        """Builds and caches descendant sub-DAGs on all nodes in this DAG."""

        def populate_node_descendants_sub_dag(
            v: BigQueryView, child_results: Dict[BigQueryView, None]
        ) -> None:
            """For a given view, calculates the descendants sub-DAG by unioning together
            the child descendant sub-DAGs.
            """
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

        # Process the DAG in the leaves -> roots direction so we process children
        # first.

        self.process_dag(
            populate_node_descendants_sub_dag, synchronous=True, reverse=True
        )

    def get_referenced_source_tables(self) -> set[BigQueryAddress]:
        referenced_source_tables: set[BigQueryAddress] = set()

        def add_child_nodes_to_referenced_source_tables(
            view: BigQueryView,
            _previous_level_results: dict[BigQueryView, None],
        ) -> None:
            node = self.node_for_view(view)
            referenced_source_tables.update(set(node.source_addresses))

        self.process_dag(
            view_process_fn=add_child_nodes_to_referenced_source_tables,
            synchronous=True,
        )
        return referenced_source_tables

    def ancestors_dfs_tree_str(
        self,
        view: BigQueryView,
        custom_node_formatter: Optional[Callable[[BigQueryAddress, bool], str]] = None,
        datasets_to_skip: Optional[Set[str]] = None,
    ) -> str:
        r"""
        Generate a string representing the dependency graph for ancestors.
        The graph begins at the given view, and ascends through ancestors
        in reverse sorted order with additional indentation at each level.
        Generally, the graph will terminate at a source table (which is not
        a node of the graph itself, but is calculated by taking the difference
        between all parent addresses and the parent addresses found from the
        process_dag parent results).

        For example, given node C in the graph

        A     B
         \   /
           C
           |
           D
         /   \
        E     F
         \   /
           G
          ...
        the ancestor tree representation may look something like:
        C
        |-- B
        |---- Source
        |-- A
        |---- Source

        If a node has already been fully explored in a previous section of the tree
        printout, it is not included twice. For example, the printout for node G in the
        same graph above would be structured like:

        G
        |-- E
        |---- D
        |------ C
        |-------- A
        |---------- Source
        |-------- B
        |---------- Source
        |-- F
        |---- D (...)
        """
        return self._build_dfs_str(
            traversal_direction=TraversalDirection.LEAVES_TO_ROOTS,
            start_address=view.address,
            custom_node_formatter=custom_node_formatter,
            datasets_to_skip=datasets_to_skip,
        )

    def descendants_dfs_tree_str(
        self,
        view: BigQueryView,
        custom_node_formatter: Optional[Callable[[BigQueryAddress, bool], str]] = None,
        datasets_to_skip: Optional[Set[str]] = None,
    ) -> str:
        r"""
        Generate a string representing the dependency graph for descendants.
        The graph begins at the given view, and descends through descendants
        in sorted order with additional indentation at each level.

        For example, given node D in the graph
           A
         /   \
        B     C
         \   /
           D
           |
           E
         /   \
        F     G
        the descendant tree representation may look something like:
        D
        |-- E
        |---- F
        |---- G

        If a node has already been fully explored in a previous section of the tree
        printout, it is not included twice. For example, the printout for node A in the
        same graph above would be structured like:
        A
        |-- B
        |---- D
        |------ E
        |-------- F
        |-------- G
        |-- C
        |---- D (...)
        """
        return self._build_dfs_str(
            traversal_direction=TraversalDirection.ROOTS_TO_LEAVES,
            start_address=view.address,
            custom_node_formatter=custom_node_formatter,
            datasets_to_skip=datasets_to_skip,
        )

    def _build_dfs_str(
        self,
        *,
        start_address: BigQueryAddress,
        traversal_direction: TraversalDirection,
        custom_node_formatter: Optional[Callable[[BigQueryAddress, bool], str]],
        datasets_to_skip: Optional[Set[str]],
    ) -> str:
        r"""
        Generate a string representing the dependency graph for the sub-DAG starting
        at the provided |start_address| and traversing in the direction denoted by
        |traversal_direction|.
        """
        # List of paths with whether that path was truncated
        sorted_paths: List[Tuple[List[BigQueryAddress], bool]] = sorted(
            self._get_unique_paths_from_address(start_address, traversal_direction)
        )

        result_rows = []
        for path_index, (path, is_truncated) in enumerate(sorted_paths):
            if path_index == 0:
                previous_path = None
            else:
                previous_path = sorted_paths[path_index - 1][0]

            for address_index, address in enumerate(path):
                if datasets_to_skip and address.dataset_id in datasets_to_skip:
                    continue
                is_last_in_path = len(path) - 1 == address_index

                if (
                    previous_path
                    and len(previous_path) > address_index
                    and previous_path[address_index] == address
                    and not is_last_in_path
                ):
                    # Don't print out elements that are exactly the same as the previous
                    # path. This makes the printout like a tree.
                    continue
                if address_index:
                    indent = "|" + ("--" * address_index)
                else:
                    indent = ""

                is_pruned_at_address = is_truncated and is_last_in_path
                if custom_node_formatter:
                    formatted_address = custom_node_formatter(
                        address, is_pruned_at_address
                    )
                else:
                    formatted_address = address.to_str()
                    if is_pruned_at_address:
                        formatted_address += " (...)"
                result_rows.append(f"{indent}{formatted_address}")

        return "\n".join(result_rows) + "\n"

    def _get_unique_paths_from_address(
        self,
        start_address: BigQueryAddress,
        traversal_direction: TraversalDirection,
    ) -> List[Tuple[List[BigQueryAddress], bool]]:
        r"""Returns a list of all from |start_address| to either a root or leaf node
        (as determined by |traversal_direction|). Each path is accompanied by a boolean
        that tells us whether the path has been truncated or not. A path may be
        truncated if we reach a node whose children have already been fully explored by
        previous paths. If this is the case the path ends early.

        Example:
           A
         /   \
        B     C
         \   /
           D
         /   \
        E     F

        _get_unique_paths_from_address(A, TraversalDirection.ROOTS_TO_LEAVES) =>
            [
              ([A, B, D, E], False),
              ([A, B, D, F], False),
              ([A, C, D], True)
            ]

        """
        # List of paths with whether that path was truncated
        paths: List[Tuple[List[BigQueryAddress], bool]] = []

        stack = [(start_address, [start_address])]
        visited = set()
        while stack:
            (current_address, path) = stack.pop()

            if current_address not in self.nodes_by_address:
                # The current_address is a source table and we have reached the end of
                # the path.
                paths.append((path, False))
                continue

            current_node: BigQueryViewDagNode = self.nodes_by_address[current_address]

            adjacent_addresses = (
                current_node.child_node_addresses
                if traversal_direction == TraversalDirection.ROOTS_TO_LEAVES
                else current_node.parent_node_addresses
            )
            if traversal_direction == TraversalDirection.LEAVES_TO_ROOTS:
                adjacent_addresses = adjacent_addresses | current_node.source_addresses

            if not adjacent_addresses:
                # The current_address is a leave/root node and we have reached the end
                # of the path.
                paths.append((path, False))
                continue

            if current_address in visited:
                # We have already visited this adjacent address so the remainder of
                # this path is redundant. We just add the path with the child and
                # mark it as truncated.
                paths.append((path, True))
                continue

            for adjacent_address in reversed(sorted(adjacent_addresses)):
                stack.append((adjacent_address, path + [adjacent_address]))
            visited.add(current_address)
        return paths

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
