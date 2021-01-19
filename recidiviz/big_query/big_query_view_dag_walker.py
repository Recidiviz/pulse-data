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
import logging
import re
from concurrent import futures
from typing import Callable, Dict, List, Set, Tuple, TypeVar

from recidiviz.big_query.big_query_view import BigQueryView, MATERIALIZED_SUFFIX

# We set this to 10 because urllib3 (used by the Google BigQuery client) has an default limit of 10 connections and
# we were seeing "urllib3.connectionpool:Connection pool is full, discarding connection" errors when this number
# increased.
# In the future, we could increase the worker number by playing around with increasing the pool size per this post:
# https://github.com/googleapis/python-storage/issues/253
from recidiviz.utils import structured_logging

DAG_WALKER_MAX_WORKERS = 10

DagKey = Tuple[str, str]
ViewResultT = TypeVar('ViewResultT')


class BigQueryViewDagNode:
    def __init__(self, view: BigQueryView, is_root: bool = False):
        self.view = view
        self.child_node_keys: Set[DagKey] = set()

        self.is_root = is_root

    @property
    def dag_key(self) -> DagKey:
        return self.view.dataset_id, self.view.view_id

    @property
    def parent_keys(self) -> Set[DagKey]:
        parent_keys: Set[DagKey] = set()

        candidates = re.findall(r'`[\w-]*\.([\w-]*)\.([\w-]*)`', self.view.view_query)
        for candidate in candidates:
            if candidate[1].endswith(MATERIALIZED_SUFFIX):
                parent_keys.add((candidate[0], candidate[1][:-len(MATERIALIZED_SUFFIX)]))
            else:
                parent_keys.add(candidate)

        return parent_keys

    def add_child_key(self, dag_key: DagKey) -> None:
        self.child_node_keys.add(dag_key)

    @property
    def child_keys(self) -> Set[DagKey]:
        return self.child_node_keys


class BigQueryViewDagWalker:
    """Class implementation that walks a DAG of BigQueryViews."""

    def __init__(self, views: List[BigQueryView]):
        dag_nodes = [BigQueryViewDagNode(view) for view in views]
        self.nodes_by_key = {node.dag_key: node for node in dag_nodes}

        self._prepare_dag()
        self.roots = [node for node in self.nodes_by_key.values() if node.is_root]
        self._check_for_cycles()

    def _prepare_dag(self) -> None:
        """Prepares for processing the full DAG by identifying root nodes and associating nodes with their children."""
        for key, node in self.nodes_by_key.items():
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
                    raise ValueError(f'Detected cycle in graph reachable from {start_key}: {path}')

                paths_to_explore.append((child_key, path+[child_key]))

    def process_dag(self, view_process_fn: Callable[[BigQueryView], ViewResultT]) -> Dict[BigQueryView, ViewResultT]:
        """This method provides a level-by-level "breadth-first" traversal of a DAG and executes
        view_process_fn on every node in level order."""
        processed: Set[DagKey] = set()
        queue: Set[BigQueryViewDagNode] = set(self.roots)
        result: Dict[BigQueryView, ViewResultT] = {}
        with futures.ThreadPoolExecutor(max_workers=DAG_WALKER_MAX_WORKERS) as executor:
            future_to_view = {
                executor.submit(structured_logging.with_context(view_process_fn), node.view): node
                for node in self.roots
            }
            processing = {node.dag_key for node in future_to_view.values()}
            while processing:
                completed, _not_completed = futures.wait(future_to_view.keys(), return_when='FIRST_COMPLETED')
                for future in completed:
                    node = future_to_view.pop(future)
                    try:
                        view_result: ViewResultT = future.result()
                    except Exception as e:
                        logging.error('Exception found fetching result for view_key: %s', node.dag_key)
                        raise e
                    result[node.view] = view_result
                    processing.remove(node.dag_key)
                    processed.add(node.dag_key)

                    for child_key in node.child_node_keys:
                        child_node = self.nodes_by_key[child_key]
                        if child_node in processed or child_node in queue:
                            raise ValueError(
                                f'Unexpected situation where child node has already been processed: {child_key}')
                        if child_node in processing:
                            continue

                        parents_all_processed = True
                        for parent_key in child_node.parent_keys:
                            if parent_key in self.nodes_by_key and parent_key not in processed:
                                parents_all_processed = False
                                break
                        if parents_all_processed:
                            future = executor.submit(structured_logging.with_context(view_process_fn), child_node.view)
                            future_to_view[future] = child_node
                            processing.add(child_node.dag_key)
        return result
