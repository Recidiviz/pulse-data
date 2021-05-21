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
from typing import Callable, Dict, List, Set, Tuple, TypeVar, Optional

import attr

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryAddress
from recidiviz.utils import structured_logging
from recidiviz.view_registry.deployed_views import NOISY_DEPENDENCY_VIEW_BUILDERS


# We set this to 10 because urllib3 (used by the Google BigQuery client) has an default limit of 10 connections and
# we were seeing "urllib3.connectionpool:Connection pool is full, discarding connection" errors when this number
# increased.
# In the future, we could increase the worker number by playing around with increasing the pool size per this post:
# https://github.com/googleapis/python-storage/issues/253
DAG_WALKER_MAX_WORKERS = 10


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


class BigQueryViewDagNode:
    """A single node in a BigQuery view DAG, i.e. a single view with relationships to other views."""

    def __init__(self, view: BigQueryView, is_root: bool = False):
        self.view = view
        # Note: Must use add_child_key() to populate this member variable before using.
        self.child_node_keys: Set[DagKey] = set()
        self.materialized_addresss: Optional[Dict[BigQueryAddress, DagKey]] = None
        self.is_root = is_root
        # Note: Must call populate_node_family_for_node() on a node before using this member variable.
        self.node_family: BigQueryViewDagNodeFamily = BigQueryViewDagNodeFamily()

    @property
    def dag_key(self) -> DagKey:
        return DagKey.for_view(self.view)

    @property
    def parent_tables(self) -> Set[BigQueryAddress]:
        """The set of actual tables/views referenced by this view."""
        parents = re.findall(r"`[\w-]*\.([\w-]*)\.([\w-]*)`", self.view.view_query)
        return {
            BigQueryAddress(dataset_id=candidate[0], table_id=candidate[1])
            for candidate in parents
        }

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

    @property
    def child_keys(self) -> Set[DagKey]:
        return self.child_node_keys


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
    ) -> Dict[BigQueryView, ViewResultT]:
        """This method provides a level-by-level "breadth-first" traversal of a DAG and executes
        view_process_fn on every node in level order."""
        processed: Set[DagKey] = set()
        queue: Set[BigQueryViewDagNode] = set(self.roots)
        result: Dict[BigQueryView, ViewResultT] = {}
        with futures.ThreadPoolExecutor(max_workers=DAG_WALKER_MAX_WORKERS) as executor:
            future_to_view = {
                executor.submit(
                    structured_logging.with_context(view_process_fn), node.view, {}
                ): node
                for node in self.roots
            }
            processing = {node.dag_key for node in future_to_view.values()}
            while processing:
                completed, _not_completed = futures.wait(
                    future_to_view.keys(), return_when="FIRST_COMPLETED"
                )
                for future in completed:
                    node = future_to_view.pop(future)
                    try:
                        view_result: ViewResultT = future.result()
                    except Exception as e:
                        logging.error(
                            "Exception found fetching result for view_key: %s",
                            node.dag_key,
                        )
                        raise e
                    result[node.view] = view_result
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
                                parent_results[parent_view] = result[parent_view]
                        if parents_all_processed:
                            future = executor.submit(
                                structured_logging.with_context(view_process_fn),
                                child_node.view,
                                parent_results,
                            )
                            future_to_view[future] = child_node
                            processing.add(child_node.dag_key)
        return result

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
        # TODO(#7049): refactor most_recent_job_id_by_metric_and_state_code dependencies
        noisy_dependency_keys = [
            DagKey(
                view_address=BigQueryAddress(
                    dataset_id=builder.dataset_id, table_id=builder.view_id
                )
            )
            for builder in NOISY_DEPENDENCY_VIEW_BUILDERS
        ]

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
                        # Stop if we hit a noisy dependency
                        if related_key not in noisy_dependency_keys:
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

        node.node_family = BigQueryViewDagNodeFamily(
            full_parentage=full_parentage,
            parent_dfs_tree_str=parent_tree,
            full_descendants=full_descendants,
            child_dfs_tree_str=child_tree,
        )
