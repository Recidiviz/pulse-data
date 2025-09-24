# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Store used to keep information related to our data lineage graph."""
import json
from collections import defaultdict
from enum import Enum
from typing import Sequence

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.admin_panel.models.lineage_api_schemas import (
    BigQueryGraphNode,
    BigQuerySourceTableMetadata,
    BigQuerySourceTableNode,
    BigQueryViewNode,
    BigQueryViewNodeMetadata,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import (
    BigQueryViewDagWalker,
    TraversalDirection,
)
from recidiviz.big_query.big_query_view_utils import build_views_to_update
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.utils import metadata
from recidiviz.view_registry.deployed_views import deployed_view_builders


class GraphDirection(Enum):
    DOWNSTREAM = "DOWNSTREAM"
    UPSTREAM = "UPSTREAM"


class LineageStore(AdminPanelStore):
    """A store for tracking the current state of our data lineage assets."""

    def __init__(self) -> None:
        self.cache_key_base = f"{self.__class__.__name__}"
        self.walker = self._build_walker()
        self.source_tables = build_source_table_repository_for_collected_schemata(
            metadata.project_id()
        )

    def _build_walker(self) -> BigQueryViewDagWalker:
        view_builders = deployed_view_builders()

        views_to_update = build_views_to_update(
            candidate_view_builders=view_builders, sandbox_context=None
        )

        return BigQueryViewDagWalker(views_to_update)

    def _cache_key_for_direction_and_address(
        self, direction: GraphDirection, address: BigQueryAddress
    ) -> str:
        return f"{self.cache_key_base}__{direction.value}__{address.to_str()}"

    def hydrate_cache(self) -> None:
        for address, downstream_list in self._build_downstream_dependencies().items():
            self.redis.set(
                self._cache_key_for_direction_and_address(
                    direction=GraphDirection.DOWNSTREAM, address=address
                ),
                json.dumps(downstream_list),
            )

        for address, downstream_list in self._build_upstream_dependencies().items():
            self.redis.set(
                self._cache_key_for_direction_and_address(
                    direction=GraphDirection.UPSTREAM, address=address
                ),
                json.dumps(downstream_list),
            )

    def _build_upstream_dependencies(
        self,
    ) -> dict[BigQueryAddress, list[str]]:
        def build_upstream_dependencies(
            v: BigQueryView, parent_results: dict[BigQueryView, set[str]]
        ) -> set[str]:
            """Builds the upstream dependencies for |v|"""
            return (
                # all source addresses for our current view, if any exist
                {
                    addr.to_str()
                    for addr in self.walker.node_for_view(v).source_addresses
                }
                # all grandparents (parents of our parents)
                | {parent.address.to_str() for parent in parent_results.keys()}
                # all grandparents (parents of our parents)
                | {
                    parent_result
                    for parent_result_set in parent_results.values()
                    for parent_result in parent_result_set
                }
            )

        result = self.walker.process_dag(build_upstream_dependencies, synchronous=True)

        return {
            view.address: list(upstream_result_set)
            for view, upstream_result_set in result.view_results.items()
        } | {address: [] for address in self.walker.get_referenced_source_tables()}

    def _build_downstream_dependencies(
        self,
    ) -> dict[BigQueryAddress, list[str]]:
        """Builds a dictionary of node downstream dependencies, both for views within
        our view graph as well as source tables.
        """

        def build_downstream_dependencies(
            _v: BigQueryView, parent_results: dict[BigQueryView, set[str]]
        ) -> set[str]:
            """Builds the downstream dependencies for |_v|"""
            return (
                # all grandparents (parents of our parents)
                {parent.address.to_str() for parent in parent_results.keys()}
                # all grandparents (parents of our parents)
                | {
                    parent_result
                    for parent_result_set in parent_results.values()
                    for parent_result in parent_result_set
                }
            )

        result = self.walker.process_dag(
            build_downstream_dependencies,
            synchronous=True,
            traversal_direction=TraversalDirection.LEAVES_TO_ROOTS,
        )

        # for each source address, declare its downstream references to be all downstream
        # references of all of it's child nodes
        all_source_address_references: dict[BigQueryAddress, set[str]] = defaultdict(
            set
        )
        for node in self.walker.nodes_by_address.values():
            if node.source_addresses:
                for source_table in node.source_addresses:
                    all_source_address_references[source_table] = (
                        all_source_address_references[source_table]
                        | result.view_results[node.view]
                        | {node.view.address.to_str()}
                    )

        return {
            view.address: list(upstream_result_set)
            for view, upstream_result_set in result.view_results.items()
        } | {
            address: list(downstream_deps)
            for address, downstream_deps in all_source_address_references.items()
        }

    def get_ancestor_dependencies(
        self, direction: GraphDirection, address: BigQueryAddress
    ) -> list[BigQueryAddress] | None:
        serialized_downstream = self.redis.get(
            self._cache_key_for_direction_and_address(direction, address)
        )
        if serialized_downstream is None:
            return None
        return [
            BigQueryAddress.from_str(dep) for dep in json.loads(serialized_downstream)
        ]

    def get_nodes_between(
        self, start_set: set[BigQueryAddress], end_set: set[BigQueryAddress]
    ) -> set[BigQueryAddress]:

        start_source_addresses = start_set - self.walker.nodes_by_address.keys()
        start_node_addresses = start_set & self.walker.nodes_by_address.keys()

        return (
            self.walker.get_all_node_addresses_between_start_and_end_collections(
                start_source_addresses=start_source_addresses,
                start_node_addresses=start_node_addresses,
                end_node_addresses=end_set,
            )
            | start_source_addresses
        )

    def get_all_nodes(self) -> Sequence[BigQueryGraphNode]:
        view_nodes = [
            BigQueryViewNode.from_node(node=node)
            for node in self.walker.nodes_by_address.values()
        ]

        source_table_nodes = [
            BigQuerySourceTableNode.from_address(address=address)
            for address in self.walker.get_referenced_source_tables()
        ]

        return [*view_nodes, *source_table_nodes]

    def get_node_metadata(
        self, address: BigQueryAddress
    ) -> BigQueryViewNodeMetadata | None:
        node = self.walker.nodes_by_address.get(address)
        if not node:
            return None

        return BigQueryViewNodeMetadata.from_node(node)

    def get_source_metadata(
        self, address: BigQueryAddress
    ) -> BigQuerySourceTableMetadata | None:
        config = self.source_tables.source_tables.get(address)
        if not config:
            return None

        return BigQuerySourceTableMetadata.from_source_table_config(config)
