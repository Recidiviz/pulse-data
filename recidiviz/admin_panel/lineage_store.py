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
from typing import Sequence

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.admin_panel.models.lineage_api_schemas import (
    BigQueryGraphNode,
    BigQuerySourceTableNode,
    BigQueryViewNode,
    BigQueryViewNodeMetadata,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.big_query_view_utils import build_views_to_update
from recidiviz.view_registry.deployed_views import deployed_view_builders


class LineageStore(AdminPanelStore):
    """A store for tracking the current state of our data lineage assets."""

    def __init__(self) -> None:
        self.cache_key = f"{self.__class__}V2"
        self.walker = self._build_walker()

    def _build_walker(self) -> BigQueryViewDagWalker:
        view_builders = deployed_view_builders()

        views_to_update = build_views_to_update(
            candidate_view_builders=view_builders, sandbox_context=None
        )

        return BigQueryViewDagWalker(views_to_update)

    def hydrate_cache(self) -> None:
        # TODO(#36174) consider caching path or other info that would be helpful to
        # pre-compute at certain intervals
        pass

    def get_nodes_between(
        self, start_set: set[BigQueryAddress], end_set: set[BigQueryAddress]
    ) -> set[BigQueryAddress]:
        return self.walker.get_all_node_addresses_between_start_and_end_collections(
            start_source_addresses=set(),
            start_node_addresses=start_set,
            end_node_addresses=end_set,
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
