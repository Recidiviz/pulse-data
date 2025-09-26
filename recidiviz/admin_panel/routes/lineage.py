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
"""Routes for /api/lineage endpoints"""

from http import HTTPStatus
from typing import Sequence

from flask.views import MethodView
from flask_smorest import Blueprint, abort

from recidiviz.admin_panel.admin_stores import get_lineage_store
from recidiviz.admin_panel.lineage_store import GraphDirection
from recidiviz.admin_panel.models.lineage_api_schemas import (
    BigQueryBetweenSchema,
    BigQueryDownstreamSchema,
    BigQueryNodeExpandedSchema,
    BigQuerySourceTableExpandedSchema,
    BigQuerySourceTableMetadata,
    BigQueryViewNodeMetadata,
    LineageGraphSchema,
)
from recidiviz.big_query.big_query_address import BigQueryAddress

lineage_blueprint = Blueprint("lineage", "lineage")


@lineage_blueprint.route("fetch_all")
class AllViewsRoute(MethodView):
    """Route for fetching the whole view graph"""

    @lineage_blueprint.response(HTTPStatus.OK, LineageGraphSchema)
    def get(self) -> dict[str, Sequence]:
        all_nodes = get_lineage_store().get_all_nodes()

        all_references = [
            ref for node in all_nodes for ref in node.upstream_references()
        ]

        return {"nodes": all_nodes, "references": all_references}


@lineage_blueprint.route("between/<direction_str>/<source_str>/<ancestor_str>")
class FetchBetweenRoute(MethodView):
    """Route for fetching the routes between two nodes"""

    @lineage_blueprint.response(HTTPStatus.OK, BigQueryBetweenSchema)
    def get(
        self,
        direction_str: str,
        source_str: str,
        ancestor_str: str,
    ) -> dict[str, list]:
        try:
            direction = GraphDirection(direction_str.upper())
        except ValueError:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"Unknown direction view: {direction_str=}",
            )

        if direction == GraphDirection.DOWNSTREAM:
            start_address = BigQueryAddress.from_str(source_str)
            end_address = BigQueryAddress.from_str(ancestor_str)
        else:
            start_address = BigQueryAddress.from_str(ancestor_str)
            end_address = BigQueryAddress.from_str(source_str)

        urns = [
            addr.to_str()
            for addr in get_lineage_store().get_nodes_between(
                start_address, end_address
            )
        ]

        return {"urns": urns}


@lineage_blueprint.route("metadata/view/<urn>")
class FetchViewMetadata(MethodView):
    """Route for fetching metadata about a bq view"""

    @lineage_blueprint.response(HTTPStatus.OK, BigQueryNodeExpandedSchema)
    def get(
        self,
        urn: str,
    ) -> BigQueryViewNodeMetadata:
        address = BigQueryAddress.from_str(urn)
        metadata = get_lineage_store().get_node_metadata(address)
        if metadata is None:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"No known view: {urn=}",
            )
        return metadata


@lineage_blueprint.route("metadata/source/<urn>")
class FetchSourceMetadata(MethodView):
    """Route for fetching metadata about a bq view"""

    @lineage_blueprint.response(HTTPStatus.OK, BigQuerySourceTableExpandedSchema)
    def get(
        self,
        urn: str,
    ) -> BigQuerySourceTableMetadata:
        address = BigQueryAddress.from_str(urn)
        metadata = get_lineage_store().get_source_metadata(address)
        if metadata is None:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"No known view: {urn=}",
            )
        return metadata


@lineage_blueprint.route("ancestors/<direction_str>/<urn>")
class FetchAncestor(MethodView):
    """Route for fetching ancestor for a view"""

    @lineage_blueprint.response(HTTPStatus.OK, BigQueryDownstreamSchema)
    def get(self, direction_str: str, urn: str) -> dict[str, list[str]]:
        try:
            direction = GraphDirection(direction_str.upper())
        except ValueError:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"Unknown direction view: {direction_str=}",
            )

        address = BigQueryAddress.from_str(urn)

        downstream_deps = get_lineage_store().get_ancestor_dependencies(
            direction, address
        )

        if downstream_deps is None:
            return {"urns": []}

        return {"urns": [addr.to_str() for addr in downstream_deps]}
