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
from recidiviz.admin_panel.models.lineage_api_schemas import (
    BigQueryBetweenSchema,
    BigQueryNodeExpandedSchema,
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


@lineage_blueprint.route("fetch_between/<start_address_str>/<end_address_str>")
class FetchBetweenRoute(MethodView):
    """Route for fetching the routes between two nodes"""

    @lineage_blueprint.response(HTTPStatus.OK, BigQueryBetweenSchema)
    def get(
        self,
        start_address_str: str,
        end_address_str: str,
    ) -> dict[str, list]:
        start_address = BigQueryAddress.from_str(start_address_str)
        end_address = BigQueryAddress.from_str(end_address_str)

        urns = [
            addr.to_str()
            for addr in get_lineage_store().get_nodes_between(
                {start_address}, {end_address}
            )
        ]

        return {"urns": urns}


@lineage_blueprint.route("metadata/<dataset_id>/<view_id>")
class FetchMetadata(MethodView):
    """Route for fetching metadata about a bq view"""

    @lineage_blueprint.response(HTTPStatus.OK, BigQueryNodeExpandedSchema)
    def get(
        self,
        dataset_id: str,
        view_id: str,
    ) -> BigQueryViewNodeMetadata:
        address = BigQueryAddress(dataset_id=dataset_id, table_id=view_id)
        metadata = get_lineage_store().get_node_metadata(address)
        if metadata is None:
            abort(
                HTTPStatus.BAD_REQUEST,
                message=f"No known view: {dataset_id=} {view_id=}",
            )
        return metadata
