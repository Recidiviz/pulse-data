# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Defines routes for the Dataset Metadata API the admin panel."""
from http import HTTPStatus
from typing import Dict, Tuple

from flask import Blueprint, Response, jsonify, request

from recidiviz.admin_panel.admin_stores import (
    get_ingest_metadata_store,
    get_validation_metadata_store,
)
from recidiviz.admin_panel.dataset_metadata_store import (
    DatasetMetadataCountsStore,
    DatasetMetadataResult,
)
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.types import assert_type


def add_dataset_metadata_routes(admin_panel_blueprint: Blueprint) -> None:
    """Adds the relevant Dataset Metadata API routes to an input Blueprint."""

    def jsonify_dataset_metadata_result(
        result: DatasetMetadataResult,
    ) -> Tuple[Response, HTTPStatus]:
        results_dict: Dict[str, Dict[str, Dict[str, int]]] = {}
        for name, state_map in result.items():
            results_dict[name] = {}
            for state_code, counts in state_map.items():
                results_dict[name][state_code] = counts.to_json()
        return jsonify(results_dict), HTTPStatus.OK

    def _get_metadata_store(metadata_dataset: str) -> DatasetMetadataCountsStore:
        if metadata_dataset == "ingest_metadata":
            return get_ingest_metadata_store()
        if metadata_dataset == "validation_metadata":
            return get_validation_metadata_store()
        raise ValueError(
            f"Unknown metadata dataset [{metadata_dataset}], must be 'ingest_metadata' or 'validation_metadata' "
        )

    # Dataset column counts
    @admin_panel_blueprint.route(
        "/api/<metadata_dataset>/fetch_column_object_counts_by_value", methods=["POST"]
    )
    @requires_gae_auth
    def fetch_column_object_counts_by_value(
        metadata_dataset: str,
    ) -> Tuple[Response, HTTPStatus]:
        request_json = assert_type(request.json, dict)
        table = request_json["table"]
        column = request_json["column"]
        metadata_store = _get_metadata_store(metadata_dataset)

        return jsonify_dataset_metadata_result(
            metadata_store.fetch_column_object_counts_by_value(table, column)
        )

    @admin_panel_blueprint.route(
        "/api/<metadata_dataset>/fetch_table_nonnull_counts_by_column", methods=["POST"]
    )
    @requires_gae_auth
    def fetch_table_nonnull_counts_by_column(
        metadata_dataset: str,
    ) -> Tuple[Response, HTTPStatus]:
        request_json = assert_type(request.json, dict)
        table = request_json["table"]
        metadata_store = _get_metadata_store(metadata_dataset)

        return jsonify_dataset_metadata_result(
            metadata_store.fetch_table_nonnull_counts_by_column(table)
        )

    @admin_panel_blueprint.route(
        "/api/<metadata_dataset>/fetch_object_counts_by_table", methods=["POST"]
    )
    @requires_gae_auth
    def fetch_object_counts_by_table(metadata_dataset: str) -> Tuple[Response, int]:
        metadata_store = _get_metadata_store(metadata_dataset)
        return jsonify_dataset_metadata_result(
            metadata_store.fetch_object_counts_by_table()
        )
