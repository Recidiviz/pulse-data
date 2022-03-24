# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Flask server for admin panel."""
import logging
import os
from http import HTTPStatus
from typing import Dict, Optional, Tuple

from flask import Blueprint, Response, jsonify, request, send_from_directory

from recidiviz.admin_panel.admin_stores import AdminStores, fetch_state_codes
from recidiviz.admin_panel.dataset_metadata_store import (
    DatasetMetadataCountsStore,
    DatasetMetadataResult,
)
from recidiviz.admin_panel.routes.data_discovery import add_data_discovery_routes
from recidiviz.admin_panel.routes.ingest_ops import add_ingest_ops_routes
from recidiviz.admin_panel.routes.justice_counts_tools import (
    add_justice_counts_tools_routes,
)
from recidiviz.admin_panel.routes.line_staff_tools import add_line_staff_tools_routes
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import (
    in_development,
    in_gcp_production,
    in_gcp_staging,
)
from recidiviz.utils.types import assert_type
from recidiviz.validation.configured_validations import get_all_validations

logging.getLogger().setLevel(logging.INFO)

admin_stores = AdminStores()
admin_stores.start_timers()  # Start store refresh timers

static_folder = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../frontends/admin-panel/build/",
    )
)


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
        return admin_stores.ingest_metadata_store
    if metadata_dataset == "validation_metadata":
        return admin_stores.validation_metadata_store
    raise ValueError(
        f"Unknown metadata dataset [{metadata_dataset}], must be 'ingest_metadata' or 'validation_metadata' "
    )


admin_panel = Blueprint("admin_panel", __name__, static_folder=static_folder)
add_line_staff_tools_routes(admin_panel)
add_data_discovery_routes(admin_panel)
add_ingest_ops_routes(admin_panel, admin_stores)
add_justice_counts_tools_routes(admin_panel)

# Dataset column counts
@admin_panel.route(
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


@admin_panel.route(
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


@admin_panel.route(
    "/api/<metadata_dataset>/fetch_object_counts_by_table", methods=["POST"]
)
@requires_gae_auth
def fetch_object_counts_by_table(metadata_dataset: str) -> Tuple[Response, int]:
    metadata_store = _get_metadata_store(metadata_dataset)
    return jsonify_dataset_metadata_result(
        metadata_store.fetch_object_counts_by_table()
    )


# Data freshness
@admin_panel.route("/api/ingest_metadata/data_freshness", methods=["POST"])
@requires_gae_auth
def fetch_ingest_data_freshness() -> Tuple[Response, HTTPStatus]:
    return (
        jsonify(admin_stores.ingest_data_freshness_store.data_freshness_results),
        HTTPStatus.OK,
    )


# Validation status
@admin_panel.route("/api/validation_metadata/state_codes", methods=["POST"])
@requires_gae_auth
def fetch_validation_state_codes() -> Tuple[Response, HTTPStatus]:
    all_state_codes = admin_stores.validation_status_store.state_codes
    state_code_info = fetch_state_codes(all_state_codes)
    return jsonify(state_code_info), HTTPStatus.OK


@admin_panel.route("api/validation_metadata/status", methods=["POST"])
@requires_gae_auth
def fetch_validation_metadata_status() -> Tuple[bytes, HTTPStatus]:
    records = admin_stores.validation_status_store.get_most_recent_validation_results()
    return (
        records.SerializeToString(),
        HTTPStatus.OK,
    )


@admin_panel.route(
    "api/validation_metadata/status/<validation_name>/<state_code>", methods=["POST"]
)
@requires_gae_auth
def fetch_validation_metadata_status_for_validation(
    validation_name: str, state_code: str
) -> Tuple[bytes, HTTPStatus]:
    records = admin_stores.validation_status_store.get_results_for_validation(
        validation_name, state_code
    )
    return (
        records.SerializeToString(),
        HTTPStatus.OK,
    )


@admin_panel.route(
    "/api/validation_metadata/description/<validation_name>", methods=["POST"]
)
@requires_gae_auth
def fetch_validation_description(validation_name: str) -> Tuple[str, HTTPStatus]:
    validations = get_all_validations()
    for validation in validations:
        if validation.validation_name == validation_name:
            return validation.view_builder.description, HTTPStatus.OK

    return (
        f"No validation name matches the name {validation_name}",
        HTTPStatus.BAD_REQUEST,
    )


# Frontend configuration
@admin_panel.route("/runtime_env_vars.js")
@requires_gae_auth
def runtime_env_vars() -> Tuple[str, HTTPStatus]:
    if in_development():
        env_string = "development"
    elif in_gcp_staging():
        env_string = "staging"
    elif in_gcp_production():
        env_string = "production"
    else:
        env_string = "unknown"
    return f'window.RUNTIME_GCP_ENVIRONMENT="{env_string}";', HTTPStatus.OK


@admin_panel.route("/")
@admin_panel.route("/<path:path>")
def fallback(path: Optional[str] = None) -> Response:
    if path is None or not os.path.exists(os.path.join(static_folder, path)):
        logging.info("Rewriting path")
        path = "index.html"
    return send_from_directory(static_folder, path)
