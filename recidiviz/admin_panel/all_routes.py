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

from recidiviz.admin_panel.dataset_metadata_store import (
    DatasetMetadataCountsStore,
    DatasetMetadataResult,
)
from recidiviz.admin_panel.ingest_metadata_store import IngestDataFreshnessStore
from recidiviz.admin_panel.routes.case_triage import add_case_triage_routes
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_region_dir_names,
)
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import (
    GCP_PROJECT_STAGING,
    in_development,
    in_gcp,
    in_gcp_staging,
    in_gcp_production,
)
from recidiviz.utils.timer import RepeatedTimer


_INGEST_METADATA_NICKNAME = "ingest"
_INGEST_METADATA_PREFIX = "ingest_state_metadata"
_VALIDATION_METADATA_NICKNAME = "validation"
_VALIDATION_METADATA_PREFIX = "validation_metadata"


logging.getLogger().setLevel(logging.INFO)

if in_gcp() or in_development():
    override_project_id: Optional[str] = None
    if in_development():
        override_project_id = GCP_PROJECT_STAGING

    ingest_metadata_store = DatasetMetadataCountsStore(
        _INGEST_METADATA_NICKNAME,
        _INGEST_METADATA_PREFIX,
        override_project_id=override_project_id,
    )
    validation_metadata_store = DatasetMetadataCountsStore(
        _VALIDATION_METADATA_NICKNAME,
        _VALIDATION_METADATA_PREFIX,
        override_project_id=override_project_id,
    )
    ingest_data_freshness_store = IngestDataFreshnessStore(
        override_project_id=override_project_id
    )

    all_stores = [
        ingest_metadata_store,
        validation_metadata_store,
        ingest_data_freshness_store,
    ]

    store_refresh_timers = [
        RepeatedTimer(15 * 60, store.recalculate_store, run_immediately=True)
        for store in all_stores
    ]

    for timer in store_refresh_timers:
        timer.start()

static_folder = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../frontends/admin-panel/build/",
    )
)

admin_panel = Blueprint("admin_panel", __name__, static_folder=static_folder)
add_case_triage_routes(admin_panel)


def jsonify_dataset_metadata_result(
    result: DatasetMetadataResult,
) -> Tuple[str, HTTPStatus]:
    results_dict: Dict[str, Dict[str, Dict[str, int]]] = {}
    for name, state_map in result.items():
        results_dict[name] = {}
        for state_code, counts in state_map.items():
            results_dict[name][state_code] = counts.to_json()
    return jsonify(results_dict), HTTPStatus.OK


def _get_metadata_store(metadata_dataset: str) -> DatasetMetadataCountsStore:
    if metadata_dataset == "ingest_metadata":
        return ingest_metadata_store
    if metadata_dataset == "validation_metadata":
        return validation_metadata_store
    raise ValueError(
        f"Unknown metadata dataset [{metadata_dataset}], must be 'ingest_metadata' or 'validation_metadata' "
    )


# Dataset column counts
@admin_panel.route(
    "/api/<metadata_dataset>/fetch_column_object_counts_by_value", methods=["POST"]
)
@requires_gae_auth
def fetch_column_object_counts_by_value(
    metadata_dataset: str,
) -> Tuple[str, HTTPStatus]:
    table = request.json["table"]
    column = request.json["column"]
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
) -> Tuple[str, HTTPStatus]:
    table = request.json["table"]
    metadata_store = _get_metadata_store(metadata_dataset)

    return jsonify_dataset_metadata_result(
        metadata_store.fetch_table_nonnull_counts_by_column(table)
    )


@admin_panel.route(
    "/api/<metadata_dataset>/fetch_object_counts_by_table", methods=["POST"]
)
@requires_gae_auth
def fetch_object_counts_by_table(metadata_dataset: str) -> Tuple[str, int]:
    metadata_store = _get_metadata_store(metadata_dataset)
    return jsonify_dataset_metadata_result(
        metadata_store.fetch_object_counts_by_table()
    )


# Data freshness
@admin_panel.route("/api/ingest_metadata/data_freshness", methods=["POST"])
@requires_gae_auth
def fetch_ingest_data_freshness() -> Tuple[str, HTTPStatus]:
    return jsonify(ingest_data_freshness_store.data_freshness_results), HTTPStatus.OK


# Ingest Operations Actions
@admin_panel.route("/api/ingest_operations/fetch_ingest_region_codes", methods=["POST"])
@requires_gae_auth
def fetch_ingest_region_codes() -> Tuple[str, HTTPStatus]:
    ingest_region_codes = [
        region_code
        for region_code in sorted(get_existing_region_dir_names())
        if StateCode.is_state_code(region_code)
    ]
    return jsonify(ingest_region_codes), HTTPStatus.OK


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
