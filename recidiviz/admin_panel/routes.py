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

from recidiviz.admin_panel.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.admin_panel.ingest_metadata_store import (
    IngestMetadataCountsStore,
    IngestMetadataResult,
)
from recidiviz.case_triage.views.view_config import CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.metrics.export.export_config import (
    CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import (
    GCP_PROJECT_STAGING,
    in_development,
    in_gcp_staging,
    in_gcp_production,
    in_test,
)
from recidiviz.utils.timer import RepeatedTimer

logging.getLogger().setLevel(logging.INFO)
if in_development():
    ingest_metadata_store = IngestMetadataCountsStore(
        override_project_id=GCP_PROJECT_STAGING
    )
else:
    ingest_metadata_store = IngestMetadataCountsStore()

static_folder = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../frontends/admin-panel/build/",
    )
)
admin_panel = Blueprint("admin_panel", __name__, static_folder=static_folder)
store_refresh = RepeatedTimer(
    15 * 60, ingest_metadata_store.recalculate_store, run_immediately=True
)
if not in_test():
    store_refresh.start()


def jsonify_ingest_metadata_result(
    result: IngestMetadataResult,
) -> Tuple[str, HTTPStatus]:
    results_dict: Dict[str, Dict[str, Dict[str, int]]] = {}
    for name, state_map in result.items():
        results_dict[name] = {}
        for state_code, counts in state_map.items():
            results_dict[name][state_code] = counts.to_json()
    return jsonify(results_dict), HTTPStatus.OK


# State dataset column counts
@admin_panel.route(
    "/api/ingest_metadata/fetch_column_object_counts_by_value", methods=["POST"]
)
@requires_gae_auth
def fetch_column_object_counts_by_value() -> Tuple[str, HTTPStatus]:
    table = request.json["table"]
    column = request.json["column"]
    return jsonify_ingest_metadata_result(
        ingest_metadata_store.fetch_column_object_counts_by_value(table, column)
    )


@admin_panel.route(
    "/api/ingest_metadata/fetch_table_nonnull_counts_by_column", methods=["POST"]
)
@requires_gae_auth
def fetch_table_nonnull_counts_by_column() -> Tuple[str, HTTPStatus]:
    table = request.json["table"]
    return jsonify_ingest_metadata_result(
        ingest_metadata_store.fetch_table_nonnull_counts_by_column(table)
    )


@admin_panel.route(
    "/api/ingest_metadata/fetch_object_counts_by_table", methods=["POST"]
)
@requires_gae_auth
def fetch_object_counts_by_table() -> Tuple[str, int]:
    return jsonify_ingest_metadata_result(
        ingest_metadata_store.fetch_object_counts_by_table()
    )


# Data freshness
@admin_panel.route("/api/ingest_metadata/data_freshness", methods=["POST"])
@requires_gae_auth
def fetch_data_freshness() -> Tuple[str, HTTPStatus]:
    return jsonify(ingest_metadata_store.data_freshness_results), HTTPStatus.OK


# GCS CSV -> Cloud SQL Import
@admin_panel.route("/api/case_triage/fetch_etl_view_ids", methods=["POST"])
@requires_gae_auth
def fetch_etl_view_ids() -> Tuple[str, HTTPStatus]:
    return (
        jsonify([builder.view_id for builder in CASE_TRIAGE_EXPORTED_VIEW_BUILDERS]),
        HTTPStatus.OK,
    )


@admin_panel.route("/api/case_triage/run_gcs_import", methods=["POST"])
@requires_gae_auth
def run_gcs_import() -> Tuple[str, HTTPStatus]:
    """Executes an import of data from Google Cloud Storage into Cloud SQL,
    based on the query parameters in the request."""
    if "viewIds" not in request.json:
        return "`viewIds` must be present in arugment list", HTTPStatus.BAD_REQUEST

    known_view_builders = {
        builder.view_id: builder for builder in CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
    }
    for view_id in request.json["viewIds"]:
        if view_id not in known_view_builders:
            logging.warning(
                "Unexpected view_id (%s) found in call to run_gcs_import", view_id
            )
            continue

        # NOTE: We are currently taking advantage of the fact that the destination table name
        # matches the view id of the corresponding builder here. This invariant isn't enforced
        # in code (yet), but the aim is to preserve this invariant for as long as possible.
        import_gcs_csv_to_cloud_sql(
            view_id,
            GcsfsFilePath.from_absolute_path(
                os.path.join(
                    CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI.format(
                        project_id=metadata.project_id()
                    ),
                    f"{view_id}.csv",
                )
            ),
            known_view_builders[view_id].columns,
        )
        logging.info("View (%s) successfully exported", view_id)

    return "", HTTPStatus.OK


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
