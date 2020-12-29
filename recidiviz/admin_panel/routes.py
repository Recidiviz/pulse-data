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
from typing import Dict, Optional

from flask import Blueprint, Response, jsonify, request, send_from_directory

from recidiviz.admin_panel.ingest_metadata_store import IngestMetadataCountsStore, IngestMetadataResult
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import in_test
from recidiviz.utils.timer import RepeatedTimer

logging.getLogger().setLevel(logging.INFO)
ingest_metadata_store = IngestMetadataCountsStore()

static_folder = os.path.abspath(os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    '../../frontends/admin-panel/build/',
))
admin_panel = Blueprint('admin_panel', __name__, static_folder=static_folder)
store_refresh = RepeatedTimer(15 * 60, ingest_metadata_store.recalculate_store, run_immediately=True)
if not in_test():
    store_refresh.start()


def jsonify_ingest_metadata_result(result: IngestMetadataResult) -> str:
    results_dict: Dict[str, Dict[str, Dict[str, int]]] = {}
    for name, state_map in result.items():
        results_dict[name] = {}
        for state_code, counts in state_map.items():
            results_dict[name][state_code] = counts.to_json()
    return jsonify(results_dict)


@admin_panel.route('/api/ingest_metadata/fetch_column_object_counts_by_value', methods=['POST'])
@requires_gae_auth
def fetch_column_object_counts_by_value() -> str:
    table = request.json['table']
    column = request.json['column']
    return jsonify_ingest_metadata_result(ingest_metadata_store.fetch_column_object_counts_by_value(table, column))


@admin_panel.route('/api/ingest_metadata/fetch_table_nonnull_counts_by_column', methods=['POST'])
@requires_gae_auth
def fetch_table_nonnull_counts_by_column() -> str:
    table = request.json['table']
    return jsonify_ingest_metadata_result(ingest_metadata_store.fetch_table_nonnull_counts_by_column(table))


@admin_panel.route('/api/ingest_metadata/fetch_object_counts_by_table', methods=['POST'])
@requires_gae_auth
def fetch_object_counts_by_table() -> str:
    return jsonify_ingest_metadata_result(ingest_metadata_store.fetch_object_counts_by_table())


@admin_panel.route('/')
@admin_panel.route('/<path:path>')
def fallback(path: Optional[str] = None) -> Response:
    if path is None or not os.path.exists(os.path.join(static_folder, path)):
        logging.info('Rewriting path')
        path = 'index.html'
    return send_from_directory(static_folder, path)
