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
from typing import Optional, Tuple

from flask import Blueprint, Response, send_from_directory

from recidiviz.admin_panel.routes.data_freshness import add_data_freshness_routes
from recidiviz.admin_panel.routes.dataset_metadata import add_dataset_metadata_routes
from recidiviz.admin_panel.routes.ingest_ops import add_ingest_ops_routes
from recidiviz.admin_panel.routes.justice_counts_tools import (
    add_justice_counts_tools_routes,
)
from recidiviz.admin_panel.routes.line_staff_tools import add_line_staff_tools_routes
from recidiviz.admin_panel.routes.validation import add_validation_routes
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import (
    in_development,
    in_gcp_production,
    in_gcp_staging,
)

logging.getLogger().setLevel(logging.INFO)

_STATIC_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../frontends/admin-panel/build/",
    )
)

admin_panel_blueprint = Blueprint("admin_panel", __name__, static_folder=_STATIC_FOLDER)
add_line_staff_tools_routes(admin_panel_blueprint)
add_ingest_ops_routes(admin_panel_blueprint)
add_justice_counts_tools_routes(admin_panel_blueprint)
add_validation_routes(admin_panel_blueprint)
add_dataset_metadata_routes(admin_panel_blueprint)
add_data_freshness_routes(admin_panel_blueprint)


# Frontend configuration
@admin_panel_blueprint.route("/runtime_env_vars.js")
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


@admin_panel_blueprint.route("/")
@admin_panel_blueprint.route("/<path:path>")
def fallback(path: Optional[str] = None) -> Response:
    if path is None or not os.path.exists(os.path.join(_STATIC_FOLDER, path)):
        logging.info("Rewriting path")
        path = "index.html"
    return send_from_directory(_STATIC_FOLDER, path)
