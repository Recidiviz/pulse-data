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
"""Global registration of all endpoints for the main Recidiviz server backend."""
from typing import List, Tuple

from flask import Blueprint

from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.auth.auth_endpoint import get_auth_endpoint_blueprint
from recidiviz.auth.auth_users_endpoint import get_users_blueprint
from recidiviz.backup.backup_manager import backup_manager_blueprint
from recidiviz.outliers.utils.routes import get_outliers_utils_blueprint
from recidiviz.workflows.etl.routes import get_workflows_etl_blueprint

default_blueprints_with_url_prefixes: List[Tuple[Blueprint, str]] = [
    (backup_manager_blueprint, "/backup_manager"),
    (get_workflows_etl_blueprint(), "/practices-etl"),
    (get_outliers_utils_blueprint(), "/outliers-utils"),
]


def get_blueprints_for_documentation() -> List[Tuple[Blueprint, str]]:
    # TODO(#24741): Add back admin panel / auth blueprints once removed from blueprint registry
    return default_blueprints_with_url_prefixes + [
        (admin_panel_blueprint, "/admin"),
        (
            get_auth_endpoint_blueprint(authentication_middleware=None),
            "/auth",
        ),
        (
            get_users_blueprint(authentication_middleware=None),
            "/auth/users",
        ),
    ]
