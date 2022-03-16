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
"""Backend entry point for Justice Counts Control Panel backend API."""
from typing import List, Tuple

from flask import Blueprint, Flask
from flask_wtf.csrf import CSRFProtect

from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.routes.api import api_blueprint
from recidiviz.justice_counts.control_panel.routes.auth import auth_blueprint
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions

# Note that we don't have to explicitly call `create_app` anywhere;
# Flask will automatically detect the factory function during `flask run`
# source: https://flask.palletsprojects.com/en/2.0.x/patterns/appfactories/#using-applications


def get_blueprints_for_justice_counts_documentation() -> List[Tuple[Blueprint, str]]:
    return [
        (api_blueprint, "/justice_counts/api"),
        (auth_blueprint, "/justice_counts/auth"),
    ]


def create_app(config: Config) -> Flask:
    app = Flask(__name__)
    app.config.from_object(config)

    setup_scoped_sessions(
        app=app, database_key=app.config["DATABASE_KEY"], db_url=app.config["DB_URL"]
    )

    app.register_blueprint(api_blueprint, url_prefix="/api")
    app.register_blueprint(auth_blueprint, url_prefix="/auth")

    CSRFProtect(app)

    return app
