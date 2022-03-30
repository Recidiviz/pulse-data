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
import os
from http import HTTPStatus
from typing import List, Optional, Tuple

from flask import Blueprint, Flask, Response, send_from_directory
from flask_wtf.csrf import CSRFProtect

from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.routes.api import get_api_blueprint
from recidiviz.justice_counts.control_panel.routes.auth import get_auth_blueprint
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.utils.auth.auth0 import passthrough_authorization_decorator
from recidiviz.utils.secrets import get_local_secret


def get_blueprints_for_justice_counts_documentation() -> List[Tuple[Blueprint, str]]:

    return [
        (
            get_api_blueprint(passthrough_authorization_decorator()),
            "/justice_counts/api",
        ),
        (get_auth_blueprint(), "/justice_counts/auth"),
    ]


def create_app(config: Optional[Config] = None) -> Flask:
    """
    This function is known as the application factory. It is responsible
    for returning the Flask app instance. Any configuration, registration,
    and other setup the application needs should happen inside the function,
    and then the application will be returned.

    Note that we don't have to explicitly call `create_app` anywhere;
    Flask will automatically detect the factory function during `flask run`
    source: https://flask.palletsprojects.com/en/2.0.x/patterns/appfactories/#using-applications
    """
    # We use Flask to serve not only the backend, but also our React frontend,
    # which lives in ../../frontends/justice_counts/control_panel
    static_folder = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "../../../frontends/justice-counts/control-panel/build/",
        )
    )

    local_path = os.path.join(
        os.path.realpath(os.path.dirname(os.path.realpath(__file__))), "local"
    )
    app = Flask(__name__, static_folder=static_folder)

    config = config or Config()
    app.config.from_object(config)
    app.secret_key = get_local_secret(local_path, "justice_counts_secret_key")
    setup_scoped_sessions(
        app=app, database_key=config.DATABASE_KEY, db_url=config.DB_URL
    )
    app.register_blueprint(get_api_blueprint(config.AUTH_DECORATOR), url_prefix="/api")
    app.register_blueprint(get_auth_blueprint(), url_prefix="/auth")

    CSRFProtect(app)

    @app.route("/health")
    def health() -> Tuple[str, HTTPStatus]:
        """This just returns 200, and is used by Docker to verify that the app is running."""

        return "", HTTPStatus.OK

    # Based on this answer re: serving React app with Flask:
    # https://stackoverflow.com/a/45634550
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def index(path: str = "") -> Response:
        if path != "" and os.path.exists(os.path.join(static_folder, path)):
            return send_from_directory(static_folder, path)

        return send_from_directory(static_folder, "index.html")

    return app
