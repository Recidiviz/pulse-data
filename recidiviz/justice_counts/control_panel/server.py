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
import logging
import os
from http import HTTPStatus
from typing import Optional, Tuple

import sentry_sdk
from flask import Flask, Response, request, send_from_directory
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sentry_sdk.integrations.flask import FlaskIntegration
from werkzeug.middleware.proxy_fix import ProxyFix

from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.error_handlers import (
    register_error_handlers,
)
from recidiviz.justice_counts.control_panel.routes.admin import get_admin_blueprint
from recidiviz.justice_counts.control_panel.routes.auth import get_auth_blueprint
from recidiviz.justice_counts.control_panel.utils import (
    initialize_flask_server_debugger,
)
from recidiviz.justice_counts.feed import FeedInterface
from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.sqlalchemy_flask_utils import (
    current_session,
    setup_scoped_sessions,
)
from recidiviz.utils import structured_logging
from recidiviz.utils.environment import (
    GCP_PROJECT_STAGING,
    get_gcp_environment,
    in_development,
    in_gcp,
)
from recidiviz.utils.metadata import set_development_project_id_override
from recidiviz.utils.secrets import get_secret

os.environ.setdefault("APP_URL", "http://localhost:3000")


def create_app(config: Optional[Config] = None) -> Flask:
    """
    This function is known as the application factory. It is responsible
    for returning the Flask app instance. Any configuration, registration,
    and other setup the application needs should happen inside the function,
    and then the application will be returned.

    Note that we don't have to explicitly call `create_app` anywhere;
    Flask will automatically detect the factory function during `flask run`
    source:  https://flask.palletsprojects.com/en/2.0.x/patterns/appfactories/#using-applications
    """
    logging.getLogger().setLevel(logging.INFO)

    # We use Flask to serve not only the backend, but also the React frontend.
    # This API actually powers two different frontends: Publisher and Agency Dashboards,
    # both of which live in the justice-counts GH repo but are pulled into the same
    # Docker image as the backend during deployment (see Dockerfile.justice-counts).
    # We choose which one to serve based on the FRONTEND_APP_DIR environment variable.
    frontend_app_dir = os.getenv("FRONTEND_APP_DIR")
    static_folder = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            f"../../../frontends/justice-counts/{frontend_app_dir}/build/",
        )
    )

    if in_development():
        initialize_flask_server_debugger()

        # Set the project to recidiviz-staging if we are running the application locallly.
        set_development_project_id_override(GCP_PROJECT_STAGING)

    if in_gcp():
        structured_logging.setup()
        # pylint: disable=abstract-class-instantiated
        sentry_sdk.init(
            dsn=JUSTICE_COUNTS_SENTRY_DSN,
            integrations=[FlaskIntegration()],
            traces_sample_rate=1.0,
            environment=get_gcp_environment(),
            # profile 10% of transactions
            profiles_sample_rate=0.1,
        )

    app = Flask(__name__, static_folder=static_folder)
    config = config or Config()
    app.config.from_object(config)
    app.secret_key = get_secret("justice_counts_secret_key")
    setup_scoped_sessions(
        app=app,
        schema_type=config.SCHEMA_TYPE,
        database_url_override=config.DB_URL,
        secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
    )
    app.register_blueprint(
        get_auth_blueprint(
            auth_decorator=config.AUTH_DECORATOR,
        ),
        url_prefix="/auth",
    )
    app.register_blueprint(
        get_admin_blueprint(
            auth0_client=config.AUTH0_CLIENT,
            auth_decorator=config.AUTH_DECORATOR_ADMIN_PANEL,
        ),
        url_prefix="/admin",
    )

    # Need to silence mypy error `Cannot assign to a method`
    app.wsgi_app = ProxyFix(app.wsgi_app)  # type: ignore[assignment]
    register_error_handlers(app)

    Limiter(
        key_func=get_remote_address,
        app=app,
        default_limits=["15 per second"],
        # TODO(#13437) Add Redis storage backend for rate limiting
    )

    app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024 * 1024  # 2 GB max body size

    if not in_development():
        app.config["SESSION_COOKIE_HTTPONLY"] = True
        app.config["SESSION_COOKIE_SECURE"] = True
        app.config["SESSION_COOKIE_SAMESITE"] = "Strict"

    # Security headers
    @app.after_request  # type: ignore
    def set_headers(response: Response) -> Response:
        if not in_development():
            response.headers[
                "Strict-Transport-Security"
            ] = "max-age=63072000; includeSubDomains"  # max age of 2 years
        response.headers["Content-Security-Policy"] = (
            "default-src 'self' *.run.app https://recidiviz-justice-counts-staging.us.auth0.com https://recidiviz-justice-counts.us.auth0.com https://cdn.segment.com/ https://api.segment.io;"
            "object-src 'none'; "
            "img-src * data:; "
            # TODO(#13507) Replace unsafe-inline for style-src and script-src with a nonce
            "style-src 'self' https://fonts.googleapis.com 'unsafe-inline'; "
            "script-src 'self' *.run.app https://recidiviz-justice-counts-staging.us.auth0.com https://recidiviz-justice-counts.us.auth0.com https://cdn.segment.com;"
            "font-src 'self' https://fonts.gstatic.com; "
            "worker-src blob:; "
            "frame-ancestors 'none'; "
            "upgrade-insecure-requests; "
            "block-all-mixed-content"
        )

        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"

        # Set cache control to no-store if it isn't already set
        if "Cache-Control" not in response.headers:
            response.headers["Cache-Control"] = "no-store, max-age=0"

        return response

    @app.route("/health")
    def health() -> Tuple[str, HTTPStatus]:
        """This just returns 200, and is used by Docker to verify that the app is running."""

        return "", HTTPStatus.OK

    @app.route("/feed/<agency_id>", methods=["GET"])
    def get_feed_for_agency_id(agency_id: int) -> Response:
        """
        Returns a feed for an agency, formatted according to the Technical Specification.
        This is a public endpoint used by the agency dashboards.
        """
        return FeedInterface.get_csv_of_feed(
            session=current_session,
            agency_id=agency_id,
            metric=request.args.get("metric"),
            include_unpublished_data=False,
            system=request.args.get("system"),
        )

    # Based on this answer re: serving React app with Flask:
    # https://stackoverflow.com/a/45634550
    @app.route("/<path:path>")
    def index_with_path(path: str = "") -> Response:
        if path != "" and os.path.exists(os.path.join(static_folder, path)):
            return send_from_directory(static_folder, path)

        return send_from_directory(static_folder, "index.html")

    @app.route("/")
    def index() -> Response:
        return send_from_directory(static_folder, "index.html")

    @app.route("/app_public_config.js")
    def app_public_config() -> Response:
        logging.info("get_remote_address: %s", get_remote_address())

        # Expose ONLY the necessary variables to configure our Auth0 frontend
        auth0_config = app.config["AUTH0_CONFIGURATION"]
        segment_key = app.config["SEGMENT_KEY"]
        return Response(
            f"window.APP_CONFIG = {auth0_config.as_public_config()}; window.SEGMENT_KEY = '{segment_key}';",
            mimetype="application/javascript",
        )

    return app
