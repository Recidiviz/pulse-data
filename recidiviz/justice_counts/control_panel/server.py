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

import pandas as pd
from flask import Flask, Response, make_response, request, send_from_directory, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy_session import current_session
from flask_wtf.csrf import CSRFProtect
from werkzeug.middleware.proxy_fix import ProxyFix

from recidiviz.justice_counts.control_panel.config import Config
from recidiviz.justice_counts.control_panel.error_handlers import (
    register_error_handlers,
)
from recidiviz.justice_counts.control_panel.routes.api import get_api_blueprint
from recidiviz.justice_counts.control_panel.routes.auth import get_auth_blueprint
from recidiviz.justice_counts.exceptions import JusticeCountsBadRequestError
from recidiviz.justice_counts.feed import FeedInterface
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
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
    source: https://flask.palletsprojects.com/en/2.0.x/patterns/appfactories/#using-applications
    """
    logging.getLogger().setLevel(logging.INFO)

    # We use Flask to serve not only the backend, but also our React frontend,
    # which lives in ../../frontends/justice_counts/control_panel
    static_folder = os.path.abspath(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "../../../frontends/justice-counts/control-panel/build/",
        )
    )
    if in_development():
        # Set the project to recidiviz-staging if we are running the application locallly.
        set_development_project_id_override(GCP_PROJECT_STAGING)
    app = Flask(__name__, static_folder=static_folder)
    config = config or Config()
    app.config.from_object(config)
    app.secret_key = get_secret("justice_counts_secret_key")
    setup_scoped_sessions(
        app=app, schema_type=config.SCHEMA_TYPE, database_url_override=config.DB_URL
    )
    app.register_blueprint(
        get_api_blueprint(
            auth_decorator=config.AUTH_DECORATOR,
            auth0_client=config.AUTH0_CLIENT,
            secret_key=app.secret_key,
        ),
        url_prefix="/api",
    )
    app.register_blueprint(
        get_auth_blueprint(
            auth_decorator=config.AUTH_DECORATOR,
        ),
        url_prefix="/auth",
    )

    # Need to silence mypy error `Cannot assign to a method`
    app.wsgi_app = ProxyFix(app.wsgi_app)  # type: ignore[assignment]
    CSRFProtect(app)
    register_error_handlers(app)

    Limiter(
        app,
        key_func=get_remote_address,
        default_limits=["15 per second"],
        # TODO(#13437) Add Redis storage backend for rate limiting
    )

    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MiB max body size

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
        # TODO(#13905) Turn off report-only after testing that changes aren't reporting any issues
        response.headers["Content-Security-Policy-Report-Only"] = (
            "default-src 'self' *.run.app https://recidiviz-justice-counts-staging.us.auth0.com https://recidiviz-justice-counts.us.auth0.com;"
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
    def get_feed_for_agency_id(agency_id: str) -> Response:
        """Returns a feed for an agency, formatted according to the Technical Specification."""
        system_to_filename_to_rows = FeedInterface.get_feed_for_agency_id(
            current_session, agency_id=int(agency_id)
        )

        metric = request.args.get("metric")
        system: Optional[str]
        if len(system_to_filename_to_rows) == 1:
            # If the agency has only provided for one system,
            # no need to specify `system` parameter
            system = list(system_to_filename_to_rows.items())[0][0]
        else:
            system = request.args.get("system")

        # Invalid state: metric parameter is present, but not system
        # Since some metrics are present in multiple systems, we can't
        # figure out which data to render
        if metric and not system:
            raise JusticeCountsBadRequestError(
                code="justice_counts_bad_request",
                description="If the `metric` parameter is specified and the agency is "
                "multi-system, then you must also provide the `system` parameter.",
            )

        # Valid state: metric parameter is not present, system may or may not be
        # Return plaintext list of rows, one for each <system, metric> pair
        if not metric:
            rows = []
            for current_system, filename_to_rows in system_to_filename_to_rows.items():
                if system and current_system != system:
                    # If system parameter is present, skip over systems that don't match
                    continue

                for metric_name in sorted(list(filename_to_rows.keys())):
                    metric_url = url_for(
                        "get_feed_for_agency_id",
                        agency_id=agency_id,
                        system=current_system,
                        metric=metric_name,
                    )
                    rows.append(
                        {
                            "system": current_system,
                            "metric_name": metric_name,
                            "metric_url": metric_url,
                        }
                    )

        # Valid state: both metric and system parameters are present
        # Return plaintext feed of metric rows
        elif system and metric:
            rows = system_to_filename_to_rows[system][metric]

        df = pd.DataFrame.from_dict(rows)
        csv = df.to_csv(index=False)

        feed_response = make_response(csv)
        feed_response.headers["Content-type"] = "text/plain"
        return feed_response

    # Based on this answer re: serving React app with Flask:
    # https://stackoverflow.com/a/45634550
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def index(path: str = "") -> Response:
        if path != "" and os.path.exists(os.path.join(static_folder, path)):
            return send_from_directory(static_folder, path)

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
