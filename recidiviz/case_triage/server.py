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
"""Backend entry point for Case Triage API server."""
import logging
import os
from http import HTTPStatus
from typing import Tuple

import sentry_sdk
from flask import Flask, Response, redirect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
from sentry_sdk.integrations.flask import FlaskIntegration
from werkzeug.middleware.proxy_fix import ProxyFix

import recidiviz
from recidiviz.calculator.query.state.views.dashboard.shared_pathways.pathways_helpers import (
    PATHWAYS_OFFLINE_DEMO_STATE,
)
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.jii.jii_texts_routes import create_jii_api_blueprint
from recidiviz.case_triage.outliers.outliers_routes import create_outliers_api_blueprint
from recidiviz.case_triage.pathways.pathways_routes import create_pathways_api_blueprint
from recidiviz.case_triage.util import (
    get_rate_limit_storage_uri,
    get_redis_connection_options,
)
from recidiviz.case_triage.workflows.workflows_routes import (
    create_workflows_api_blueprint,
)
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import get_pathways_database_entities
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.utils.fixture_helpers import create_dbs, reset_fixtures
from recidiviz.utils import structured_logging
from recidiviz.utils.environment import (
    get_gcp_environment,
    in_development,
    in_gcp,
    in_offline_mode,
)

# Sentry setup
if in_gcp():
    # pylint: disable=abstract-class-instantiated
    sentry_sdk.init(
        # not a secret!
        dsn="https://1aa10e823cad49d9a662d71cedb3365b@o432474.ingest.sentry.io/5623757",
        integrations=[FlaskIntegration()],
        # This value may need to be adjusted over time as usage increases.
        traces_sample_rate=1.0,
        environment=get_gcp_environment(),
    )

# Flask setup
app = Flask(__name__)


# Need to silence mypy error `Cannot assign to a method`
app.wsgi_app = ProxyFix(app.wsgi_app)  # type: ignore[assignment]
register_error_handlers(app)

jii_api_blueprint = create_jii_api_blueprint()

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    default_limits=["15 per second"],
    storage_uri=get_rate_limit_storage_uri(),
    storage_options=get_redis_connection_options(),
)

app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MiB max body size

if not in_development():
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Strict"

# Logging setup
if in_gcp():
    structured_logging.setup()
else:
    # Python logs at the warning level by default
    logging.basicConfig(level=logging.INFO)

if in_offline_mode():

    def initialize_pathways_db_from_fixtures() -> None:
        state_code = PATHWAYS_OFFLINE_DEMO_STATE.value.lower()
        create_dbs([state_code], SchemaType.PATHWAYS)
        db_key = SQLAlchemyDatabaseKey(SchemaType.PATHWAYS, db_name=state_code)
        engine = SQLAlchemyEngineManager.init_engine(db_key)
        logging.info("(re)creating tables")
        # reset_fixtures doesn't handle schema changes, so drop and recreate the tables
        PathwaysBase.metadata.drop_all(engine)
        PathwaysBase.metadata.create_all(engine)
        logging.info("finished (re)creating tables")

        reset_fixtures(
            engine,
            get_pathways_database_entities(),
            os.path.join(os.path.dirname(recidiviz.__file__), "local/fixtures"),
            csv_headers=True,
        )

        logging.info("finished initializing pathways database")

    initialize_pathways_db_from_fixtures()


# Security headers
@app.after_request  # type: ignore
def set_headers(response: Response) -> Response:
    if not in_development():
        response.headers[
            "Strict-Transport-Security"
        ] = "max-age=63072000"  # max age of 2 years
    response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"

    # Recidiviz-specific version header
    response.headers["X-Recidiviz-Current-Version"] = os.getenv("CURRENT_GIT_SHA", "")

    # Set cache control to no-store if it isn't already set
    if "Cache-Control" not in response.headers:
        response.headers["Cache-Control"] = "no-store, max-age=0"

    return response


# Routes & Blueprints
pathways_api_blueprint = create_pathways_api_blueprint()
workflows_blueprint = create_workflows_api_blueprint()
outliers_api_blueprint = create_outliers_api_blueprint()

csrf = CSRFProtect(app)
# Disable CSRF protection for workflows+outliers routes because the session id changes between
# requests to get the CSRF token and subsequent POST requests, with no successful workarounds.
# Since we use a JWT (the Bearer token in the Auth header), we should be protected from CSRF.
# https://security.stackexchange.com/questions/170388/do-i-need-csrf-token-if-im-using-bearer-jwt
csrf.exempt(workflows_blueprint)
csrf.exempt(outliers_api_blueprint)
csrf.exempt(jii_api_blueprint)

app.register_blueprint(pathways_api_blueprint, url_prefix="/pathways")
# Only the pathways endpoints are accessible in offline mode
if not in_offline_mode():
    app.register_blueprint(workflows_blueprint, url_prefix="/workflows")
    app.register_blueprint(outliers_api_blueprint, url_prefix="/outliers")
    app.register_blueprint(jii_api_blueprint, url_prefix="/jii")

    @app.route("/")
    def index() -> Response:
        return redirect("https://dashboard.recidiviz.org")  # type: ignore[return-value]

    @app.route("/health")
    def health() -> Tuple[str, HTTPStatus]:
        """This just returns 200, and is used by Docker and GCP uptime checks to verify that the flask workers are
        up and serving requests."""
        return "", HTTPStatus.OK
