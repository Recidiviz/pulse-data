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
import json
import os
from typing import Dict

import sentry_sdk
from flask import Flask, Response, g, send_from_directory, session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
from jwt.exceptions import MissingRequiredClaimError
from sentry_sdk.integrations.flask import FlaskIntegration

from recidiviz.case_triage.admin_flask_views import RefreshAuthStore
from recidiviz.case_triage.analytics import CaseTriageSegmentClient
from recidiviz.case_triage.api_routes import (
    IMPERSONATED_EMAIL_KEY,
    create_api_blueprint,
)
from recidiviz.case_triage.auth_routes import create_auth_blueprint
from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.e2e_routes import e2e_blueprint
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.case_triage.exceptions import CaseTriageAuthorizationError
from recidiviz.case_triage.redis_sessions import RedisSessionInterface
from recidiviz.case_triage.user_context import UserContext
from recidiviz.case_triage.util import (
    get_local_secret,
    get_rate_limit_storage_uri,
    get_redis_connection_options,
    get_sessions_redis,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions
from recidiviz.utils.auth.auth0 import (
    Auth0Config,
    AuthorizationError,
    TokenClaims,
    build_auth0_authorization_decorator,
    get_userinfo,
)
from recidiviz.utils.environment import in_development, in_gcp, in_test
from recidiviz.utils.timer import RepeatedTimer

# Sentry setup
if in_gcp():
    sentry_sdk.init(
        # not a secret!
        dsn="https://1aa10e823cad49d9a662d71cedb3365b@o432474.ingest.sentry.io/5623757",
        integrations=[FlaskIntegration()],
        # This value may need to be adjusted over time as usage increases.
        traces_sample_rate=1.0,
    )

# Flask setup
static_folder = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../frontends/case-triage/build/",
    )
)

app = Flask(__name__, static_folder=static_folder)
app.secret_key = get_local_secret("case_triage_secret_key")

sessions_redis = get_sessions_redis()

if sessions_redis:
    # We have to ignore mypy here because the Flask source code here (as of version 2.0.2)
    # types `session_interface` as `SecureCookieSessionInterface` instead of
    # `SessionInterface`.
    app.session_interface = RedisSessionInterface(sessions_redis)  # type: ignore[assignment]

CSRFProtect(app).exempt(e2e_blueprint)
register_error_handlers(app)


limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["15 per second"],
    storage_uri=get_rate_limit_storage_uri(),
    storage_options=get_redis_connection_options(),
)

if in_development():
    db_url = os.environ.get("CASE_TRIAGE_DEVELOPMENT_POSTGRES_URL")
else:
    db_url = SQLAlchemyEngineManager.get_server_postgres_instance_url(
        database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
    )
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Strict"

app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MiB max body size
setup_scoped_sessions(
    app, SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE), db_url
)


# Auth setup
def _get_userinfo_from_token(claims: TokenClaims) -> Dict[str, str]:
    try:
        return get_userinfo(claims)
    except MissingRequiredClaimError as e:
        raise AuthorizationError(
            code="invalid_claims", description="claims must include email address"
        ) from e


def on_successful_authorization(jwt_claims: TokenClaims) -> None:
    """
    Memoize the user's info (email_address, picture, etc) into our session
    """

    # Populate the session with user information; This could have changed since the last request
    if session.get("jwt_sub", None) != jwt_claims["sub"]:
        session["jwt_sub"] = jwt_claims["sub"]
        session["user_info"] = _get_userinfo_from_token(jwt_claims)
        # Also pop the impersonated email key if it exists, since the request could've been an impersonation request prior.
        if IMPERSONATED_EMAIL_KEY in session:
            session.pop(IMPERSONATED_EMAIL_KEY)

    auth_error = CaseTriageAuthorizationError(
        code="no_case_triage_access",
        description="You are not authorized to access this application",
    )

    if "email" not in session["user_info"]:
        # This happens when API routes are hit with well-formed but
        # invalid authorization tokens, or if a previous error in populating user_info
        # was stored on the session.

        # to recover from errors, try refreshing user info
        session["user_info"] = _get_userinfo_from_token(jwt_claims)

        # if that didn't work, deny access
        if "email" not in session["user_info"]:
            raise auth_error

    email = session["user_info"]["email"].lower()
    # TODO(PyCQA/pylint#5317): Remove ignore fixed by PyCQA/pylint#5457
    g.user_context = UserContext(  # pylint: disable=assigning-non-slot
        email, authorization_store, jwt_claims=jwt_claims
    )
    if (
        not g.user_context.access_permissions.can_access_case_triage
        and not g.user_context.access_permissions.can_access_leadership_dashboard
    ):
        raise auth_error


auth0_configuration = get_local_secret("case_triage_auth0")

if not auth0_configuration:
    raise ValueError("Missing Case Triage Auth0 configuration secret")

authorization_store = AuthorizationStore()
authorization_config = Auth0Config.from_config_json(json.loads(auth0_configuration))
requires_authorization = build_auth0_authorization_decorator(
    authorization_config, on_successful_authorization
)

store_refresh = RepeatedTimer(
    15 * 60,
    authorization_store.refresh,
    run_immediately_synchronously=True,
)

if not in_test():
    store_refresh.start()


# Security headers
@app.after_request
def set_headers(response: Response) -> Response:
    if not in_development():
        response.headers[
            "Strict-Transport-Security"
        ] = "max-age=63072000"  # max age of 2 years
    response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
    response.headers["X-Frame-Options"] = "DENY"

    # Recidiviz-specific version header
    response.headers["X-Recidiviz-Current-Version"] = os.getenv("CURRENT_GIT_SHA", "")

    # Set cache control to no-store if it isn't already set
    if "Cache-Control" not in response.headers:
        response.headers["Cache-Control"] = "no-store, max-age=0"

    return response


# Segment setup
write_key = os.getenv("SEGMENT_WRITE_KEY", "")
segment_client = CaseTriageSegmentClient(write_key)


# Routes & Blueprints
api_blueprint = create_api_blueprint(segment_client, requires_authorization)
auth_blueprint = create_auth_blueprint(authorization_config)

app.register_blueprint(api_blueprint, url_prefix="/api")
app.register_blueprint(auth_blueprint, url_prefix="/auth")
app.register_blueprint(e2e_blueprint, url_prefix="/e2e")

app.add_url_rule(
    "/refresh_auth_store",
    view_func=RefreshAuthStore.as_view(
        "refresh_auth_store",
        redirect_url="/",
        authorization_store=authorization_store,
        authorization_decorator=requires_authorization,
    ),
)


@app.route("/auth0_public_config.js")
def auth0_public_config() -> str:
    # Expose ONLY the necessary variables to configure our Auth0 frontend
    return f"window.AUTH0_CONFIG = {authorization_config.as_public_config()};"


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def index(path: str = "") -> Response:
    if path != "" and os.path.exists(os.path.join(static_folder, path)):
        return send_from_directory(static_folder, path)

    return send_from_directory(static_folder, "index.html")
