# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Flask server for the Admin Panel"""
import datetime
import logging
import os
from http import HTTPStatus
from typing import Optional, Tuple

from flask import Flask, send_from_directory
from flask_smorest import Api
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.trace.sampling import Sampler, TraceIdRatioBased
from opentelemetry.trace import set_tracer_provider
from werkzeug import Response

from recidiviz.admin_panel.admin_stores import initialize_admin_stores
from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.admin_panel.constants import LOAD_BALANCER_SERVICE_ID_SECRET_NAME
from recidiviz.admin_panel.routes.lineage import lineage_blueprint
from recidiviz.admin_panel.routes.outliers import outliers_blueprint
from recidiviz.admin_panel.routes.workflows import workflows_blueprint
from recidiviz.auth.auth_endpoint import get_auth_endpoint_blueprint
from recidiviz.auth.auth_users_endpoint import get_users_blueprint
from recidiviz.monitoring.providers import (
    create_monitoring_meter_provider,
    create_monitoring_tracer_provider,
)
from recidiviz.monitoring.trace import CompositeSampler
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.server_config import initialize_engines, initialize_scoped_sessions
from recidiviz.utils import environment, metadata, structured_logging
from recidiviz.utils.auth.gce import build_compute_engine_auth_decorator
from recidiviz.utils.environment import in_gcp, in_gunicorn
from recidiviz.utils.metadata import CloudRunMetadata

structured_logging.setup()

logging.info("[%s] Running server.py", datetime.datetime.now().isoformat())

_STATIC_FOLDER = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../frontends/admin-panel/build/",
    )
)


if in_gcp():
    structured_logging.setup()
    cloud_run_metadata = CloudRunMetadata.build_from_metadata_server(
        CloudRunMetadata.Service.ADMIN_PANEL
    )
else:
    cloud_run_metadata = CloudRunMetadata(
        project_id="123",
        region="us-central1",
        url="http://localhost:5000",
        service_account_email="fake-acct@fake-project.iam.gserviceaccount.com",
    )

app = Flask(__name__)

requires_authorization = build_compute_engine_auth_decorator(
    backend_service_id_secret_name=LOAD_BALANCER_SERVICE_ID_SECRET_NAME
)


@app.before_request
@requires_authorization
def auth_middleware() -> None:
    pass


api = Api(
    app,
    # These are needed for flask-smorests OpenAPI generation. We don't use this right now, so these
    # values can be set to ~anything
    spec_kwargs={
        "title": "default",
        "version": "1.0.0",
        "openapi_version": "3.1.0",
    },
)


app.register_blueprint(admin_panel_blueprint, url_prefix="/admin")
app.register_blueprint(
    get_auth_endpoint_blueprint(
        cloud_run_metadata=cloud_run_metadata,
        authentication_middleware=requires_authorization,
    ),
    url_prefix="/auth",
)
app.register_blueprint(
    get_users_blueprint(authentication_middleware=requires_authorization),
    url_prefix="/auth/users",
)
app.register_blueprint(outliers_blueprint, url_prefix="/admin/outliers")
app.register_blueprint(workflows_blueprint, url_prefix="/admin/workflows")
app.register_blueprint(lineage_blueprint, url_prefix="/admin/lineage")

if environment.in_development():
    # We set the project to recidiviz-staging
    metadata.set_development_project_id_override(environment.GCP_PROJECT_STAGING)

    initialize_scoped_sessions(app)
    initialize_engines(
        schema_types=[
            SchemaType.JUSTICE_COUNTS,
            SchemaType.OPERATIONS,
        ]
    )
elif environment.in_gcp():
    initialize_scoped_sessions(app)
    initialize_engines(schema_types=set(SchemaType))


# OpenTelemetry's MeterProvider and `CloudMonitoringMetricsExporter` are compatible with gunicorn's
# forking mechanism and can be instantiated pre-fork.
meter_provider = create_monitoring_meter_provider()
set_meter_provider(meter_provider)


def initialize_worker_process() -> None:
    """OpenTelemetry's BatchSpanProcessor is not compatible with gunicorn's forking mechanism,
     so our providers must be instantiated per-worker after the worker has been forked. For more information see:
    https://opentelemetry-python.readthedocs.io/en/latest/examples/fork-process-model/README.html
    """
    sampler: Optional[Sampler] = None
    if environment.in_gcp():
        sampler = CompositeSampler(
            {},
            # For other requests, trace 1 in 20.
            default_sampler=TraceIdRatioBased(rate=1 / 20),
        )

    tracer_provider = create_monitoring_tracer_provider(sampler=sampler)

    set_tracer_provider(tracer_provider)


# Called by the configured hook in `gunicorn.conf.py` and `gunicorn.gthread.conf.py`
app.initialize_worker_process = initialize_worker_process  # type: ignore

# Call manually running via the `flask` command and not `gunicorn`
if not in_gunicorn():
    initialize_worker_process()

if environment.in_development() or environment.in_gcp():
    # Initialize datastores for the admin panel and trigger a data refresh. This call
    # will crash unless the project_id is set globally, which is not the case when
    # running in CI.
    initialize_admin_stores()


@app.route("/")
def index() -> Response:
    return send_from_directory(_STATIC_FOLDER, "index.html")


@app.route("/health")
def health() -> Tuple[str, HTTPStatus]:
    """This just returns 200, and is used by Docker and GCP uptime checks to verify that the flask workers are
    up and serving requests."""
    return "", HTTPStatus.OK
