# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Entrypoint for the application."""
import datetime
import gc
import logging
from http import HTTPStatus
from typing import Tuple

import zope.event.classhandler
from flask import Flask, request
from flask_smorest import Api
from gevent import events
from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.ext.stackdriver import trace_exporter as stackdriver_trace
from opencensus.trace import base_exporter, config_integration, file_exporter, samplers
from opencensus.trace.propagation import google_cloud_format

from recidiviz.admin_panel.admin_stores import initialize_admin_stores
from recidiviz.admin_panel.all_routes import admin_panel_blueprint
from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.server_blueprint_registry import (
    default_blueprints_with_url_prefixes,
    flask_smorest_api_blueprints_with_url_prefixes,
)
from recidiviz.server_config import initialize_engines, initialize_scoped_sessions
from recidiviz.utils import environment, metadata, monitoring, structured_logging, trace
from recidiviz.utils.auth.gae import requires_gae_auth

structured_logging.setup()

logging.info("[%s] Running server.py", datetime.datetime.now().isoformat())

app = Flask(__name__)

# TODO(#24741): Remove once admin panel migration is completed
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
service_type = environment.get_service_type()

if service_type is environment.ServiceType.DEFAULT:
    for blueprint, url_prefix in default_blueprints_with_url_prefixes:
        app.register_blueprint(blueprint, url_prefix=url_prefix)
    for blueprint, url_prefix in flask_smorest_api_blueprints_with_url_prefixes:
        api.register_blueprint(blueprint, url_prefix=url_prefix)
else:
    raise ValueError(f"Unsupported service type: {service_type}")


# TODO(#24741): Remove once admin panel migration is completed
@admin_panel_blueprint.before_request
@auth_endpoint_blueprint.before_request
@requires_gae_auth
def authorization_middleware() -> None:
    pass


# Export traces and metrics to stackdriver if running in GCP
if environment.in_gcp():
    monitoring.register_stackdriver_exporter()
    trace_exporter: base_exporter.Exporter = stackdriver_trace.StackdriverExporter(
        project_id=metadata.project_id(), transport=AsyncTransport
    )
    trace_sampler: samplers.Sampler = trace.CompositeSampler(
        {
            "/direct/extract_and_merge": samplers.AlwaysOnSampler(),
        },
        # For other requests, trace 1 in 20.
        default_sampler=samplers.ProbabilitySampler(rate=0.05),
    )
else:
    trace_exporter = file_exporter.FileExporter(file_name="traces")
    trace_sampler = samplers.AlwaysOnSampler()

middleware = FlaskMiddleware(
    app,
    excludelist_paths=["metadata", "computeMetadata"],  # Don't trace metadata requests
    sampler=trace_sampler,
    exporter=trace_exporter,
    propagator=google_cloud_format.GoogleCloudFormatPropagator(),
)
config_integration.trace_integrations(
    [
        "google_cloud_clientlibs",
        "requests",
        "sqlalchemy",
    ]
)

# TODO(#24741): Remove in_development initializers once admin panel migration is completed
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


if environment.in_development() or environment.in_gcp():
    # Initialize datastores for the admin panel and trigger a data refresh. This call
    # will crash unless the project_id is set globally, which is not the case when
    # running in CI.
    initialize_admin_stores()


@app.route("/health")
def health() -> Tuple[str, HTTPStatus]:
    """This just returns 200, and is used by Docker and GCP uptime checks to verify that the flask workers are
    up and serving requests."""
    return "", HTTPStatus.OK


@zope.event.classhandler.handler(events.MemoryUsageThresholdExceeded)
def memory_condition_handler(event: events.MemoryUsageThresholdExceeded) -> None:
    logging.warning(
        "Memory usage %d is more than limit of %d, forcing gc",
        event.mem_usage,
        event.max_allowed,
    )
    gc.collect()


@zope.event.classhandler.handler(events.EventLoopBlocked)
def blocked_condition_handler(event: events.EventLoopBlocked) -> None:
    logging.warning(
        "Worker blocked for more than %d seconds [greenlet: %s]:\n%s",
        event.blocking_time,
        str(event.greenlet),
        "\n".join(event.info),
    )


@app.before_request
def log_request_entry() -> None:
    logging.getLogger(structured_logging.RECIDIVIZ_BEFORE_REQUEST_LOG).info(
        "%s %s", request.method, request.full_path
    )
