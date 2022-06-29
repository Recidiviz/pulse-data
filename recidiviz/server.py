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
from gevent import events
from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.ext.stackdriver import trace_exporter as stackdriver_trace
from opencensus.trace import base_exporter, config_integration, file_exporter, samplers
from opencensus.trace.propagation import google_cloud_format

from recidiviz.admin_panel.admin_stores import initialize_admin_stores
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.server_blueprint_registry import default_blueprints_with_url_prefixes
from recidiviz.server_config import database_keys_for_schema_type
from recidiviz.utils import environment, metadata, monitoring, structured_logging, trace

structured_logging.setup()

logging.info("[%s] Running server.py", datetime.datetime.now().isoformat())

app = Flask(__name__)

service_type = environment.get_service_type()

if service_type is environment.ServiceType.DEFAULT:
    for blueprint, url_prefix in default_blueprints_with_url_prefixes:
        app.register_blueprint(blueprint, url_prefix=url_prefix)
else:
    raise ValueError(f"Unsupported service type: {service_type}")


# Export traces and metrics to stackdriver if running in GCP
if environment.in_gcp():
    monitoring.register_stackdriver_exporter()
    trace_exporter: base_exporter.Exporter = stackdriver_trace.StackdriverExporter(
        project_id=metadata.project_id(), transport=AsyncTransport
    )
    trace_sampler: samplers.Sampler = trace.CompositeSampler(
        {
            "/direct/extract_and_merge": samplers.AlwaysOnSampler(),
            # There are a lot of scraper requests, so they can use the default rate of 1 in 10k.
            "/scraper/": samplers.ProbabilitySampler(),
            "/scrape_aggregate_reports/": samplers.ProbabilitySampler(),
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
        # TODO(#4283): The 'google_cloud_clientlibs' integration is currently not compatible with the
        # 'proto-plus' objects used by the 2.0.0 versions of the client libraries. Investigate best way to hydrate
        # spans in traces for these calls in the future.
        "google_cloud_clientlibs",
        "requests",
        "sqlalchemy",
    ]
)
if environment.in_development():
    # We can connect to the justice counts / case triage database using the default `init_engine` configurations,
    # which uses secrets in `recidiviz/local`. If you are missing these secrets, run these scripts:
    # ./recidiviz/tools/case_triage/initialize_development_environment.sh
    # ./recidiviz/tools/justice_counts/control_panel/initialize_development_environment.sh

    # If we fail to connect a message will be logged but we won't raise an error.
    enabled_development_schema_types = [
        SchemaType.CASE_TRIAGE,
        SchemaType.JUSTICE_COUNTS,
    ]

    for schema_type in enabled_development_schema_types:
        try:
            SQLAlchemyEngineManager.init_engine(
                SQLAlchemyDatabaseKey.for_schema(schema_type),
            )
        except BaseException as e:
            logging.warning(
                "Could not initialize engine for %s - have you run `initialize_development_environment.sh`?",
                schema_type,
            )

    # We also set the project to recidiviz-staging
    metadata.set_development_project_id_override(environment.GCP_PROJECT_STAGING)
elif environment.in_gcp():
    # This attempts to connect to all of our databases. Any connections that fail will
    # be logged and not raise an error, so that a single database outage doesn't take
    # down the entire application. Any attempt to use those databases later will
    # attempt to connect again in case the database was just unhealthy.
    if service_type is environment.ServiceType.DEFAULT:
        schemas = set(SchemaType) - {SchemaType.JAILS}
    else:
        raise ValueError(f"Unsupported service type: {service_type}")

    for schema_type in schemas:
        SQLAlchemyEngineManager.attempt_init_engines_for_databases(
            database_keys_for_schema_type(schema_type)
        )
if environment.in_development() or environment.in_gcp():
    # Initialize datastores for the admin panel and trigger a data refresh. This call
    # will crash unless the project_id is set globally, which is not the case when
    # running in CI.
    initialize_admin_stores()


@app.route("/health")
def health() -> Tuple[str, HTTPStatus]:
    """This just returns 200, and is used by Docker to verify that the flask workers are
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
    logging.getLogger(structured_logging.BEFORE_REQUEST_LOG).info(
        "%s %s", request.method, request.full_path
    )
