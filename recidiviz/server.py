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
from typing import List, Tuple

import zope.event.classhandler
from flask import Blueprint, Flask, request
from gevent import events
from opencensus.common.transports.async_ import AsyncTransport
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.ext.stackdriver import trace_exporter as stackdriver_trace
from opencensus.trace import base_exporter, config_integration, file_exporter, samplers
from opencensus.trace.propagation import google_cloud_format

from recidiviz.admin_panel.all_routes import admin_panel
from recidiviz.auth.auth_endpoint import auth_endpoint_blueprint
from recidiviz.backup.backup_manager import backup_manager_blueprint
from recidiviz.calculator.calculation_data_storage_manager import (
    calculation_data_storage_manager_blueprint,
)
from recidiviz.case_triage.ops_routes import case_triage_ops_blueprint
from recidiviz.ingest.aggregate.parse import aggregate_parse_blueprint
from recidiviz.ingest.aggregate.scrape_aggregate_reports import (
    scrape_aggregate_reports_blueprint,
)
from recidiviz.ingest.aggregate.single_count import store_single_count_blueprint
from recidiviz.ingest.direct.direct_ingest_control import direct_ingest_control
from recidiviz.ingest.justice_counts.control import justice_counts_control
from recidiviz.ingest.scrape.infer_release import infer_release_blueprint
from recidiviz.ingest.scrape.scraper_control import scraper_control
from recidiviz.ingest.scrape.scraper_status import scraper_status
from recidiviz.ingest.scrape.worker import worker
from recidiviz.metrics.export.view_export_manager import export_blueprint
from recidiviz.persistence.batch_persistence import batch_blueprint
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_control import (
    cloud_sql_to_bq_blueprint,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.server_config import database_keys_for_schema_type
from recidiviz.utils import environment, metadata, monitoring, structured_logging, trace
from recidiviz.validation.validation_manager import validation_manager_blueprint

structured_logging.setup()
logging.info("[%s] Running server.py", datetime.datetime.now().isoformat())

app = Flask(__name__)

service_type = environment.get_service_type()

scraper_blueprints_with_url_prefixes: List[Tuple[Blueprint, str]] = [
    (batch_blueprint, "/batch"),
    (aggregate_parse_blueprint, "/aggregate"),
    (infer_release_blueprint, "/infer_release"),
    (scraper_control, "/scraper"),
    (scraper_status, "/scraper"),
    (worker, "/scraper"),
    (scrape_aggregate_reports_blueprint, "/scrape_aggregate_reports"),
    (store_single_count_blueprint, "/single_count"),
]

default_blueprints_with_url_prefixes: List[Tuple[Blueprint, str]] = [
    (admin_panel, "/admin"),
    (auth_endpoint_blueprint, "/auth"),
    (backup_manager_blueprint, "/backup_manager"),
    (calculation_data_storage_manager_blueprint, "/calculation_data_storage_manager"),
    (case_triage_ops_blueprint, "/case_triage_ops"),
    (cloud_sql_to_bq_blueprint, "/cloud_sql_to_bq"),
    (direct_ingest_control, "/direct"),
    (export_blueprint, "/export"),
    (justice_counts_control, "/justice_counts"),
    (validation_manager_blueprint, "/validation_manager"),
]


def get_blueprints_for_documentation() -> List[Tuple[Blueprint, str]]:
    all_blueprints_with_url_prefixes = (
        scraper_blueprints_with_url_prefixes + default_blueprints_with_url_prefixes
    )

    return all_blueprints_with_url_prefixes


if service_type is environment.ServiceType.SCRAPERS:
    for blueprint, url_prefix in scraper_blueprints_with_url_prefixes:
        app.register_blueprint(blueprint, url_prefix=url_prefix)
elif service_type is environment.ServiceType.DEFAULT:
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
            "/direct/process_job": samplers.AlwaysOnSampler(),
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

if environment.in_gcp():
    # This attempts to connect to all of our databases. Any connections that fail will
    # be logged and not raise an error, so that a single database outage doesn't take
    # down the entire application. Any attempt to use those databases later will
    # attempt to connect again in case the database was just unhealthy.
    if service_type is environment.ServiceType.SCRAPERS:
        schemas = {SchemaType.JAILS}
    elif service_type is environment.ServiceType.DEFAULT:
        schemas = set(SchemaType) - {SchemaType.JAILS}
    else:
        raise ValueError(f"Unsupported service type: {service_type}")

    for schema_type in schemas:
        SQLAlchemyEngineManager.attempt_init_engines_for_databases(
            database_keys_for_schema_type(schema_type)
        )


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
