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
"""Contains functions for executing entrypoints"""
import argparse
import atexit
import logging
import os
import sys
from typing import List, Set, Tuple, Type

import requests
from opentelemetry import trace
from opentelemetry.metrics import set_meter_provider
from opentelemetry.trace import set_tracer_provider

from recidiviz.entrypoints.bq_refresh.cloud_sql_to_bq_refresh import (
    BigQueryRefreshEntrypoint,
)
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.ingest.check_raw_data_flashing_not_in_progress import (
    IngestCheckRawDataFlashingEntrypoint,
)
from recidiviz.entrypoints.ingest.ingest_pipeline_should_run_in_dag import (
    IngestPipelineShouldRunInDagEntrypoint,
)
from recidiviz.entrypoints.metric_export.metric_view_export import (
    MetricViewExportEntrypoint,
)
from recidiviz.entrypoints.monitoring.report_metric_export_timeliness import (
    MetricExportTimelinessEntrypoint,
)
from recidiviz.entrypoints.normalization.update_normalized_state_dataset import (
    UpdateNormalizedStateEntrypoint,
)
from recidiviz.entrypoints.raw_data.divide_raw_file_into_chunks import (
    RawDataFileChunkingEntrypoint,
)
from recidiviz.entrypoints.raw_data.normalize_raw_file_chunks import (
    RawDataChunkNormalizationEntrypoint,
)
from recidiviz.entrypoints.validation.validate import ValidationEntrypoint
from recidiviz.entrypoints.view_update.update_all_managed_views import (
    UpdateAllManagedViewsEntrypoint,
)
from recidiviz.entrypoints.view_update.update_big_query_source_table_schemata_entrypoint import (
    UpdateBigQuerySourceTableSchemataEntrypoint,
)
from recidiviz.monitoring.context import get_current_trace_id
from recidiviz.monitoring.flask_insrumentation import instrument_common_libraries
from recidiviz.monitoring.providers import (
    create_monitoring_meter_provider,
    create_monitoring_tracer_provider,
)
from recidiviz.monitoring.trace import TRACER_NAME
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import set_development_project_id_override

ENTRYPOINTS: Set[Type[EntrypointInterface]] = {
    BigQueryRefreshEntrypoint,
    MetricViewExportEntrypoint,
    MetricExportTimelinessEntrypoint,
    RawDataChunkNormalizationEntrypoint,
    RawDataFileChunkingEntrypoint,
    UpdateBigQuerySourceTableSchemataEntrypoint,
    UpdateNormalizedStateEntrypoint,
    ValidationEntrypoint,
    UpdateAllManagedViewsEntrypoint,
    IngestCheckRawDataFlashingEntrypoint,
    IngestPipelineShouldRunInDagEntrypoint,
}


def get_entrypoint_name(entrypoint: Type[EntrypointInterface]) -> str:
    return entrypoint.__name__.split(".")[-1]


ENTRYPOINTS_BY_NAME = {
    get_entrypoint_name(entrypoint): entrypoint for entrypoint in ENTRYPOINTS
}


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses arguments for the Cloud SQL to BQ refresh process."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--entrypoint",
        help="The entrypoint to run",
        type=str,
        choices=ENTRYPOINTS_BY_NAME.keys(),
        required=True,
    )

    return parser.parse_known_args(argv)


def execute_entrypoint(entrypoint: str, entrypoint_argv: List[str]) -> None:
    tracer = trace.get_tracer(TRACER_NAME)
    entrypoint_cls = ENTRYPOINTS_BY_NAME[entrypoint]
    entrypoint_parser = entrypoint_cls.get_parser()
    entrypoint_args = entrypoint_parser.parse_args(entrypoint_argv)

    with tracer.start_as_current_span(entrypoint) as current_span:
        for key, arg in vars(entrypoint_args).items():
            current_span.set_attribute(key, str(arg))

        try:
            entrypoint_cls.run_entrypoint(args=entrypoint_args)
        finally:
            logging.info(
                "Cloud Trace profile can be found with the following trace id: %s",
                get_current_trace_id(),
            )


def quit_kubernetes_cloud_sql_proxy() -> None:
    """Sends a request to the Cloud SQL Proxy admin control to gracefully shut down"""
    # These environment variables are added to our runtime when a pod is executing with the Cloud SQL Proxy sidecar
    # See recidiviz.airflow.dags.operators.cloud_sql_proxy_sidecar for more information
    cloud_sql_admin_host = os.environ.get("K8S_CLOUD_SQL_PROXY_ADMIN_HOST", None)
    cloud_sql_admin_port = os.environ.get("K8S_CLOUD_SQL_PROXY_ADMIN_PORT", None)

    if cloud_sql_admin_host and cloud_sql_admin_port:
        # Send a request to gracefully shut down the proxy container
        requests.post(
            f"http://{cloud_sql_admin_host}:{cloud_sql_admin_port}/quitquitquit",
            timeout=10,
        )


if __name__ == "__main__":
    if in_development():
        set_development_project_id_override(GCP_PROJECT_STAGING)

    set_meter_provider(create_monitoring_meter_provider())
    set_tracer_provider(create_monitoring_tracer_provider())

    instrument_common_libraries()

    logging.basicConfig(level=logging.INFO)
    atexit.register(quit_kubernetes_cloud_sql_proxy)

    args, unknown_args = parse_arguments(sys.argv[1:])
    execute_entrypoint(args.entrypoint, entrypoint_argv=unknown_args)
