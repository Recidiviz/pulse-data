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
import logging
import sys
from typing import List, Set, Tuple, Type

from opencensus.ext.stackdriver.stats_exporter import GLOBAL_RESOURCE_TYPE, Options

from recidiviz.entrypoints.bq_refresh.cloud_sql_to_bq_refresh import (
    BigQueryRefreshEntrypoint,
)
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.metric_export.metric_view_export import (
    MetricViewExportEntrypoint,
)
from recidiviz.entrypoints.monitoring.report_metric_export_timeliness import (
    MetricExportTimelinessEntrypoint,
)
from recidiviz.entrypoints.normalization.update_normalized_state_dataset import (
    UpdateNormalizedStateEntrypoint,
)
from recidiviz.entrypoints.validation.validate import ValidationEntrypoint
from recidiviz.entrypoints.view_update.update_all_managed_views import (
    UpdateAllManagedViewsEntrypoint,
)
from recidiviz.utils import metadata, monitoring

ENTRYPOINTS: Set[Type[EntrypointInterface]] = {
    BigQueryRefreshEntrypoint,
    MetricViewExportEntrypoint,
    MetricExportTimelinessEntrypoint,
    UpdateNormalizedStateEntrypoint,
    ValidationEntrypoint,
    UpdateAllManagedViewsEntrypoint,
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
    monitoring.register_stackdriver_exporter(
        # We register the Stackdriver using the resource type of `global`.
        # The exporter would automatically detect that we are running in a `k8s_container` resource,
        # but we do not want these metrics associated with that resource type.
        # More information can be found here- https://cloud.google.com/monitoring/api/resources
        options=Options(project_id=metadata.project_id(), resource=GLOBAL_RESOURCE_TYPE)
    )
    entrypoint_cls = ENTRYPOINTS_BY_NAME[entrypoint]
    entrypoint_parser = entrypoint_cls.get_parser()
    entrypoint_cls.run_entrypoint(args=entrypoint_parser.parse_args(entrypoint_argv))

    monitoring.wait_for_stackdriver_export()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args, unknown_args = parse_arguments(sys.argv[1:])
    execute_entrypoint(args.entrypoint, entrypoint_argv=unknown_args)
