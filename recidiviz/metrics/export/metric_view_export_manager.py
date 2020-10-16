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
"""Export data from BigQuery metric views to configurable locations.

Run this export locally with the following command:
    python -m recidiviz.metrics.export.metric_view_export_manager --project_id [PROJECT_ID]
"""
import argparse
import logging
import sys
from typing import Tuple, List

from google.cloud import storage

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.big_query_view_exporter import BigQueryViewExporter, JsonLinesBigQueryViewExporter
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystemImpl

from recidiviz.calculator.query.state import view_config
from recidiviz.big_query.export.composite_big_query_view_exporter import CompositeBigQueryViewExporter
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import OptimizedMetricBigQueryViewExporter
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override


def export_view_data_to_cloud_storage(view_exporter: BigQueryViewExporter = None) -> None:
    """Exports data in BigQuery metric views to cloud storage buckets.

    Optionally takes in a BigQueryViewExporter for performing the export operation. If none is provided, this defaults
    to using a CompositeBigQueryViewExporter with delegates of JsonLinesBigQueryViewExporter and
    OptimizedMetricBigQueryViewExporter.
    """
    view_builders_for_views_to_update = view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE
    view_update_manager.create_dataset_and_update_views_for_view_builders(BigQueryViewNamespace.STATE,
                                                                          view_builders_for_views_to_update)

    if not view_exporter:
        bq_client = BigQueryClientImpl()
        view_exporter = CompositeBigQueryViewExporter(
            bq_client,
            GCSFileSystemImpl(storage.Client()),
            [JsonLinesBigQueryViewExporter(bq_client), OptimizedMetricBigQueryViewExporter(bq_client)],
        )

    project_id = metadata.project_id()

    for dataset_export_config in view_config.METRIC_DATASET_EXPORT_CONFIGS:
        view_export_configs = dataset_export_config.export_configs_for_views_to_export(project_id=project_id)
        view_exporter.export(view_export_configs)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
                        required=True)

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        export_view_data_to_cloud_storage()
