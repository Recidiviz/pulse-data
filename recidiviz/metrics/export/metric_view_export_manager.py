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
    python -m recidiviz.big_query.export.metrics.metric_view_export_manager --project_id [PROJECT_ID]
"""
import argparse
import logging
import sys
from typing import Dict, List

from google.cloud import storage

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.big_query_view_exporter import BigQueryViewExporter, JsonLinesBigQueryViewExporter
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import DirectIngestGCSFileSystemImpl
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder, MetricBigQueryView

from recidiviz.calculator.query.state import view_config
from recidiviz.big_query.export.composite_big_query_view_exporter import CompositeBigQueryViewExporter
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import \
    OptimizedMetricBigQueryViewExporter
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override


class ExportMetricBigQueryViewConfig(ExportBigQueryViewConfig[MetricBigQueryView]):
    """Extends the ExportBigQueryViewConfig specifically for MetricBigQueryView."""


def export_view_data_to_cloud_storage(view_exporter: BigQueryViewExporter = None):
    """Exports data in BigQuery metric views to cloud storage buckets.

    Optionally takes in a BigQueryViewExporter for performing the export operation. If none is provided, this defaults
    to using a CompositeBigQueryViewExporter with delegates of JsonLinesBigQueryViewExporter and
    OptimizedMetricBigQueryViewExporter.
    """

    view_builders_for_views_to_update = view_config.VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE
    view_update_manager.create_dataset_and_update_views_for_view_builders(view_builders_for_views_to_update)

    if not view_exporter:
        bq_client = BigQueryClientImpl()
        view_exporter = CompositeBigQueryViewExporter(
            bq_client,
            DirectIngestGCSFileSystemImpl(storage.Client()),
            [JsonLinesBigQueryViewExporter(bq_client), OptimizedMetricBigQueryViewExporter(bq_client)],
        )

    project_id = metadata.project_id()
    datasets_to_export = view_config.DATASETS_STATES_AND_VIEW_BUILDERS_TO_EXPORT.keys()

    for dataset_id in datasets_to_export:
        states_and_metrics_to_export: Dict[str, List[MetricBigQueryViewBuilder]] = \
            view_config.DATASETS_STATES_AND_VIEW_BUILDERS_TO_EXPORT[dataset_id]

        output_directory_template = view_config.OUTPUT_DIRECTORY_TEMPLATE_FOR_DATASET_EXPORT.get(dataset_id)

        if not states_and_metrics_to_export or not output_directory_template:
            raise ValueError(f"Trying to export views from an unsupported dataset: {dataset_id}")

        views_to_export = [
            ExportMetricBigQueryViewConfig(
                view=view,
                view_filter_clause=f" WHERE state_code = '{state_code}'",
                intermediate_table_name=f"{view.export_view_name}_table_{state_code}",
                output_directory=GcsfsDirectoryPath.from_absolute_path(
                    output_directory_template.format(
                        project_id=project_id,
                        state_code=state_code,
                    )
                ),
            )
            for state_code, view_builders in states_and_metrics_to_export.items()
            for view in [vb.build() for vb in view_builders]
        ]

        view_exporter.export(views_to_export)


def parse_arguments(argv):
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
