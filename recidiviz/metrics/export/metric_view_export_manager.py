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
    python -m recidiviz.metrics.export.metric_view_export_manager --project_id [PROJECT_ID] --state_code [STATE_CODE]
"""
import argparse
import logging
import sys
from typing import Tuple, List, Optional

from opencensus.stats import measure, view as opencensus_view, aggregation

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.big_query_view_exporter import BigQueryViewExporter, JsonLinesBigQueryViewExporter, \
    ViewExportValidationError
from recidiviz.big_query.export.big_query_view_export_validator import JsonLinesBigQueryViewExportValidator
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace

from recidiviz.calculator.query.state import view_config
from recidiviz.big_query.export.composite_big_query_view_exporter import CompositeBigQueryViewExporter
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import OptimizedMetricBigQueryViewExporter
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import \
    OptimizedMetricBigQueryViewExportValidator
from recidiviz.utils import metadata, monitoring
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override

m_failed_metric_export_validation = measure.MeasureInt(
    "bigquery/metric_view_export_manager/metric_view_export_validation_failure",
    "Counted every time a set of exported metric views fails validation", "1")

failed_metric_export_validation_view = opencensus_view.View(
    "bigquery/metric_view_export_manager/num_metric_view_export_validation_failure",
    "The sum of times a set of exported metric views fails validation",
    [monitoring.TagKey.REGION, monitoring.TagKey.METRIC_VIEW_EXPORT_NAME],
    m_failed_metric_export_validation,
    aggregation.SumAggregation())

m_failed_metric_export_job = measure.MeasureInt(
    "bigquery/metric_view_export_manager/metric_view_export_job_failure",
    "Counted every time a set of exported metric views fails for non-validation reasons", "1")

failed_metric_export_view = opencensus_view.View(
    "bigquery/metric_view_export_manager/num_metric_view_export_job_failure",
    "The sum of times a set of exported metric views fails to export for non-validation reasons",
    [monitoring.TagKey.REGION, monitoring.TagKey.METRIC_VIEW_EXPORT_NAME],
    m_failed_metric_export_job,
    aggregation.SumAggregation())

monitoring.register_views([failed_metric_export_validation_view, failed_metric_export_view])


def export_view_data_to_cloud_storage(export_job_filter: Optional[str] = None,
                                      view_exporter: BigQueryViewExporter = None) -> None:
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
        gcsfs_client = GcsfsFactory.build()

        json_exporter = JsonLinesBigQueryViewExporter(bq_client,
                                                      JsonLinesBigQueryViewExportValidator(gcsfs_client))

        optimized_exporter = OptimizedMetricBigQueryViewExporter(
            bq_client, OptimizedMetricBigQueryViewExportValidator(gcsfs_client))
        delegates = [json_exporter, optimized_exporter]

        view_exporter = CompositeBigQueryViewExporter(
            bq_client,
            gcsfs_client,
            delegates
        )

    project_id = metadata.project_id()

    # If the state code is set to COVID then it will match when the state_filter is None in
    # view_config.METRIC_DATASET_EXPORT_CONFIGS
    matched_export_config = False
    for dataset_export_config in view_config.METRIC_DATASET_EXPORT_CONFIGS:
        if not dataset_export_config.matches_filter(export_job_filter):
            logging.info("Skipped metric export for config [%s] with filter [%s]", dataset_export_config,
                         export_job_filter)
            continue

        matched_export_config = True
        logging.info("Starting metric export for dataset_config [%s] with filter [%s]", dataset_export_config,
                     export_job_filter)

        view_export_configs = dataset_export_config.export_configs_for_views_to_export(project_id=project_id)

        # The export will error if the validations fail for the set of view_export_configs. We want to log this failure
        # as a warning, but not block on the rest of the exports.
        try:
            view_exporter.export_and_validate(view_export_configs)
        except ViewExportValidationError:
            warning_message = f"Export validation failed from {dataset_export_config.dataset_id}"

            if dataset_export_config.state_code_filter is not None:
                warning_message += f" for state: {dataset_export_config.state_code_filter}"

            logging.warning(warning_message)
            with monitoring.measurements({
                monitoring.TagKey.METRIC_VIEW_EXPORT_NAME: dataset_export_config.export_name,
                monitoring.TagKey.REGION: dataset_export_config.state_code_filter
            }) as measurements:
                measurements.measure_int_put(m_failed_metric_export_validation, 1)

            # Do not treat validation failures as fatal errors
            continue
        except Exception as e:
            with monitoring.measurements({
                monitoring.TagKey.METRIC_VIEW_EXPORT_NAME: dataset_export_config.export_name,
                monitoring.TagKey.REGION: dataset_export_config.state_code_filter
            }) as measurements:
                measurements.measure_int_put(m_failed_metric_export_job, 1)
            raise e

    if not matched_export_config:
        raise ValueError("Export filter did not match any export configs: ", export_job_filter)


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        type=str,
                        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
                        required=True)

    parser.add_argument('--export_job_filter',
                        dest='export_job_filter',
                        type=str,
                        choices=([c.state_code_filter for c in view_config.METRIC_DATASET_EXPORT_CONFIGS] +
                                 [c.export_name for c in view_config.METRIC_DATASET_EXPORT_CONFIGS]),
                        required=False)

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        export_view_data_to_cloud_storage(known_args.export_job_filter)
