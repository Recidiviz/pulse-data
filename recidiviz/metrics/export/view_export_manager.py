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
    python -m recidiviz.metrics.export.view_export_manager \
        --project_id [PROJECT_ID] \
        --export_job_filter [FILTER] \
        --update_materialized_views [SHOULD_UPDATE]
"""
import argparse
import logging
import sys
from http import HTTPStatus
from typing import Dict, List, Optional, Sequence, Set, Tuple

from flask import Blueprint, request
from opencensus.stats import measure, view as opencensus_view, aggregation

from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.metrics.export import export_config
from recidiviz.big_query import view_update_manager
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.big_query_view_exporter import (
    BigQueryViewExporter,
    CSVBigQueryViewExporter,
    JsonLinesBigQueryViewExporter,
    ViewExportValidationError,
)
from recidiviz.big_query.export.big_query_view_export_validator import ExistsBigQueryViewExportValidator
from recidiviz.big_query.export.export_query_config import ExportOutputFormatType
from recidiviz.big_query.view_update_manager import BigQueryViewNamespace
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.metrics.export.export_config import ExportBigQueryViewConfig, ExportViewCollectionConfig
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import OptimizedMetricBigQueryViewExporter
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import \
    OptimizedMetricBigQueryViewExportValidator
from recidiviz.metrics.export.view_export_cloud_task_manager import ViewExportCloudTaskManager
from recidiviz.utils import metadata, monitoring
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import get_str_param_value, str_to_bool

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


export_blueprint = Blueprint('export', __name__)


@export_blueprint.route('/create_metric_view_data_export_task')
@requires_gae_auth
def create_metric_view_data_export_task() -> Tuple[str, HTTPStatus]:
    """Queues a task to export data in BigQuery metric views to cloud storage buckets.

    Example:
        export/create_metric_view_data_export_task?export_job_filter=US_ID
    URL parameters:
        export_job_filter: (string) Kind of jobs to initiate export for. Can either be an export_name (e.g. LANTERN)
                                    or a state_code (e.g. US_ND)
    Args:
        N/A
    Returns:
        N/A
    """
    logging.info("Queueing a task to export view data to cloud storage")

    export_job_filter = get_str_param_value("export_job_filter", request.args)

    if not export_job_filter:
        return 'missing required export_job_filter URL parameter', HTTPStatus.BAD_REQUEST

    ViewExportCloudTaskManager().create_metric_view_data_export_task(export_job_filter=export_job_filter)

    return '', HTTPStatus.OK


@export_blueprint.route('/metric_view_data', methods=['GET', 'POST'])
@requires_gae_auth
def metric_view_data_export() -> Tuple[str, HTTPStatus]:
    """Exports data in BigQuery metric views to cloud storage buckets.

    Example:
        export/metric_view_data?export_job_filter=US_ID
    URL parameters:
        export_job_filter: (string) Kind of jobs to initiate export for. Can either be an export_name (e.g. LANTERN)
                                    or a state_code (e.g. US_ND)
    Args:
        N/A
    Returns:
        N/A
    """
    logging.info("Attempting to export view data to cloud storage")

    export_job_filter = get_str_param_value("export_job_filter", request.args)

    if not export_job_filter:
        return 'missing required export_job_filter URL parameter', HTTPStatus.BAD_REQUEST

    export_view_data_to_cloud_storage(export_job_filter)

    return '', HTTPStatus.OK


def export_view_data_to_cloud_storage(export_job_filter: str,
                                      override_view_exporter: Optional[BigQueryViewExporter] = None,
                                      update_materialized_views: bool = True) -> None:
    """Exports data in BigQuery metric views to cloud storage buckets.

    Optionally takes in a BigQueryViewExporter for performing the export operation. If none is provided, this defaults
    to using a CompositeBigQueryViewExporter with delegates of JsonLinesBigQueryViewExporter and
    OptimizedMetricBigQueryViewExporter.
    """
    export_configs_for_filter: List[ExportViewCollectionConfig] = []
    bq_view_namespaces_to_update: Set[BigQueryViewNamespace] = set()
    for dataset_export_config in export_config.VIEW_COLLECTION_EXPORT_CONFIGS:
        if not dataset_export_config.matches_filter(export_job_filter):
            logging.info("Skipped metric export for config [%s] with filter [%s]", dataset_export_config,
                         export_job_filter)
            continue

        export_configs_for_filter.append(dataset_export_config)
        bq_view_namespaces_to_update.add(dataset_export_config.bq_view_namespace)

    if not export_configs_for_filter:
        raise ValueError("Export filter did not match any export configs: ", export_job_filter)

    for bq_view_namespace_to_update in bq_view_namespaces_to_update:
        view_builders_for_views_to_update = view_update_manager.VIEW_BUILDERS_BY_NAMESPACE[
            bq_view_namespace_to_update]

        # TODO(#5125): Once view update is consistently trivial, always update all views in namespace
        if bq_view_namespace_to_update in export_config.NAMESPACES_REQUIRING_FULL_UPDATE:
            view_update_manager.create_dataset_and_update_views_for_view_builders(bq_view_namespace_to_update,
                                                                                  view_builders_for_views_to_update,
                                                                                  materialized_views_only=False)
        elif update_materialized_views:
            view_update_manager.create_dataset_and_update_views_for_view_builders(bq_view_namespace_to_update,
                                                                                  view_builders_for_views_to_update,
                                                                                  materialized_views_only=True)

    json_exporter = None
    metric_exporter = None

    gcsfs_client = GcsfsFactory.build()
    if override_view_exporter is None:
        bq_client = BigQueryClientImpl()

        # Some our views intentionally export empty files (e.g. some of the ingest_metadata views)
        # so we just check for existence
        csv_exporter = CSVBigQueryViewExporter(bq_client,
                                               ExistsBigQueryViewExportValidator(gcsfs_client))
        json_exporter = JsonLinesBigQueryViewExporter(bq_client,
                                                      ExistsBigQueryViewExportValidator(gcsfs_client))
        metric_exporter = OptimizedMetricBigQueryViewExporter(
            bq_client, OptimizedMetricBigQueryViewExportValidator(gcsfs_client))

        delegate_export_map = {
            ExportOutputFormatType.CSV: csv_exporter,
            ExportOutputFormatType.HEADERLESS_CSV: csv_exporter,
            ExportOutputFormatType.JSON: json_exporter,
            ExportOutputFormatType.METRIC: metric_exporter,
        }
    else:
        delegate_export_map = {
            ExportOutputFormatType.CSV: override_view_exporter,
            ExportOutputFormatType.HEADERLESS_CSV: override_view_exporter,
            ExportOutputFormatType.JSON: override_view_exporter,
            ExportOutputFormatType.METRIC: override_view_exporter,
        }

    project_id = metadata.project_id()

    for dataset_export_config in export_configs_for_filter:
        logging.info("Starting metric export for dataset_config [%s] with filter [%s]", dataset_export_config,
                     export_job_filter)

        view_export_configs = dataset_export_config.export_configs_for_views_to_export(project_id=project_id)

        # The export will error if the validations fail for the set of view_export_configs. We want to log this failure
        # as a warning, but not block on the rest of the exports.
        try:
            export_views_with_exporters(gcsfs_client, view_export_configs, delegate_export_map)
        except ViewExportValidationError:
            warning_message = f"Export validation failed for {dataset_export_config.export_name}"

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


def export_views_with_exporters(gcsfs: GCSFileSystem,
                                export_configs: Sequence[ExportBigQueryViewConfig],
                                delegate_exporter_for_output: Dict[ExportOutputFormatType, BigQueryViewExporter]) -> \
        List[GcsfsFilePath]:
    """Runs all exporters on relevant export configs, first placing contents into a staging/ directory
    before copying all results over when everything is seen to have succeeded."""
    if not export_configs or not delegate_exporter_for_output:
        return []

    logging.info("Starting composite BigQuery view export.")
    all_staging_paths: List[GcsfsFilePath] = []

    for export_type, view_exporter in delegate_exporter_for_output.items():
        staging_configs = [config.pointed_to_staging_subdirectory()
                           for config in export_configs if export_type in config.export_output_formats]

        logging.info("Beginning staged export of results for view exporter delegate [%s]", view_exporter.__class__)

        staging_paths = view_exporter.export_and_validate(staging_configs)
        all_staging_paths.extend(staging_paths)

        logging.info("Completed staged export of results for view exporter delegate [%s]", view_exporter.__class__)

        logging.info("Copying staged export results to final location")

    final_paths = []
    for staging_path in all_staging_paths:
        final_path = ExportBigQueryViewConfig.revert_staging_path_to_original(staging_path)
        gcsfs.copy(staging_path, final_path)
        final_paths.append(final_path)

    logging.info("Deleting staged copies of the final output paths")
    for staging_path in all_staging_paths:
        gcsfs.delete(staging_path)

    logging.info("Completed composite BigQuery view export.")
    return final_paths


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
                        choices=([c.state_code_filter for c in
                                  export_config.VIEW_COLLECTION_EXPORT_CONFIGS] +
                                 [c.export_name for c in
                                  export_config.VIEW_COLLECTION_EXPORT_CONFIGS]),
                        required=True)
    parser.add_argument('--update_materialized_views',
                        default=True,
                        type=str_to_bool,
                        help='If True, all materialized views will be refreshed before doing the export. If the '
                             'materialized tables are already up-to-date, setting this to False will save significant '
                             'execution time.')

    return parser.parse_known_args(argv)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(known_args.project_id):
        export_view_data_to_cloud_storage(export_job_filter=known_args.export_job_filter,
                                          update_materialized_views=known_args.update_materialized_views)
