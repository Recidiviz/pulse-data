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
"""Export data from BigQuery metric views to configurable locations."""
import logging
from http import HTTPStatus
from typing import Dict, List, Optional, Sequence, Tuple

from flask import Blueprint, request
from opencensus.stats import aggregation, measure
from opencensus.stats import view as opencensus_view

from recidiviz.big_query import view_update_manager
from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.big_query_view_export_validator import (
    ExistsBigQueryViewExportValidator,
)
from recidiviz.big_query.export.big_query_view_exporter import (
    BigQueryViewExporter,
    CSVBigQueryViewExporter,
    JsonLinesBigQueryViewExporter,
    ViewExportValidationError,
)
from recidiviz.big_query.export.export_query_config import ExportOutputFormatType
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.metrics.export import export_config
from recidiviz.metrics.export.export_config import (
    PRODUCTS_CONFIG_PATH,
    BadProductExportSpecificationError,
    ExportBigQueryViewConfig,
    ProductConfigs,
)
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import (
    OptimizedMetricBigQueryViewExportValidator,
)
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import (
    OptimizedMetricBigQueryViewExporter,
)
from recidiviz.metrics.export.view_export_cloud_task_manager import (
    ViewExportCloudTaskManager,
)
from recidiviz.utils import metadata, monitoring
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_value
from recidiviz.view_registry import deployed_views
from recidiviz.view_registry.datasets import VIEW_SOURCE_TABLE_DATASETS

m_failed_metric_export_validation = measure.MeasureInt(
    "bigquery/metric_view_export_manager/metric_view_export_validation_failure",
    "Counted every time a set of exported metric views fails validation",
    "1",
)

failed_metric_export_validation_view = opencensus_view.View(
    "bigquery/metric_view_export_manager/num_metric_view_export_validation_failure",
    "The sum of times a set of exported metric views fails validation",
    [monitoring.TagKey.REGION, monitoring.TagKey.METRIC_VIEW_EXPORT_NAME],
    m_failed_metric_export_validation,
    aggregation.SumAggregation(),
)

m_failed_metric_export_job = measure.MeasureInt(
    "bigquery/metric_view_export_manager/metric_view_export_job_failure",
    "Counted every time a set of exported metric views fails for non-validation reasons",
    "1",
)

failed_metric_export_view = opencensus_view.View(
    "bigquery/metric_view_export_manager/num_metric_view_export_job_failure",
    "The sum of times a set of exported metric views fails to export for non-validation reasons",
    [monitoring.TagKey.REGION, monitoring.TagKey.METRIC_VIEW_EXPORT_NAME],
    m_failed_metric_export_job,
    aggregation.SumAggregation(),
)

monitoring.register_views(
    [failed_metric_export_validation_view, failed_metric_export_view]
)

export_blueprint = Blueprint("export", __name__)


@export_blueprint.route("/create_metric_view_data_export_tasks")
@requires_gae_auth
def create_metric_view_data_export_tasks() -> Tuple[str, HTTPStatus]:
    """Queues a task to export data in BigQuery metric views to cloud storage buckets.

    Example:
        export/create_metric_view_data_export_tasks?export_job_filter=US_ID
    URL parameters:
        export_job_filter: Job name to initiate export for (e.g. US_ID or LANTERN).
                           If state_code, will create tasks for all products that have launched for that state_code.
                           If product name, will create tasks for all states that have launched for that product.
    Args:
        N/A
    Returns:
        N/A
    """
    logging.info("Queueing a task to export view data to cloud storage")

    export_job_filter = get_str_param_value("export_job_filter", request.args)

    if not export_job_filter:
        return (
            "Missing required export_job_filter URL parameter",
            HTTPStatus.BAD_REQUEST,
        )

    relevant_product_exports = ProductConfigs.from_file(
        path=PRODUCTS_CONFIG_PATH
    ).get_export_configs_for_job_filter(export_job_filter)

    for export in relevant_product_exports:
        ViewExportCloudTaskManager().create_metric_view_data_export_task(
            export_job_name=export["export_job_name"], state_code=export["state_code"]
        )

    return "", HTTPStatus.OK


@export_blueprint.route("/metric_view_data", methods=["GET", "POST"])
@requires_gae_auth
def metric_view_data_export() -> Tuple[str, HTTPStatus]:
    """Exports data in BigQuery metric views to cloud storage buckets.

    Example:
        export/metric_view_data?export_job_name=PO_MONTHLY&state_code=US_ID
    URL parameters:
        export_job_name: Name of job to initiate export for (e.g. PO_MONTHLY).
        state_code: (Optional) State code to initiate export for (e.g. US_ID)
                    State code must be present if the job is not state agnostic.
    Args:
        N/A
    Returns:
        N/A
    """
    logging.info("Attempting to export view data to cloud storage")

    export_job_name = get_str_param_value("export_job_name", request.args)
    state_code = get_str_param_value("state_code", request.args)

    if not export_job_name:
        return (
            "Missing required export_job_name URL parameter",
            HTTPStatus.BAD_REQUEST,
        )

    product_configs = ProductConfigs.from_file(path=PRODUCTS_CONFIG_PATH)
    try:
        export_launched = product_configs.is_export_launched_in_env(
            export_job_name=export_job_name, state_code=state_code
        )
    except BadProductExportSpecificationError as e:
        logging.exception(e)
        return str(e), HTTPStatus.BAD_REQUEST

    if export_launched:
        export_view_data_to_cloud_storage(
            export_job_name=export_job_name, state_code=state_code
        )

    return "", HTTPStatus.OK


def get_configs_for_export_name(
    export_name: str,
    project_id: str,
    state_code: Optional[str] = None,
    destination_override: Optional[str] = None,
    address_overrides: Optional[BigQueryAddressOverrides] = None,
) -> Sequence[ExportBigQueryViewConfig]:
    """Checks the export index for a matching export and if one exists, returns the
    export name paired with a sequence of export view configs."""
    relevant_export_collection = export_config.VIEW_COLLECTION_EXPORT_INDEX.get(
        export_name.upper()
    )

    if not relevant_export_collection:
        raise ValueError(
            f"No export configs matching export name: [{export_name.upper()}]"
        )

    return relevant_export_collection.export_configs_for_views_to_export(
        project_id=project_id,
        state_code_filter=state_code.upper() if state_code else None,
        destination_override=destination_override,
        address_overrides=address_overrides,
    )


def rematerialize_views_for_metric_export(
    project_id: str,
    export_view_configs: Sequence[ExportBigQueryViewConfig],
) -> None:
    bq_view_namespaces_to_update = {
        config.bq_view_namespace for config in export_view_configs
    }
    for bq_view_namespace_to_update in bq_view_namespaces_to_update:
        # TODO(#5125): Once view update is consistently trivial, always update all views in namespace
        if (
            bq_view_namespace_to_update
            in export_config.NAMESPACES_REQUIRING_FULL_UPDATE
        ):
            view_builders_for_views_to_update = (
                deployed_views.deployed_view_builders_for_namespace(
                    project_id, bq_view_namespace_to_update
                )
            )
            view_update_manager.create_managed_dataset_and_deploy_views_for_view_builders(
                view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
                view_builders_to_update=view_builders_for_views_to_update,
                # Don't perform a cleanup when we're just rematerializing views for
                # the metric export
                historically_managed_datasets_to_clean=None,
            )

    # The view deploy will only have rematerialized views that had been updated since the last deploy, this call
    # will ensure that all materialized tables get refreshed.
    view_update_manager.rematerialize_views(
        views_to_update=[config.view for config in export_view_configs],
        all_view_builders=deployed_views.deployed_view_builders(project_id),
        view_source_table_datasets=VIEW_SOURCE_TABLE_DATASETS,
    )


def export_view_data_to_cloud_storage(
    export_job_name: str,
    state_code: Optional[str] = None,
    override_view_exporter: Optional[BigQueryViewExporter] = None,
    should_materialize_views: Optional[bool] = True,
    destination_override: Optional[str] = None,
    address_overrides: Optional[BigQueryAddressOverrides] = None,
) -> None:
    """Exports data in BigQuery metric views to cloud storage buckets.

    Optionally takes in a BigQueryViewExporter for performing the export operation. If none is provided, this defaults
    to using a CompositeBigQueryViewExporter with delegates of JsonLinesBigQueryViewExporter and
    OptimizedMetricBigQueryViewExporter.
    """

    project_id = metadata.project_id()

    export_configs_for_filter = get_configs_for_export_name(
        export_name=export_job_name,
        state_code=state_code,
        project_id=project_id,
        destination_override=destination_override,
        address_overrides=address_overrides,
    )

    if should_materialize_views:
        rematerialize_views_for_metric_export(
            project_id=project_id, export_view_configs=export_configs_for_filter
        )

    do_metric_export_for_configs(
        export_configs={export_job_name: export_configs_for_filter},
        override_view_exporter=override_view_exporter,
        state_code_filter=state_code,
    )


def do_metric_export_for_configs(
    export_configs: Dict[str, Sequence[ExportBigQueryViewConfig]],
    state_code_filter: Optional[str],
    override_view_exporter: Optional[BigQueryViewExporter] = None,
) -> None:
    """Triggers the export given the export_configs."""

    gcsfs_client = GcsfsFactory.build()
    delegate_export_map = get_delegate_export_map(gcsfs_client, override_view_exporter)

    for export_name, view_export_configs in export_configs.items():
        export_log_message = f"Starting [{export_name}] export"
        export_log_message += (
            f" for state_code [{state_code_filter}]." if state_code_filter else "."
        )

        logging.info(export_log_message)

        # The export will error if the validations fail for the set of view_export_configs. We want to log this failure
        # as a warning, but not block on the rest of the exports.
        try:
            export_views_with_exporters(
                gcsfs_client, view_export_configs, delegate_export_map
            )
        except ViewExportValidationError as e:
            warning_message = f"Export validation failed for {export_name}"

            if state_code_filter:
                warning_message += f" for state: {state_code_filter}"

            logging.warning("%s\n%s", warning_message, str(e))
            with monitoring.measurements(
                {
                    monitoring.TagKey.METRIC_VIEW_EXPORT_NAME: export_name,
                    monitoring.TagKey.REGION: state_code_filter,
                }
            ) as measurements:
                measurements.measure_int_put(m_failed_metric_export_validation, 1)

            # Do not treat validation failures as fatal errors
            continue
        except Exception as e:
            with monitoring.measurements(
                {
                    monitoring.TagKey.METRIC_VIEW_EXPORT_NAME: export_name,
                    monitoring.TagKey.REGION: state_code_filter,
                }
            ) as measurements:
                measurements.measure_int_put(m_failed_metric_export_job, 1)
            raise e


def get_delegate_export_map(
    gcsfs_client: GCSFileSystem,
    override_view_exporter: Optional[BigQueryViewExporter] = None,
) -> Dict[ExportOutputFormatType, BigQueryViewExporter]:
    """Builds the delegate_export_map, mapping the csv_exporter, json_exporter, and metric_exporter
    to the correct ExportOutputFormatType.
    """
    if override_view_exporter is None:
        bq_client = BigQueryClientImpl()

        # Some our views intentionally export empty files (e.g. some of the ingest_metadata views)
        # so we just check for existence
        csv_exporter = CSVBigQueryViewExporter(
            bq_client, ExistsBigQueryViewExportValidator(gcsfs_client)
        )
        json_exporter = JsonLinesBigQueryViewExporter(
            bq_client, ExistsBigQueryViewExportValidator(gcsfs_client)
        )
        metric_exporter = OptimizedMetricBigQueryViewExporter(
            bq_client, OptimizedMetricBigQueryViewExportValidator(gcsfs_client)
        )

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
    return delegate_export_map


def export_views_with_exporters(
    gcsfs: GCSFileSystem,
    export_configs: Sequence[ExportBigQueryViewConfig],
    delegate_exporter_for_output: Dict[ExportOutputFormatType, BigQueryViewExporter],
) -> List[GcsfsFilePath]:
    """Runs all exporters on relevant export configs, first placing contents into a staging/ directory
    before copying all results over when everything is seen to have succeeded."""
    if not export_configs or not delegate_exporter_for_output:
        return []

    logging.info("Starting composite BigQuery view export.")
    all_staging_paths: List[GcsfsFilePath] = []

    for export_type, view_exporter in delegate_exporter_for_output.items():
        staging_configs = [
            config.pointed_to_staging_subdirectory()
            for config in export_configs
            if export_type in config.export_output_formats
        ]

        logging.info(
            "Beginning staged export of results for view exporter delegate [%s]",
            view_exporter.__class__.__name__,
        )

        staging_paths = view_exporter.export_and_validate(staging_configs)
        all_staging_paths.extend(staging_paths)

        logging.info(
            "Completed staged export of results for view exporter delegate [%s]",
            view_exporter.__class__.__name__,
        )

        logging.info("Copying staged export results to final location")

    final_paths = []
    for staging_path in all_staging_paths:
        final_path = ExportBigQueryViewConfig.revert_staging_path_to_original(
            staging_path
        )
        gcsfs.copy(staging_path, final_path)
        final_paths.append(final_path)

    logging.info("Deleting staged copies of the final output paths")
    for staging_path in all_staging_paths:
        gcsfs.delete(staging_path)

    logging.info("Completed composite BigQuery view export.")
    return final_paths
