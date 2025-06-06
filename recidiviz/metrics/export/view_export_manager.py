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
import datetime
import json
import logging
from typing import Dict, List, Optional, Sequence

import pytz

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer
from recidiviz.big_query.big_query_view_sandbox_context import (
    BigQueryViewSandboxContext,
)
from recidiviz.big_query.export.big_query_view_export_validator import (
    BigQueryViewExportValidator,
    ExistsBigQueryViewExportValidator,
    NonEmptyColumnsBigQueryViewExportValidator,
)
from recidiviz.big_query.export.big_query_view_exporter import (
    BigQueryViewExporter,
    CSVBigQueryViewExporter,
    HeaderlessCSVBigQueryViewExporter,
    JsonLinesBigQueryViewExporter,
    ViewExportValidationError,
)
from recidiviz.big_query.export.export_query_config import (
    ExportOutputFormatType,
    ExportValidationType,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export import export_config
from recidiviz.metrics.export.export_config import ExportBigQueryViewConfig
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import (
    OptimizedMetricBigQueryViewExportValidator,
)
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import (
    OptimizedMetricBigQueryViewExporter,
)
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfigs,
)
from recidiviz.metrics.export.with_metadata_query_big_query_view_exporter import (
    WithMetadataQueryBigQueryViewExporter,
)
from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, CounterInstrumentKey
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import VIEW_UPDATE_METADATA_DATASET
from recidiviz.utils import metadata, pubsub_helper
from recidiviz.utils.environment import GCP_PROJECT_STAGING, gcp_only


class ViewExportConfigurationError(Exception):
    """Error thrown when views are misconfigured."""


# Table that holds information about metric view data export jobs
METRIC_VIEW_DATA_EXPORT_TRACKER_ADDRESS = BigQueryAddress(
    dataset_id=VIEW_UPDATE_METADATA_DATASET, table_id="metric_view_data_export_tracker"
)


class MetricViewDataExportSuccessPersister(BigQueryRowStreamer):
    """Class that persists runtime of successful export of metric view data."""

    def __init__(self, bq_client: BigQueryClient):
        source_table_repository = build_source_table_repository_for_yaml_managed_tables(
            metadata.project_id()
        )
        source_table_config = source_table_repository.get_config(
            METRIC_VIEW_DATA_EXPORT_TRACKER_ADDRESS
        )
        super().__init__(
            bq_client, source_table_config.address, source_table_config.schema_fields
        )

    def record_success_in_bq(
        self,
        export_job_name: str,
        runtime_sec: int,
        state_code: Optional[str],
        sandbox_dataset_prefix: Optional[str],
    ) -> None:
        success_row = {
            "success_timestamp": datetime.datetime.now(tz=pytz.UTC).isoformat(),
            "export_job_name": export_job_name,
            "state_code": state_code,
            # TODO(#22440): Remove this column entirely - export location implied by existence of sandbox prefix.
            "destination_override": None,
            "sandbox_dataset_prefix": sandbox_dataset_prefix,
            "metric_view_data_export_runtime_sec": runtime_sec,
        }

        self.stream_rows([success_row])


@gcp_only
def execute_metric_view_data_export(
    export_job_name: str,
    state_code: Optional[StateCode],
) -> None:
    """Exports data in BigQuery metric views to cloud storage buckets."""
    logging.info(
        "Attempting to export view data to cloud storage for export_job_name [%s], state_code [%s]",
        export_job_name,
        state_code,
    )

    product_configs = ProductConfigs.from_file(path=PRODUCTS_CONFIG_PATH)
    export_launched = product_configs.is_export_launched_in_env(
        export_job_name=export_job_name,
        state_code=state_code.value if state_code else None,
    )

    start = datetime.datetime.now()
    if export_launched:
        relevant_export_collection = export_config.VIEW_COLLECTION_EXPORT_INDEX.get(
            export_job_name.upper()
        )

        if not relevant_export_collection:
            raise ValueError(
                f"No export configs matching export name: [{export_job_name.upper()}]"
            )

        export_view_data_to_cloud_storage(
            export_job_name=export_job_name,
            state_code=state_code.value if state_code else None,
            view_sandbox_context=None,
            gcs_output_sandbox_subdir=None,
        )

    end = datetime.datetime.now()
    runtime_sec = int((end - start).total_seconds())

    success_persister = MetricViewDataExportSuccessPersister(
        bq_client=BigQueryClientImpl()
    )
    success_persister.record_success_in_bq(
        export_job_name=export_job_name,
        state_code=state_code.value if state_code else None,
        sandbox_dataset_prefix=None,
        runtime_sec=runtime_sec,
    )
    logging.info("Finished saving success record to database.")

    export_collection = export_config.VIEW_COLLECTION_EXPORT_INDEX.get(
        export_job_name.upper()
    )

    if not export_collection:
        raise ValueError(
            f"No export configs matching export name: [{export_job_name.upper()}]"
        )

    if (
        export_launched
        and export_collection.publish_success_pubsub_message
        and export_collection.pubsub_topic_name
    ):
        # If the export has a specified output project, publish the Pub/Sub message in
        # that same output project.
        output_project_by_data_project = (
            export_collection.output_project_by_data_project
        )
        pubsub_project_id = (
            output_project_by_data_project[metadata.project_id()]
            if output_project_by_data_project
            else metadata.project_id()
        )

        state_code_output: str | None

        if (
            state_code
            and state_code.value in export_collection.export_override_state_codes
        ):
            state_code_output = export_collection.export_override_state_codes[
                state_code.value
            ]
        else:
            state_code_output = state_code.value if state_code else None

        pubsub_helper.publish_message_to_topic(
            destination_project_id=pubsub_project_id,
            topic=export_collection.pubsub_topic_name,
            message=json.dumps({"state_code": state_code_output}),
        )


def get_configs_for_export_name(
    export_name: str,
    state_code: Optional[str] = None,
    gcs_output_sandbox_subdir: Optional[str] = None,
    view_sandbox_context: BigQueryViewSandboxContext | None = None,
) -> Sequence[ExportBigQueryViewConfig]:
    """Checks the export index for a matching export and if one exists, returns the
    export name paired with a sequence of export view configs."""

    if (
        view_sandbox_context
        and not gcs_output_sandbox_subdir
        and metadata.project_id() != GCP_PROJECT_STAGING
    ):
        raise ValueError(
            f"Cannot set view_sandbox_context without gcs_output_sandbox_subdir "
            f"when exporting to [{metadata.project_id()}]. If we're reading from "
            f"views that are part of a sandbox, we must also output to a sandbox "
            f"location."
        )

    relevant_export_collection = export_config.VIEW_COLLECTION_EXPORT_INDEX.get(
        export_name.upper()
    )

    if not relevant_export_collection:
        raise ValueError(
            f"No export configs matching export name: [{export_name.upper()}]"
        )

    return relevant_export_collection.export_configs_for_views_to_export(
        state_code_filter=state_code.upper() if state_code else None,
        view_sandbox_context=view_sandbox_context,
        gcs_output_sandbox_subdir=gcs_output_sandbox_subdir,
    )


def export_view_data_to_cloud_storage(
    *,
    export_job_name: str,
    state_code: Optional[str],
    gcs_output_sandbox_subdir: Optional[str],
    view_sandbox_context: BigQueryViewSandboxContext | None,
    # Used to mock exporters in tests
    override_view_exporter: Optional[BigQueryViewExporter] = None,
) -> None:
    """Exports data in BigQuery metric views to cloud storage buckets.

    Optionally takes in a BigQueryViewExporter for performing the export operation. If none is provided, this defaults
    to using a CompositeBigQueryViewExporter with delegates of JsonLinesBigQueryViewExporter and
    OptimizedMetricBigQueryViewExporter.
    """
    if (
        view_sandbox_context
        and not gcs_output_sandbox_subdir
        and metadata.project_id() != GCP_PROJECT_STAGING
    ):
        raise ValueError(
            f"Cannot set view_sandbox_context without gcs_output_sandbox_subdir "
            f"when exporting to [{metadata.project_id()}]. If we're reading from "
            f"views that are part of a sandbox, we must also output to a sandbox "
            f"location."
        )

    export_configs_for_filter = get_configs_for_export_name(
        export_name=export_job_name,
        state_code=state_code,
        gcs_output_sandbox_subdir=gcs_output_sandbox_subdir,
        view_sandbox_context=view_sandbox_context,
    )

    do_metric_export_for_configs(
        export_name=export_job_name,
        view_export_configs=export_configs_for_filter,
        override_view_exporter=override_view_exporter,
        state_code_filter=state_code,
    )


def do_metric_export_for_configs(
    *,
    export_name: str,
    view_export_configs: Sequence[ExportBigQueryViewConfig],
    state_code_filter: Optional[str],
    override_view_exporter: Optional[BigQueryViewExporter] = None,
) -> None:
    """Triggers the export given the export_configs."""

    gcsfs_client = GcsfsFactory.build()

    export_log_message = f"Starting [{export_name}] export"
    export_log_message += (
        f" for state_code [{state_code_filter}]." if state_code_filter else "."
    )

    logging.info(export_log_message)

    delegate_export_map = get_delegate_export_map(
        gcsfs_client=gcsfs_client,
        export_name=export_name,
        export_configs=view_export_configs,
        override_view_exporter=override_view_exporter,
    )

    monitoring_attributes = {
        AttributeKey.METRIC_VIEW_EXPORT_NAME: export_name,
    }

    if state_code_filter:
        monitoring_attributes[AttributeKey.REGION] = state_code_filter

    try:
        export_views_with_exporters(
            gcsfs_client, view_export_configs, delegate_export_map
        )
    except ViewExportValidationError as e:
        warning_message = f"Export validation failed for {export_name}"

        if state_code_filter:
            warning_message += f" for state: {state_code_filter}"

        logging.warning("%s\n%s", warning_message, str(e))

        get_monitoring_instrument(
            CounterInstrumentKey.VIEW_EXPORT_VALIDATION_FAILURE
        ).add(
            amount=1,
            attributes=monitoring_attributes,
        )

        raise e
    except Exception as e:
        get_monitoring_instrument(CounterInstrumentKey.VIEW_EXPORT_JOB_FAILURE).add(
            amount=1,
            attributes=monitoring_attributes,
        )
        raise e


def get_delegate_export_map(
    *,
    gcsfs_client: GCSFileSystem,
    export_name: str,
    export_configs: Sequence[ExportBigQueryViewConfig],
    override_view_exporter: Optional[BigQueryViewExporter] = None,
) -> Dict[ExportOutputFormatType, BigQueryViewExporter]:
    """Builds the delegate_export_map, mapping the csv_exporter, json_exporter, and metric_exporter
    to the correct ExportOutputFormatType.
    """
    if override_view_exporter:
        return {
            ExportOutputFormatType.CSV: override_view_exporter,
            ExportOutputFormatType.HEADERLESS_CSV: override_view_exporter,
            ExportOutputFormatType.JSON: override_view_exporter,
            ExportOutputFormatType.METRIC: override_view_exporter,
            ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA: override_view_exporter,
        }

    bq_client = BigQueryClientImpl()

    validator_mappings = {
        ExportValidationType.EXISTS: ExistsBigQueryViewExportValidator(gcsfs_client),
        ExportValidationType.NON_EMPTY_COLUMNS: NonEmptyColumnsBigQueryViewExportValidator(
            gcsfs_client, contains_headers=True
        ),
        ExportValidationType.NON_EMPTY_COLUMNS_HEADERLESS: NonEmptyColumnsBigQueryViewExportValidator(
            gcsfs_client, contains_headers=False
        ),
        ExportValidationType.OPTIMIZED: OptimizedMetricBigQueryViewExportValidator(
            gcsfs_client
        ),
    }

    # Determine which validators this set of export configs set for each type. It fails if
    # different configs set the same output format type with different validators.
    validators_for_type: Dict[
        ExportOutputFormatType, List[BigQueryViewExportValidator]
    ] = {}
    for config in export_configs:
        for (
            export_format,
            validations,
        ) in config.export_output_formats_and_validations.items():
            validators = [validator_mappings[val_type] for val_type in validations]

            unsupported_validators = [
                validator
                for validator in validators
                if not validator.supports_output_type(export_format)
            ]

            if unsupported_validators:
                raise ViewExportConfigurationError(
                    f"""Export {export_name} validator(s) {unsupported_validators} do not support export format {export_format}"""
                )

            if (
                export_format in validators_for_type
                and validators_for_type[export_format] != validators
            ):
                raise ViewExportConfigurationError(
                    f"""Validators for export format {export_format} ({validators}) do not match
                    previously configured validations ({validators_for_type[export_format]})"""
                )
            validators_for_type[export_format] = validators

    csv_exporter = CSVBigQueryViewExporter(
        bq_client, validators_for_type.get(ExportOutputFormatType.CSV, [])
    )
    headerless_csv_exporter = HeaderlessCSVBigQueryViewExporter(
        bq_client, validators_for_type.get(ExportOutputFormatType.HEADERLESS_CSV, [])
    )
    json_exporter = JsonLinesBigQueryViewExporter(
        bq_client, validators_for_type.get(ExportOutputFormatType.JSON, [])
    )
    metric_exporter = OptimizedMetricBigQueryViewExporter(
        bq_client, validators_for_type.get(ExportOutputFormatType.METRIC, [])
    )
    with_metadata_query_exporter = WithMetadataQueryBigQueryViewExporter(
        bq_client,
        validators_for_type.get(
            ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA, []
        ),
    )

    return {
        ExportOutputFormatType.CSV: csv_exporter,
        ExportOutputFormatType.HEADERLESS_CSV: headerless_csv_exporter,
        ExportOutputFormatType.JSON: json_exporter,
        ExportOutputFormatType.METRIC: metric_exporter,
        ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA: with_metadata_query_exporter,
    }


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

    for export_lookback_window, view_exporter in delegate_exporter_for_output.items():
        staging_configs = [
            config.pointed_to_staging_subdirectory()
            for config in export_configs
            if export_lookback_window
            in config.export_output_formats_and_validations.keys()
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
