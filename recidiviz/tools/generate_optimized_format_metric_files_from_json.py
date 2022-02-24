# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A script to generate optimized format metric files from json.
It reads files from a specified GCS bucket, generates the optimized format file,
then exports the optimized format files to the same GCS bucket.
This can be run on-demand locally with the following command:
    python -m recidiviz.tools.generate_optimized_format_metric_files_from_json \
        --bucket [GCS bucket path] \
"""
import argparse
import json
import logging
import sys
from random import randint
from typing import List, Optional, Tuple, cast
from unittest.mock import create_autospec

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.calculator.query.state.views.dashboard.dashboard_views import (
    DASHBOARD_VIEW_BUILDERS,
)
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.io.contents_handle import ContentsHandle
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import (
    OptimizedMetricBigQueryViewExportValidator,
)
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import (
    OptimizedMetricBigQueryViewExporter,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.tools.export_metrics_from_dataset_to_gcs import get_protected_buckets
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.namespaces import BigQueryViewNamespace

LIMIT = 10000


def get_metric_files(
    gcsfs_client: GCSFileSystem, metric_file_bucket: str
) -> List[GcsfsFilePath]:
    """Gets the metric files from a specified bucket bucket"""
    bucket = GcsfsDirectoryPath.from_absolute_path(metric_file_bucket)
    return [
        f
        for f in gcsfs_client.ls_with_blob_prefix(
            bucket_name=bucket.bucket_name,
            blob_prefix=bucket.relative_path,
        )
        if isinstance(f, GcsfsFilePath)
    ]


def get_blob_name_without_filename(blob_name: str) -> str:
    """Removes the filename from the path blob"""
    directory = blob_name.split("/")[:-1]
    return "/".join(directory) + "/"


def get_filename(blob_name: str) -> str:
    """Gets the filename from the path blob"""
    file_name = blob_name.split("/")[-1]
    return file_name.split(".")[0]


def create_table(
    bq_client: BigQueryClientImpl,
    local_file: ContentsHandle,
    dataset_id: str,
    dataset_ref: str,
    table_id: str,
) -> None:
    """Creates a temporary table and inserts json data into it"""
    data_points = [json.loads(x.strip()) for x in local_file.get_contents_iterator()]
    schema_fields = [
        bigquery.SchemaField(column, "STRING") for column in data_points[0].keys()
    ]
    bq_client.create_table_with_schema(
        dataset_id=dataset_id,
        table_id=table_id,
        schema_fields=schema_fields,
    )
    offset = 0
    while True:
        if len(data_points) == offset:
            break
        bq_client.stream_into_table(
            dataset_ref=dataset_ref,
            table_id=table_id,
            rows=data_points[offset : offset + LIMIT],
        )
        offset += LIMIT
        if offset > len(data_points):
            break


def get_dimensions(filename: str) -> Tuple[str, ...]:
    view_builders = DASHBOARD_VIEW_BUILDERS
    view_builder = next((vb for vb in view_builders if vb.view_id == filename), None)
    if not view_builder:
        raise ValueError(f"View Builder not found for file {filename}")
    dimensions = view_builder.dimensions  # type: ignore
    # If JSON file has different dimensions than the view builder, modify them here
    return dimensions


def export_from_table(
    bq_client: BigQueryClientImpl,
    file: GcsfsFilePath,
    table_id: str,
    dataset_id: str,
) -> None:
    mock_validator = create_autospec(OptimizedMetricBigQueryViewExportValidator)
    view_exporter = OptimizedMetricBigQueryViewExporter(bq_client, mock_validator)
    output_path = cast(
        GcsfsDirectoryPath,
        GcsfsDirectoryPath.from_bucket_and_blob_name(
            bucket_name=file.bucket_name,
            blob_name=get_blob_name_without_filename(file.blob_name),
        ),
    )
    export_config = ExportBigQueryViewConfig(
        bq_view_namespace=BigQueryViewNamespace.STATE,
        view=MetricBigQueryViewBuilder(
            dataset_id=dataset_id,
            view_id=table_id,
            view_query_template="",
            dimensions=get_dimensions(table_id),
            description="temporary table for exporting optimized format fixture files from GCS",
        ).build(),
        view_filter_clause="",
        intermediate_table_name="temporary_table_for_export",
        output_directory=output_path,
    )
    view_exporter.export(export_configs=[export_config])


def generate_optimized_format_metric_files_from_json(
    bucket: str,
) -> None:
    """Generates optimized format metric files from json files in GCS bucket"""
    gcsfs_client = GcsfsFactory.build()
    bq_client = BigQueryClientImpl()

    if bucket in get_protected_buckets(GCP_PROJECT_STAGING):
        raise ValueError(
            f"Must specify a bucket that is not a protected bucket. "
            f"Protected buckets are: {get_protected_buckets(GCP_PROJECT_STAGING)}"
        )

    temp_dataset_id = f"temp_dataset_{randint(1000,9999)}"
    logging.info("Creating temporary dataset [%s]", temp_dataset_id)
    bq_client.create_dataset_if_necessary(
        bq_client.dataset_ref_for_id(dataset_id=temp_dataset_id),
        default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )
    temp_dataset_ref = bq_client.dataset_ref_for_id(dataset_id=temp_dataset_id)
    if not temp_dataset_ref:
        raise ValueError(f"Dataset reference not found for dataset {temp_dataset_id}")

    try:
        logging.info("Reading json files from GCS")
        metric_files: List[GcsfsFilePath] = get_metric_files(gcsfs_client, bucket)

        for file in metric_files:
            if not file.blob_name.endswith("json"):
                continue
            filename = get_filename(file.blob_name)

            logging.info("Downloading file [%s]", filename)
            local_file: Optional[
                LocalFileContentsHandle
            ] = gcsfs_client.download_to_temp_file(file)
            if not isinstance(local_file, LocalFileContentsHandle):
                continue

            logging.info(
                "Creating table [%s] in temporary dataset [%s]",
                filename,
                temp_dataset_id,
            )
            create_table(
                bq_client=bq_client,
                local_file=local_file,
                table_id=filename,
                dataset_id=temp_dataset_id,
                dataset_ref=temp_dataset_ref,
            )

            logging.info(
                "Exporting optimized format metric file [%s] to the destination bucket [%s]",
                filename,
                bucket,
            )
            export_from_table(
                bq_client=bq_client,
                file=file,
                table_id=filename,
                dataset_id=temp_dataset_id,
            )

    finally:
        logging.info(
            "Deleting temporary dataset [%s]",
            temp_dataset_id,
        )
        bq_client.delete_dataset(
            dataset_ref=temp_dataset_ref,
            delete_contents=True,
        )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bucket",
        dest="bucket",
        help="Path of GCS bucket that contains json metric files to generate and export in optimized format.",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    with local_project_id_override(GCP_PROJECT_STAGING):
        logging.info(
            "Importing files from [%s], "
            "and exporting optimized format metric files to same bucket ",
            known_args.bucket,
        )

        generate_optimized_format_metric_files_from_json(
            known_args.bucket,
        )
