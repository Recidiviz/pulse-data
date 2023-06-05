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
"""Export timeliness monitoring"""
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from google.cloud import storage
from opencensus.metrics.transport import DEFAULT_INTERVAL, GRACE_PERIOD
from opencensus.stats import aggregation, measure
from opencensus.stats import view as opencensus_view

from recidiviz.big_query.export.export_query_config import (
    EXPORT_OUTPUT_FORMAT_TYPE_TO_EXTENSION,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsPath
from recidiviz.metrics.export.export_config import (
    VIEW_COLLECTION_EXPORT_INDEX,
    ExportViewCollectionConfig,
)
from recidiviz.metrics.export.products.product_configs import ProductConfigs
from recidiviz.utils import monitoring

m_export_file_age = measure.MeasureInt(
    "bigquery/metric_view_export_manager/export_file_age",
    "Creation time of an export file in seconds since epoch",
    "s",
)

export_file_age_view = opencensus_view.View(
    "bigquery/metric_view_export_manager/export_file_age",
    "The age of an exported file",
    [
        monitoring.TagKey.REGION,
        monitoring.TagKey.METRIC_VIEW_EXPORT_NAME,
        monitoring.TagKey.EXPORT_FILE,
    ],
    m_export_file_age,
    aggregation.LastValueAggregation(),
)

MISSING_FILE_CREATION_TIMESTAMP = 0
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def generate_expected_file_uris(
    collection_export_config: ExportViewCollectionConfig,
    region_code: Optional[str] = None,
) -> set[str]:
    """Returns a set of all gs:// output URIs that we expect to be updated for this export job"""
    return {
        export_config.output_path(
            EXPORT_OUTPUT_FORMAT_TYPE_TO_EXTENSION[export_output_format]
        ).uri()
        for export_config in collection_export_config.export_configs_for_views_to_export(
            state_code_filter=region_code
        )
        for export_output_format in export_config.export_output_formats_and_validations.keys()
    }


def seconds_since_epoch(dt: datetime) -> int:
    """Assuming dt is timezone aware in UTC, return seconds since epoch"""
    return round((dt - UTC_EPOCH).total_seconds())


def produce_export_timeliness_metrics(
    expected_file_uris: set[str], actual_blobs: list[storage.Blob]
) -> list[tuple[str, int]]:
    blob_uris_to_blob = {GcsfsPath.from_blob(blob).uri(): blob for blob in actual_blobs}

    # Generate age metrics for blobs that exist
    metrics: list[tuple[str, int]] = [
        (blob_uri, seconds_since_epoch(blob.time_created))
        for blob_uri, blob in blob_uris_to_blob.items()
        if blob_uri in expected_file_uris
    ]

    # Generate age metrics for blobs that do not exist (not created yet or deleted)
    metrics.extend(
        [
            (blob_uri, MISSING_FILE_CREATION_TIMESTAMP)
            for blob_uri in (expected_file_uris - blob_uris_to_blob.keys())
        ]
    )

    return metrics


def report_export_timeliness_metrics() -> None:
    """Collects file age from the GCS buckets used in exports and creates measurements"""
    monitoring.register_views([export_file_age_view])

    monitoring.register_stackdriver_exporter()

    product_configs = ProductConfigs.from_file()

    client = storage.Client()

    for export in product_configs.get_all_export_configs():
        export_job_name = export["export_job_name"]
        region_code = export["state_code"]
        collection_export_config = VIEW_COLLECTION_EXPORT_INDEX[export_job_name]

        output_directory = collection_export_config.output_directory

        expected_file_uris = generate_expected_file_uris(
            collection_export_config=collection_export_config,
            region_code=region_code,
        )

        produced_metrics = produce_export_timeliness_metrics(
            expected_file_uris,
            list(client.list_blobs(output_directory.bucket_name)),
        )

        for blob_uri, creation_timestamp in produced_metrics:
            logging.info("Metric file age: %s %s", blob_uri, creation_timestamp)
            with monitoring.measurements(
                {
                    monitoring.TagKey.REGION: region_code,
                    monitoring.TagKey.METRIC_VIEW_EXPORT_NAME: export_job_name,
                    monitoring.TagKey.EXPORT_FILE: blob_uri,
                }
            ) as measurements:
                measurements.measure_int_put(m_export_file_age, creation_timestamp)

    # Keep the thread alive long enough for the stackdriver exporter to report metrics at least once.
    # TODO(#20775): Once migrated to OpenTelemetry, replace this sleep with a synchronous export
    time.sleep(DEFAULT_INTERVAL + GRACE_PERIOD + 10)
