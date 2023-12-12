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
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

# TODO(python/mypy#10360): Direct imports are used to workaround mypy issue
import google.cloud.monitoring_v3 as monitoring_v3  # pylint: disable=consider-using-from-import
from google.cloud.storage import Blob, Client
from opentelemetry.metrics import Observation

from recidiviz.big_query.export.export_query_config import (
    EXPORT_OUTPUT_FORMAT_TYPE_TO_EXTENSION,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsPath
from recidiviz.metrics.export.export_config import (
    VIEW_COLLECTION_EXPORT_INDEX,
    ExportViewCollectionConfig,
)
from recidiviz.metrics.export.products.product_configs import ProductConfigs
from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, ObservableGaugeInstrumentKey
from recidiviz.monitoring.providers import monitoring_metric_url
from recidiviz.utils import metadata
from recidiviz.utils.environment import gcp_only

DEPRECATED_FILE_TIMESTAMP = -1
MISSING_FILE_CREATION_TIMESTAMP = 0
NEWLY_ADDED_EXPORT_GRACE_PERIOD = timedelta(hours=24)
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


def build_blob_recent_reading_query(
    blob_uri: str, *, lookback: timedelta = NEWLY_ADDED_EXPORT_GRACE_PERIOD
) -> str:
    return f"""
    fetch global
      | metric "{monitoring_metric_url(ObservableGaugeInstrumentKey.EXPORT_FILE_AGE)}"
      | filter eq(resource.project_id, "{metadata.project_id()}")
      | filter metric.{AttributeKey.EXPORT_FILE} = "{blob_uri}"
      | filter val() != {MISSING_FILE_CREATION_TIMESTAMP}
      | within {int(lookback.total_seconds())}s
    """


def blob_has_recent_readings(
    blob_uri: str, *, lookback: timedelta = NEWLY_ADDED_EXPORT_GRACE_PERIOD
) -> bool:
    """Returns if the blob has non-missing readings in the lookback period"""
    query_client = monitoring_v3.QueryServiceClient()

    request = monitoring_v3.QueryTimeSeriesRequest(
        name=f"projects/{metadata.project_id()}",
        query=build_blob_recent_reading_query(blob_uri, lookback=lookback),
    )

    results = query_client.query_time_series(request)

    return len(list(results)) > 0


def produce_export_timeliness_metrics(
    expected_file_uris: set[str], actual_blobs: list[Blob]
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
            # Don't trigger alerts for exports that have not gone through their first calculation dag run
            if blob_has_recent_readings(blob_uri)
        ]
    )

    return metrics


def get_export_timeliness_metrics() -> Iterable[Observation]:
    """Collects file age from the GCS buckets used in exports and creates measurements"""
    product_configs = ProductConfigs.from_file()

    storage_client = Client()

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
            list(storage_client.list_blobs(output_directory.bucket_name)),
        )

        for blob_uri, creation_timestamp in produced_metrics:
            logging.info("Metric file age: %s %s", blob_uri, creation_timestamp)

            attributes = {
                AttributeKey.METRIC_VIEW_EXPORT_NAME: export_job_name,
                AttributeKey.EXPORT_FILE: blob_uri,
            }

            if region_code:
                attributes.update({AttributeKey.REGION: region_code})

            yield Observation(value=creation_timestamp, attributes=attributes)


@gcp_only
def report_export_timeliness_metrics() -> None:
    """Collects file age from the GCS buckets used in exports and creates measurements"""
    # Load the instrument
    get_monitoring_instrument(ObservableGaugeInstrumentKey.EXPORT_FILE_AGE)
