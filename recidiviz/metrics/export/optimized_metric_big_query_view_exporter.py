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

"""View Exporter implementation which produces metric files in a format optimized for space.

For large query jobs, the output of this view exporter can be as much 350x smaller than that of the standard
JsonLinesViewExporter, at the expense of human readability.

This format consists of two parts: a single flat array of data values, a list of "value keys" which represent the
actual values contained in the dataset, and a "dimension manifest" which contains an ordered list of dimensions in the
dataset and an ordered list of possible values for each dimension. With the manifest, it is possible to locate where
the data point with a given set of dimensions resides in the array.
"""

import gzip
import io
import json
import logging
from concurrent import futures
from typing import Any, Callable, Dict, List, Sequence, Set, Tuple, Union

import attr
from google.cloud import bigquery, storage

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.big_query_view_exporter import BigQueryViewExporter
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import (
    OptimizedMetricBigQueryViewExportValidator,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryView
from recidiviz.utils import structured_logging

# 10000 rows appears to be a reasonable balance of speed and memory usage from local testing
QUERY_PAGE_SIZE = 10000

DEFAULT_DATA_VALUE = 0

# We set this to 10 because urllib3 (used by the Google BigQuery client) has an default limit of 10 connections and
# we were seeing "urllib3.connectionpool:Connection pool is full, discarding connection" errors when this number
# increased.
# In the future, we could increase the worker number by playing around with increasing the pool size per this post:
# https://github.com/googleapis/python-storage/issues/253
OPTIMIZED_VIEW_EXPORTER_MAX_WORKERS = 10


@attr.s(frozen=True)
class OptimizedMetricRepresentation:
    # The nested array (matrix) of values
    value_matrix: List[Any] = attr.ib()

    # The ordered manifest describing the dimensions, their values, and their location in the value matrix
    dimension_manifest: List[Tuple[str, List[str]]] = attr.ib()

    # The ordered list of value keys in the dataset
    value_keys: List[str] = attr.ib()


class OptimizedMetricBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in an optimized format where data points are organized into a compact
    matrix and then flattened into a single array, which is written to text files and exported to
    Google Cloud Storage."""

    def __init__(
        self,
        bq_client: BigQueryClient,
        validator: OptimizedMetricBigQueryViewExportValidator,
        should_compress: bool = False,
    ):
        super().__init__(bq_client, validator)
        self.should_compress = should_compress

    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig[MetricBigQueryView]]
    ) -> List[Tuple[ExportBigQueryViewConfig[MetricBigQueryView], GcsfsFilePath]]:
        storage_client = storage.Client()
        output_paths = []
        with futures.ThreadPoolExecutor(
            max_workers=OPTIMIZED_VIEW_EXPORTER_MAX_WORKERS
        ) as executor:
            future_to_view = {
                executor.submit(
                    structured_logging.with_context(self._export_view),
                    storage_client,
                    config,
                ): config
                for config in export_configs
            }
            for future in futures.as_completed(future_to_view):
                config = future_to_view.pop(future)
                try:
                    output_path: GcsfsFilePath = future.result()
                except Exception as e:
                    logging.error(
                        "Exception found exporting view: %s.%s",
                        config.view.dataset_id,
                        config.view.view_id,
                    )
                    raise e
                output_paths.append((config, output_path))

        return output_paths

    def _export_view(
        self,
        storage_client: storage.Client,
        config: ExportBigQueryViewConfig[MetricBigQueryView],
    ) -> GcsfsFilePath:
        query_job = self.bq_client.run_query_async(config.query, [])
        optimized_format = self.convert_query_results_to_optimized_value_matrix(
            query_job, config
        )
        output_path = self._export_optimized_format(
            config, optimized_format, storage_client
        )
        return output_path

    def convert_query_results_to_optimized_value_matrix(
        self,
        query_job: bigquery.QueryJob,
        export_config: ExportBigQueryViewConfig[MetricBigQueryView],
    ) -> OptimizedMetricRepresentation:
        """Prepares an optimized metric file format for the results of the given query job and export configuration."""

        # Identifies the full set of keys for the given view, as well as those for for dimensions and values
        export_view = export_config.view

        logging.info(
            "Converting query results to the optimized metric file format for view: %s",
            export_view.view_id,
        )

        table = self.bq_client.get_table(
            self.bq_client.dataset_ref_for_id(export_view.dataset_id),
            export_view.view_id,
        )
        all_keys = [field.name for field in table.schema]

        logging.debug("Determined full set of keys for the view: %s", all_keys)

        if len(all_keys) == 0:
            logging.warning(
                "No columns for this view query, returning an empty representation "
            )
            return OptimizedMetricRepresentation(
                value_matrix=[], dimension_manifest=[], value_keys=[]
            )

        dimension_keys = export_view.dimensions
        value_keys = sorted(list(set(all_keys) - set(dimension_keys)))

        # Look at all records to identify full range of values for each dimension
        dimension_values_by_key: Dict[str, Set[str]] = _initialize_dimension_manifest(
            dimension_keys
        )
        assemble_manifest_fn = _gen_assemble_manifest(dimension_values_by_key)
        self.bq_client.paged_read_and_process(
            query_job, QUERY_PAGE_SIZE, assemble_manifest_fn
        )
        logging.info(
            "Produced dictionary-based manifest for view: %s", export_view.view_id
        )
        logging.debug(
            "Dictionary-based manifest for view %s: %s",
            export_view.view_id,
            dimension_values_by_key,
        )

        # Transform dimension ranges into list of tuples first ordered by dimension key and internally by values
        dimension_manifest: List[
            Tuple[str, List[str]]
        ] = transform_manifest_to_order_enforced_form(dimension_values_by_key)
        logging.info(
            "Produced ordered dimension manifest for view: %s", export_view.view_id
        )
        logging.debug(
            "Ordered dimension manifest for view %s: %s",
            export_view.view_id,
            dimension_manifest,
        )

        # Allocate an array with nested arrays for each dimension and value key
        data_values: List[List[Any]] = [
            [] for _ in range(len(dimension_keys) + len(value_keys))
        ]

        # For each data point, set its numeric values in the spot determined by its dimensional combination
        place_value_in_matrix_fn = _gen_place_in_compact_matrix(
            data_values, value_keys, dimension_manifest
        )
        self.bq_client.paged_read_and_process(
            query_job, QUERY_PAGE_SIZE, place_value_in_matrix_fn
        )
        logging.info(
            "Finished paged read and process for view: %s", export_view.view_id
        )

        # Return the array and the dimensional manifest
        return OptimizedMetricRepresentation(
            value_matrix=data_values,
            dimension_manifest=dimension_manifest,
            value_keys=value_keys,
        )

    def _export_optimized_format(
        self,
        export_config: ExportBigQueryViewConfig,
        formatted: OptimizedMetricRepresentation,
        storage_client: storage.Client,
    ) -> GcsfsFilePath:
        """Writes the optimized metric representation to Cloud Storage, based on the export configuration. Returns the
        output path the file was written to.
        """
        output_path = export_config.output_path(extension="txt")

        logging.info(
            "Writing optimized metric file %s to GCS bucket %s...",
            output_path.blob_name,
            output_path.bucket_name,
        )

        blob = storage.Blob.from_string(output_path.uri(), client=storage_client)
        self._set_format_metadata(formatted, blob, should_compress=True)
        blob.upload_from_string(
            self._produce_transmission_format(formatted, should_compress=True),
            content_type="text/plain",
        )

        logging.info(
            "Optimized metric file %s written to GCS bucket %s.",
            output_path.blob_name,
            output_path.bucket_name,
        )

        return output_path

    @staticmethod
    def _set_format_metadata(
        formatted: OptimizedMetricRepresentation,
        blob: storage.Blob,
        should_compress: bool = False,
    ) -> None:
        """Sets metadata on the Cloud Storage blob that can be used to retrieve data points from the optimized
        representation.

        This includes the ordered dimension manifest, the ordered list of value keys, and the total
        number of data points to effectively "unflatten" the flattened matrix. Also sets the 'Content-Encoding: gzip'
        header if the content is going to be compressed.
        """
        total_data_points = (
            len(formatted.value_matrix[0]) if formatted.value_matrix else 0
        )
        metadata = {
            "dimension_manifest": json.dumps(formatted.dimension_manifest),
            "value_keys": json.dumps(formatted.value_keys),
            "total_data_points": total_data_points,
        }
        blob.metadata = metadata

        if should_compress:
            blob.content_encoding = "gzip"

    def _produce_transmission_format(
        self, formatted: OptimizedMetricRepresentation, should_compress: bool = False
    ) -> Union[str, bytes]:
        """Converts the value matrix into a flattened comma-separated string of values.

        Returns the output as a string if should_compress is false. If should_compress is true, returns the output
        as a gzip-compressed array of bytes.
        """
        flattened = [
            value for dimension in formatted.value_matrix for value in dimension
        ]
        as_string = ",".join([str(value) for value in flattened])

        if should_compress:
            return self._gzip_str(as_string)

        return as_string

    @staticmethod
    def _gzip_str(uncompressed: str) -> bytes:
        """Gzip-compresses the given uncompressed string."""
        out = io.BytesIO()

        with gzip.GzipFile(fileobj=out, mode="w") as fo:
            fo.write(uncompressed.encode())

        return out.getvalue()


def _initialize_dimension_manifest(
    dimension_keys: Tuple[str, ...]
) -> Dict[str, Set[str]]:
    """Initializes an empty dictionary-based dimension manifest.

    Each of the given keys will be put in the dictionary with an empty set as its value.
    """
    dimension_values_by_key: Dict[str, Set[str]] = {}

    for key in dimension_keys:
        dimension_values_by_key[key.lower()] = set()

    return dimension_values_by_key


def _gen_assemble_manifest(
    dimension_values_by_key: Dict[str, Set[str]]
) -> Callable[[List[bigquery.table.Row]], None]:
    """Generates and returns a function which will take a given result set row from BigQuery and update the given
    mapping of dimension keys to sets of possible values."""

    def _assemble_by_row(rows: List[bigquery.table.Row]) -> None:
        for row in rows:
            add_to_dimension_manifest(dict(row), dimension_values_by_key)

    return _assemble_by_row


def add_to_dimension_manifest(
    data_point: Dict[str, Any], dimension_values_by_key: Dict[str, Set[str]]
) -> Dict[str, Set[str]]:
    """Updates the given dictionary-based dimension manifest with the dimensional contents of the given data point,
    i.e. ensures that any new values for any dimensions in the data point are included in the manifest."""
    for key in dimension_values_by_key.keys():
        dimension_value = data_point[key]
        normalized_value = _normalize_dimension_value(dimension_value)
        dimension_values_by_key[key].add(normalized_value)

    return dimension_values_by_key


def transform_manifest_to_order_enforced_form(
    dimension_values_by_key: Dict[str, Set[str]]
) -> List[Tuple[str, List[str]]]:
    """Transforms the dictionary version of the dimension manifest into list-based one which enforces ordering for
    both dimension keys and dimension values per key."""
    dimension_manifest: List[Tuple[str, List[str]]] = []
    for dimension_key in sorted(dimension_values_by_key.keys()):
        sorted_values = sorted(dimension_values_by_key[dimension_key])
        dimension_manifest.append((dimension_key, sorted_values))

    return dimension_manifest


def get_row_values(data_point: Dict[str, Any], value_keys: List[str]) -> List[Any]:
    """Returns the actual values in the data point, i.e. the values that are not dimensions."""
    return [data_point.get(vk, DEFAULT_DATA_VALUE) for vk in value_keys]


def _gen_place_in_compact_matrix(
    data_values: List[List[Any]],
    value_keys: List[str],
    dimension_manifest: List[Tuple[str, List[str]]],
) -> Callable[[List[bigquery.table.Row]], None]:
    def _place_by_row(rows: List[bigquery.table.Row]) -> None:
        for row in rows:
            data_point = dict(row)
            place_in_compact_matrix(
                data_point, data_values, value_keys, dimension_manifest
            )

    return _place_by_row


def place_in_compact_matrix(
    data_point: Dict[str, Any],
    data_values: List[List[Any]],
    value_keys: List[str],
    dimension_manifest: List[Tuple[str, List[str]]],
) -> None:
    """Places the given data point within the compact matrix representation of the dataset.

    The compact matrix is composed of an array of arrays: one for each dimension key and for each value key. They are
    ordered in alphabetical order of each dimension key and then in alphabetical order of each value key. Each
    internal array should be the same length: for each non-empty data point in the dataset, there will be a value
    in each array at the same index. A data point's value in a given dimensional array will be the index of that
    data point's value for the dimension within the dimension manifest. Its value in a given value array will simply
    be that particular value.
    """
    for i, manifest_kv in enumerate(dimension_manifest):
        dimension_key = manifest_kv[0].lower()
        dimension_values = manifest_kv[1]

        dimension_value = data_point.get(dimension_key, None)
        normalized_value = _normalize_dimension_value(dimension_value)

        try:
            value_index = dimension_values.index(normalized_value)
        except ValueError as e:
            raise KeyError(
                f"Dimension of [{dimension_key}: {dimension_value}] not found in dimension manifest: "
                f"[{dimension_manifest}]. This indicates that either the manifest was not constructed from "
                "a dataset containing the given data point or a bug in the optimized view exporter. "
                f"Data point: {data_point}"
            ) from e

        data_values[i].append(value_index)

    row_values = get_row_values(data_point, value_keys)
    for i, value in enumerate(row_values):
        data_values[len(dimension_manifest) + i].append(value)


def _normalize_dimension_value(dimension_value: Any) -> str:
    return str(dimension_value).lower()
