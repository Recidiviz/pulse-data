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

"""Defines configuration for a BigQuery query whose results should be exported somewhere."""
import os
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.metrics.metric_big_query_view import MetricBigQueryView

STAGING_DIRECTORY = "staging/"


class ExportOutputFormatType(Enum):
    METRIC = "metric"
    HEADERLESS_CSV = "headerless_csv"
    CSV = "csv"
    JSON = "json"
    # TODO(#14474): Allow other types to be exported with metadata, ideally by delegating the output
    # format type instead of hardcoding all the "_with_metadata" format types.
    HEADERLESS_CSV_WITH_METADATA = "headerless_csv_with_metadata"


EXPORT_OUTPUT_FORMAT_TYPE_TO_EXTENSION = {
    ExportOutputFormatType.METRIC: "txt",
    ExportOutputFormatType.CSV: "csv",
    ExportOutputFormatType.JSON: "json",
    ExportOutputFormatType.HEADERLESS_CSV: "csv",
    ExportOutputFormatType.HEADERLESS_CSV_WITH_METADATA: "csv",
}


class ExportValidationType(Enum):
    EXISTS = "exists"
    NON_EMPTY_COLUMNS = "non_empty_columns"
    NON_EMPTY_COLUMNS_HEADERLESS = "non_empty_columns_headerless"
    OPTIMIZED = "optimized"


@attr.s(frozen=True)
class ExportQueryConfig:
    """Defines configuration for a BigQuery query whose results should be exported somewhere."""

    # The query to export
    query: str = attr.ib()

    # Query parameters for the above query
    query_parameters: List[bigquery.ScalarQueryParameter] = attr.ib()

    # The name of the dataset to write the intermediate table to.
    intermediate_dataset_id: str = attr.ib()

    # The name of the intermediate table to create/overwrite.
    intermediate_table_name: str = attr.ib()

    # The desired path of the output file (starts with 'gs://').
    output_uri: str = attr.ib()

    # The desired format of the output file
    output_format: str = attr.ib()

    @property
    def intermediate_table_address(self) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=self.intermediate_dataset_id,
            table_id=self.intermediate_table_name,
        )

    @classmethod
    def from_view_query(
        cls,
        view: BigQueryView,
        view_filter_clause: str,
        intermediate_table_name: str,
        output_uri: str,
        output_format: str,
    ) -> "ExportQueryConfig":
        query = f"{view.select_query} {view_filter_clause}"
        return ExportQueryConfig(
            query=query,
            query_parameters=[],
            intermediate_dataset_id=view.dataset_id,
            intermediate_table_name=intermediate_table_name,
            output_uri=output_uri,
            output_format=output_format,
        )


BigQueryViewType = TypeVar("BigQueryViewType", bound=BigQueryView)


@attr.s(frozen=True)
class ExportBigQueryViewConfig(Generic[BigQueryViewType]):
    """Defines configuration for a BigQuery view whose results should be exported somewhere."""

    # The Big Query view to export from
    view: BigQueryViewType = attr.ib()

    # The name of the intermediate table to create/overwrite.
    intermediate_table_name: str = attr.ib()

    # The desired path to the output directory.
    output_directory: GcsfsDirectoryPath = attr.ib()

    # Map of output format types for this view to any validations to perform on the export.
    export_output_formats_and_validations: Dict[
        ExportOutputFormatType, List[ExportValidationType]
    ] = attr.ib()

    # The filter clause that should be used to filter the view
    view_filter_clause: Optional[str] = attr.ib(default=None)

    # If set to True then empty files are considered valid. If False then some
    # validators may choose to mark empty files as invalid.
    allow_empty: bool = attr.ib(default=False)

    # If set, the specified columns will have a find/replace of values applied
    # This is particularly useful when we wish to export data for a "sandbox" state to its production counterpart
    # For example, `state_code` US_IX -> US_ID
    # accepts the form of { "column": { "SEARCH_VALUE": "REPLACE_VALUE" } }
    remap_columns: Dict[str, Dict[Any, Any]] = attr.ib(factory=dict)

    # If set to True, the wildcard character '*' will be included in the URI path,
    # which will allow the exporter to split the export into multiple files if the
    # total export size would exceed 1 GB
    include_wildcard_in_uri: bool = attr.ib(default=False)

    @export_output_formats_and_validations.default
    def _default_export_output_formats_and_validations(
        self,
    ) -> Dict[ExportOutputFormatType, List[ExportValidationType]]:
        if isinstance(self.view, MetricBigQueryView):
            return {
                ExportOutputFormatType.JSON: [ExportValidationType.EXISTS],
                ExportOutputFormatType.METRIC: [ExportValidationType.OPTIMIZED],
            }
        return {ExportOutputFormatType.JSON: [ExportValidationType.EXISTS]}

    def output_path(self, extension: str) -> GcsfsFilePath:
        file_name = f"{self.view.view_id}.{extension}"
        return GcsfsFilePath.from_directory_and_file_name(
            self.output_directory, file_name
        )

    @property
    def query(self) -> str:
        query = f"{self.view.select_query} {self.view_filter_clause}"

        if self.remap_columns is not None and len(self.remap_columns) > 0:
            remapped_columns_clauses = []

            for column, mappings in self.remap_columns.items():
                cases = "\n".join(
                    [
                        f"WHEN {column} = {repr(value)} THEN {repr(mapped_value)}"
                        for value, mapped_value in mappings.items()
                    ]
                )
                remapped_columns_clauses.append(
                    f"""CASE\n{cases}\n ELSE {column} END AS {column}"""
                )

            return f"""
                WITH base_data AS (
                    {query}
                )
                SELECT {",".join(remapped_columns_clauses)}, * EXCEPT ({",".join(self.remap_columns.keys())})
                FROM base_data
            """

        return query

    def as_export_query_config(self, output_format: str) -> ExportQueryConfig:
        if output_format == bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON:
            extension = "json"
        elif output_format == bigquery.DestinationFormat.CSV:
            extension = "csv"
        elif output_format == bigquery.DestinationFormat.AVRO:
            extension = "avro"
        else:
            raise ValueError(f"Unexpected destination format: [{output_format}].")

        output_path = self.output_path(extension=extension)

        if self.include_wildcard_in_uri:
            output_path = output_path.sharded()

        return ExportQueryConfig(
            query=self.query,
            query_parameters=[],
            intermediate_dataset_id=self.view.dataset_id,
            intermediate_table_name=self.intermediate_table_name,
            output_uri=output_path.uri(),
            output_format=output_format,
        )

    @staticmethod
    def revert_staging_path_to_original(staging_path: GcsfsFilePath) -> GcsfsFilePath:
        non_staging_relative_path = staging_path.blob_name
        if non_staging_relative_path.startswith(STAGING_DIRECTORY):
            non_staging_relative_path = non_staging_relative_path[
                len(STAGING_DIRECTORY) :
            ]
        return GcsfsFilePath.from_absolute_path(
            f"{staging_path.bucket_name}/{non_staging_relative_path}"
        )

    def pointed_to_staging_subdirectory(self) -> "ExportBigQueryViewConfig":
        """Returns an updated version of this configuration that points to a 'staging' upload location instead of
        the given location.

        The staging location is equivalent to the given path with the exception that a directory of staging/ is
        inserted directly after the bucket name, before the relative path. This ensures that a staging location can
        be created for any conceivable output uri set in the export config, since every uri must have both a bucket
        and a relative path.
        """
        bucket_name = self.output_directory.bucket_name
        relative_path = self.output_directory.relative_path

        return attr.evolve(
            self,
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                os.path.join(bucket_name, STAGING_DIRECTORY, relative_path)
            ),
        )
