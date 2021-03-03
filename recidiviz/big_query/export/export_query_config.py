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
from typing import List, TypeVar, Generic, Optional

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.metrics.metric_big_query_view import MetricBigQueryView


STAGING_DIRECTORY = "staging/"


class ExportOutputFormatType(Enum):
    METRIC = "metric"
    HEADERLESS_CSV = "headerless_csv"
    CSV = "csv"
    JSON = "json"


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
    output_format: bigquery.DestinationFormat = attr.ib()

    @classmethod
    def from_view_query(
        cls,
        view: BigQueryView,
        view_filter_clause: str,
        intermediate_table_name: str,
        output_uri: str,
        output_format: bigquery.DestinationFormat,
    ) -> "ExportQueryConfig":
        query = "{select_query} {filter_clause}".format(
            select_query=view.select_query, filter_clause=view_filter_clause
        )
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

    # The output format types that are preferred for this view
    export_output_formats: List[ExportOutputFormatType] = attr.ib()

    # The filter clause that should be used to filter the view
    view_filter_clause: Optional[str] = attr.ib(default=None)

    @export_output_formats.default
    def _default_export_output_formats(self) -> List[ExportOutputFormatType]:
        if isinstance(self.view, MetricBigQueryView):
            return [ExportOutputFormatType.JSON, ExportOutputFormatType.METRIC]
        return [ExportOutputFormatType.JSON]

    def output_path(self, extension: str) -> GcsfsFilePath:
        file_name = f"{self.view.view_id}.{extension}"
        return GcsfsFilePath.from_directory_and_file_name(
            self.output_directory, file_name
        )

    @property
    def query(self) -> str:
        return "{select_query} {filter_clause}".format(
            select_query=self.view.select_query, filter_clause=self.view_filter_clause
        )

    def as_export_query_config(
        self, output_format: bigquery.DestinationFormat
    ) -> ExportQueryConfig:
        if output_format == bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON:
            extension = "json"
        elif output_format == bigquery.DestinationFormat.CSV:
            extension = "csv"
        elif output_format == bigquery.DestinationFormat.AVRO:
            extension = "avro"
        else:
            raise ValueError(f"Unexpected destination format: [{output_format}].")

        return ExportQueryConfig(
            query=self.query,
            query_parameters=[],
            intermediate_dataset_id=self.view.dataset_id,
            intermediate_table_name=self.intermediate_table_name,
            output_uri=self.output_path(extension=extension).uri(),
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

        return ExportBigQueryViewConfig(
            view=self.view,
            view_filter_clause=self.view_filter_clause,
            intermediate_table_name=self.intermediate_table_name,
            output_directory=GcsfsDirectoryPath.from_absolute_path(
                os.path.join(bucket_name, STAGING_DIRECTORY, relative_path)
            ),
            export_output_formats=self.export_output_formats,
        )
