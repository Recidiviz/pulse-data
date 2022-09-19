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

"""Interface and implementations which export BigQuery view results to specific locations in specific formats."""

import abc
from typing import List, Sequence, Tuple

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.big_query_view_export_validator import (
    BigQueryViewExportValidator,
)
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class ViewExportValidationError(Exception):
    """Error thrown when exported view files fail a validation."""


class BigQueryViewExporter:
    """Interface for implementations which export BigQuery view results to specific locations in specific formats."""

    def __init__(
        self, bq_client: BigQueryClient, validator: BigQueryViewExportValidator
    ):
        self.bq_client = bq_client
        self.validator = validator

    @abc.abstractmethod
    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig]
    ) -> List[Tuple[ExportBigQueryViewConfig, GcsfsFilePath]]:
        """Exports the results of each of the given query configurations.

        Returns a list of tuples containing the provided config and its final export path.

        Raises errors if any of the export operations fails.
        """

    def export_and_validate(
        self,
        export_configs: Sequence[ExportBigQueryViewConfig],
        assert_path_total: bool = False,
    ) -> List[GcsfsFilePath]:
        """Exports the results of each of the given query configurations.

        Returns a list of tuples containing the provided config and its final export path.

        Raises errors if any of the export operations fails or if any of the files fail validation.
        """

        exported_configs_and_paths = self.export(export_configs)

        expected_num_output_paths = len(export_configs)
        num_unique_output_paths = len(
            set(path for _, path in exported_configs_and_paths)
        )

        if assert_path_total and num_unique_output_paths != expected_num_output_paths:
            raise ValueError(
                f"Expected [{expected_num_output_paths}] output paths, found [{num_unique_output_paths}]"
            )

        exported_paths = []
        for config, output_path in exported_configs_and_paths:
            if not self.validator.validate(output_path, config.allow_empty):
                raise ViewExportValidationError(
                    f"Validation on path {output_path.abs_path()} failed the metric file export. "
                    f"Stopping execution here."
                )
            exported_paths.append(output_path)

        return exported_paths


class JsonLinesBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in the Json Lines format and exports to Google Cloud Storage.

    This format is natively supported by the BigQuery Export API so this is a thin wrapper around that call.
    """

    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig]
    ) -> List[Tuple[ExportBigQueryViewConfig, GcsfsFilePath]]:
        for config in export_configs:
            if ExportOutputFormatType.JSON not in config.export_output_formats:
                raise ValueError(
                    "JsonLinesBigQueryViewExporter received config that does not export to JSON"
                )

        export_query_configs = [
            c.as_export_query_config(bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
            for c in export_configs
        ]
        self.bq_client.export_query_results_to_cloud_storage(
            export_query_configs, print_header=True
        )

        return [
            (
                export_config,
                GcsfsFilePath.from_absolute_path(export_query_config.output_uri),
            )
            for export_config, export_query_config in zip(
                export_configs, export_query_configs
            )
        ]


class CSVBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in CSV format and exports to Google Cloud Storage.

    This format is natively supported by the BigQuery Export API so this is a thin wrapper around that call.
    Note: This exporter does strip the header from the outputted CSV.
    """

    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig]
    ) -> List[Tuple[ExportBigQueryViewConfig, GcsfsFilePath]]:
        for config in export_configs:
            if (
                ExportOutputFormatType.CSV not in config.export_output_formats
                and ExportOutputFormatType.HEADERLESS_CSV
                not in config.export_output_formats
            ):
                raise ValueError(
                    "CSVBigQueryViewExporter received config that does not export to CSV"
                )
            if (
                ExportOutputFormatType.CSV in config.export_output_formats
                and ExportOutputFormatType.HEADERLESS_CSV
                in config.export_output_formats
            ):
                raise ValueError(
                    "CSVBigQueryViewExporter cannot export CSV both with and without headers."
                )

        export_and_query_configs = [
            (c, c.as_export_query_config(bigquery.DestinationFormat.CSV))
            for c in export_configs
            if ExportOutputFormatType.HEADERLESS_CSV in c.export_output_formats
            or ExportOutputFormatType.CSV in c.export_output_formats
        ]

        headerless_export_query_configs = [
            query_config
            for export_config, query_config in export_and_query_configs
            if ExportOutputFormatType.HEADERLESS_CSV
            in export_config.export_output_formats
        ]
        self.bq_client.export_query_results_to_cloud_storage(
            headerless_export_query_configs, print_header=False
        )

        headered_export_query_configs = [
            query_config
            for export_config, query_config in export_and_query_configs
            if ExportOutputFormatType.CSV in export_config.export_output_formats
        ]
        self.bq_client.export_query_results_to_cloud_storage(
            headered_export_query_configs, print_header=True
        )

        return [
            (export_config, GcsfsFilePath.from_absolute_path(query_config.output_uri))
            for export_config, query_config in export_and_query_configs
        ]
