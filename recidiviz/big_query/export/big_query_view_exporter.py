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
        self,
        bq_client: BigQueryClient,
        validators: Sequence[BigQueryViewExportValidator],
    ):
        self.bq_client = bq_client
        self.validators = validators

    @abc.abstractmethod
    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig]
    ) -> List[Tuple[ExportBigQueryViewConfig, List[GcsfsFilePath]]]:
        """Exports the results of each of the given query configurations.

        Returns a list of tuples containing the provided config and its final export path.

        Raises errors if any of the export operations fails.
        """

    def export_and_validate(
        self,
        export_configs: Sequence[ExportBigQueryViewConfig],
    ) -> List[GcsfsFilePath]:
        """Exports the results of each of the given query configurations.

        Returns a list of tuples containing the provided config and its final export path.

        Raises errors if any of the export operations fails or if any of the files fail validation.
        """

        exported_configs_and_paths = self.export(export_configs)

        exported_paths = []
        for config, output_paths in exported_configs_and_paths:
            for output_path in output_paths:
                for validator in self.validators:
                    if not validator.validate(output_path, config.allow_empty):
                        raise ViewExportValidationError(
                            f"Validation on path {output_path.abs_path()} failed on validator {validator}. "
                            f"Stopping execution here without completing the export."
                        )
                exported_paths.append(output_path)

        return exported_paths


class JsonLinesBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in the Json Lines format and exports to Google Cloud Storage.

    This format is natively supported by the BigQuery Export API so this is a thin wrapper around that call.
    """

    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig]
    ) -> List[Tuple[ExportBigQueryViewConfig, List[GcsfsFilePath]]]:
        for config in export_configs:
            if (
                ExportOutputFormatType.JSON
                not in config.export_output_formats_and_validations.keys()
            ):
                raise ValueError(
                    "JsonLinesBigQueryViewExporter received config that does not export to JSON"
                )

        export_query_configs_and_export_configs = [
            (
                c.as_export_query_config(
                    bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
                ),
                c,
            )
            for c in export_configs
        ]
        export_query_configs = [
            export_query_config
            for export_query_config, _export_config in export_query_configs_and_export_configs
        ]
        exported_configs_and_paths = (
            self.bq_client.export_query_results_to_cloud_storage(
                export_configs=export_query_configs,
                print_header=True,
                use_query_cache=True,
            )
        )

        export_configs_by_query_config_query = {
            export_query_config.query: export_config
            for export_query_config, export_config in export_query_configs_and_export_configs
        }

        return [
            (export_configs_by_query_config_query[config.query], paths)
            for config, paths in exported_configs_and_paths
        ]


class CSVBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in CSV format and exports to Google Cloud Storage.

    This format is natively supported by the BigQuery Export API so this is a thin wrapper around that call.
    """

    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig]
    ) -> List[Tuple[ExportBigQueryViewConfig, List[GcsfsFilePath]]]:
        for config in export_configs:
            export_output_formats = config.export_output_formats_and_validations.keys()
            if ExportOutputFormatType.CSV not in export_output_formats:
                raise ValueError(
                    "CSVBigQueryViewExporter received config that does not export to CSV"
                )

        export_query_configs_and_export_configs = [
            (
                c.as_export_query_config(bigquery.DestinationFormat.CSV),
                c,
            )
            for c in export_configs
        ]
        export_query_configs = [
            export_query_config
            for export_query_config, _export_config in export_query_configs_and_export_configs
        ]
        exported_configs_and_paths = (
            self.bq_client.export_query_results_to_cloud_storage(
                export_configs=export_query_configs,
                print_header=True,
                use_query_cache=True,
            )
        )

        export_configs_by_query_config_query = {
            export_query_config.query: export_config
            for export_query_config, export_config in export_query_configs_and_export_configs
        }

        return [
            (export_configs_by_query_config_query[config.query], paths)
            for config, paths in exported_configs_and_paths
        ]


class HeaderlessCSVBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in CSV format and exports to Google Cloud Storage.

    This format is natively supported by the BigQuery Export API so this is a thin wrapper around that call.
    """

    def export(
        self, export_configs: Sequence[ExportBigQueryViewConfig]
    ) -> List[Tuple[ExportBigQueryViewConfig, List[GcsfsFilePath]]]:
        for config in export_configs:
            export_output_formats = config.export_output_formats_and_validations.keys()
            if ExportOutputFormatType.HEADERLESS_CSV not in export_output_formats:
                raise ValueError(
                    "HeaderlessCSVBigQueryViewExporter received config that does not export to Headerless CSV"
                )

        export_query_configs_and_export_configs = [
            (
                c.as_export_query_config(bigquery.DestinationFormat.CSV),
                c,
            )
            for c in export_configs
        ]
        export_query_configs = [
            export_query_config
            for export_query_config, _export_config in export_query_configs_and_export_configs
        ]
        exported_configs_and_paths = (
            self.bq_client.export_query_results_to_cloud_storage(
                export_configs=export_query_configs,
                print_header=False,
                use_query_cache=True,
            )
        )

        export_configs_by_query_config_query = {
            export_query_config.query: export_config
            for export_query_config, export_config in export_query_configs_and_export_configs
        }

        return [
            (export_configs_by_query_config_query[config.query], paths)
            for config, paths in exported_configs_and_paths
        ]
