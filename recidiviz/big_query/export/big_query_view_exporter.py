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
from typing import List, Sequence

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.big_query_view_export_validator import BigQueryViewExportValidator
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class ViewExportValidationError(Exception):
    """Error thrown when exported view files fail a validation."""


class BigQueryViewExporter:
    """Interface for implementations which export BigQuery view results to specific locations in specific formats."""

    def __init__(self, bq_client: BigQueryClient, validator: BigQueryViewExportValidator):
        self.bq_client = bq_client
        self.validator = validator

    @abc.abstractmethod
    def export(self, export_configs: Sequence[ExportBigQueryViewConfig]) -> List[GcsfsFilePath]:
        """Exports the results of each of the given query configurations.

        Returns a list of final export paths.

        Raises errors if any of the export operations fails.
        """

    def export_and_validate(self,
                            export_configs: Sequence[ExportBigQueryViewConfig],
                            assert_path_total: bool = False) -> List[GcsfsFilePath]:
        """Exports the results of each of the given query configurations.

        Returns a list of final export paths and validates that the results are valid.

        Raises errors if any of the export operations fails or if any of the files fail validation.
        """

        exported_paths = self.export(export_configs)

        expected_num_output_paths = len(export_configs)
        num_output_paths = len(set(exported_paths))

        if assert_path_total and num_output_paths != expected_num_output_paths:
            raise ValueError(f'Expected [{expected_num_output_paths}] output paths, found [{num_output_paths}]')

        for output_path in exported_paths:
            if not self.validator.validate(output_path):
                raise ViewExportValidationError(
                    f'Validation on path {output_path.abs_path()} failed the metric file export. '
                    f'Stopping execution here.')

        return exported_paths


class JsonLinesBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in the Json Lines format and exports to Google Cloud Storage.

    This format is natively supported by the BigQuery Export API so this is a thin wrapper around that call.
    """

    def export(self, export_configs: Sequence[ExportBigQueryViewConfig]) -> List[GcsfsFilePath]:
        export_query_configs = [c.as_export_query_config(bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
                                for c in export_configs]
        self.bq_client.export_query_results_to_cloud_storage(export_query_configs)

        return [GcsfsFilePath.from_absolute_path(config.output_uri) for config in export_query_configs]


class CSVBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which produces results in CSV format and exports to Google Cloud Storage.

    This format is natively supported by the BigQuery Export API so this is a thin wrapper around that call.
    """

    def export(self, export_configs: Sequence[ExportBigQueryViewConfig]) -> List[GcsfsFilePath]:
        export_query_configs = [c.as_export_query_config(bigquery.DestinationFormat.CSV)
                                for c in export_configs]
        self.bq_client.export_query_results_to_cloud_storage(export_query_configs)

        return [GcsfsFilePath.from_absolute_path(config.output_uri) for config in export_query_configs]
