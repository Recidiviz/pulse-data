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

"""BigQuery View Exporter implementation which exports views in all of the formats provided by the exporter delegates.

An additional feature of this exporter is that it first writes all output files to a staging directory in the final
destination bucket. Only once all files for all formats have been written to the staging location does it then perform
GCS copy operations to rapidly copy all files from the staging directory to the final destination. This mitigates the
possibility of partial file exports, i.e. only uploading some of the files we want to export and putting data for
consumers in an inconsistent state.

Note: since this class exports via multiple delegate exporters for each export config that it receives, callers should
ensure that the delegates used to instantiate this class will not duplicate file extensions among them, or else the
results of the last delegate will overwrite the results of previous delegates.
"""

import logging
from typing import List, Sequence

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.big_query_view_exporter import BigQueryViewExporter
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath

QUERY_PAGE_SIZE = 1000


class CompositeBigQueryViewExporter(BigQueryViewExporter):
    """View exporter which takes in multiple delegate view exporters, writes all of their contents to a
    staging location, i.e. not the final destination, and only when all contents have been staged does it move all of
    them to the final destination."""

    def __init__(self,
                 bq_client: BigQueryClient,
                 fs: GCSFileSystem,
                 delegate_view_exporters: List[BigQueryViewExporter]):
        super().__init__(bq_client)
        self.delegate_view_exporters = delegate_view_exporters
        self.fs = fs

    def export(self, export_configs: Sequence[ExportBigQueryViewConfig]) -> List[GcsfsFilePath]:
        logging.info("Starting composite BigQuery view export.")

        staging_configs = [config.pointed_to_staging_subdirectory() for config in export_configs]

        output_staging_paths = []
        for view_exporter in self.delegate_view_exporters:
            logging.info("Beginning staged export of results for view exporter delegate [%s]", view_exporter.__class__)

            output_staging_paths.extend(view_exporter.export(staging_configs))

            logging.info("Completed staged export of results for view exporter delegate [%s]", view_exporter.__class__)

        self._verify_staged_paths(export_configs, output_staging_paths)

        logging.info("Copying staged export results to final location")

        final_paths = []
        for staging_path in output_staging_paths:
            final_path = ExportBigQueryViewConfig.revert_staging_path_to_original(staging_path)
            self.fs.copy(staging_path, final_path)
            final_paths.append(final_path)

        logging.info("Deleting staged copies of the final output paths")
        for staging_path in output_staging_paths:
            self.fs.delete(staging_path)

        logging.info("Completed composite BigQuery view export.")
        return final_paths

    def _verify_staged_paths(self,
                             export_configs: Sequence[ExportBigQueryViewConfig],
                             output_staging_paths: List[GcsfsFilePath]) -> None:
        """Verifies that we've exported all expected paths and they actually exist in GCS."""
        expected_num_output_staging_paths = len(export_configs)*len(self.delegate_view_exporters)
        num_output_staging_paths = len(set(output_staging_paths))
        if len(set(output_staging_paths)) != expected_num_output_staging_paths:
            raise ValueError(f'Expected [{expected_num_output_staging_paths}] staging output paths, '
                             f'found [{num_output_staging_paths}]')

        for staging_path in output_staging_paths:
            if not self.fs.exists(staging_path):
                raise ValueError(
                    f'File at path [{staging_path.uri()}] does not exist - export did not complete properly')
