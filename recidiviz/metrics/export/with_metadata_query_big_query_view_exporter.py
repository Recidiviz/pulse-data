# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

"""View Exporter implementation that delegates its export to another BigQueryViewExporter and makes
an additional query to BigQuery, the results of which are written to the GCS object metadata.
"""

import logging
from typing import List, Sequence, Tuple

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.big_query_view_export_validator import (
    BigQueryViewExportValidator,
    ExistsBigQueryViewExportValidator,
)
from recidiviz.big_query.export.big_query_view_exporter import (
    BigQueryViewExporter,
    CSVBigQueryViewExporter,
)
from recidiviz.big_query.export.export_query_config import (
    ExportBigQueryViewConfig,
    ExportOutputFormatType,
)
from recidiviz.big_query.with_metadata_query_big_query_view import (
    WithMetadataQueryBigQueryView,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


class WithMetadataQueryBigQueryViewExporter(BigQueryViewExporter):
    """View Exporter implementation that delegates its export to another BigQueryViewExporter and makes
    an additional query to BigQuery, the results of which are written to the GCS object metadata.

    The metadata_query truncates the result so at most a single row is returned. The metadata keys are
    the columns returned by the query, and the values are the values of the row for that key. If no data
    is returned by the query, no metadata will be written.

    Any view filters contained in `export_configs` will be applied to both the original view query
    and the metadata query."""

    def __init__(
        self,
        bq_client: BigQueryClient,
        validator: BigQueryViewExportValidator,
    ):
        super().__init__(bq_client, validator)

    def export(
        self,
        export_configs: Sequence[
            ExportBigQueryViewConfig[WithMetadataQueryBigQueryView]
        ],
    ) -> List[Tuple[ExportBigQueryViewConfig, GcsfsFilePath]]:
        # TODO(#14474): allow delegates that aren't HEADERLESS_CSV
        csv_configs = [
            attr.evolve(
                c,
                export_output_formats=[ExportOutputFormatType.HEADERLESS_CSV],
            )
            for c in export_configs
        ]
        gcsfs_client = GcsfsFactory.build()
        csv_exporter = CSVBigQueryViewExporter(
            self.bq_client, ExistsBigQueryViewExportValidator(gcsfs_client)
        )
        configs_and_destinations = csv_exporter.export(csv_configs)

        # Query for metadata
        for config, destination in configs_and_destinations:
            metadata_query = self._build_metadata_query(config)
            logging.info("Running metadata query for view %s:", config.view.view_id)
            query_job = self.bq_client.run_query_async(metadata_query)
            results = [dict(row) for row in query_job]
            if len(results) > 1:
                raise ValueError(
                    f"Too many results received from metadata query for view {config.view.view_id}; this should never happen since we add LIMIT 1"
                )

            if results:
                gcsfs_client.update_metadata(destination, results[0])

        return configs_and_destinations

    @classmethod
    def _build_metadata_query(
        cls, config: ExportBigQueryViewConfig[WithMetadataQueryBigQueryView]
    ) -> str:
        # TODO(#14475): allow instances where the view filter does not apply
        view_filter_clause = config.view_filter_clause or ""
        return f"""
            WITH metadata_query_cte AS (
                {config.view.metadata_query}
            )
            SELECT * FROM metadata_query_cte
            {view_filter_clause}
            LIMIT 1
            """
