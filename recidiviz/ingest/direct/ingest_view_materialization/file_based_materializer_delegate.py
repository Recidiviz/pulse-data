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
"""Implementation of the IngestViewMaterializerDelegate for use on
states using file-based ingest view materialization.

TODO(#11424): Delete this whole class/file once all states have shipped to BQ-based
 ingest view materialization.
"""

import datetime
import logging
from typing import Optional

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_name,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer_delegate import (
    IngestViewMaterializerDelegate,
    ingest_view_materialization_temp_dataset,
)
from recidiviz.ingest.direct.metadata.direct_ingest_file_metadata_manager import (
    DirectIngestIngestFileMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
)


class FileBasedMaterializerDelegate(
    IngestViewMaterializerDelegate[GcsfsIngestViewExportArgs]
):
    """Implementation of the IngestViewMaterializerDelegate for use on
    states using file-based ingest view materialization.
    """

    def __init__(
        self,
        ingest_file_metadata_manager: DirectIngestIngestFileMetadataManager,
        big_query_client: BigQueryClient,
    ):
        self.ingest_file_metadata_manager = ingest_file_metadata_manager
        self.big_query_client = big_query_client

    def get_job_completion_time_for_args(
        self, args: GcsfsIngestViewExportArgs
    ) -> Optional[datetime.datetime]:
        metadata = (
            self.ingest_file_metadata_manager.get_ingest_view_metadata_for_export_job(
                args
            )
        )

        if not metadata:
            raise ValueError(f"Found no metadata for the given job args: [{args}].")

        return metadata.export_time

    def prepare_for_job(self, args: GcsfsIngestViewExportArgs) -> None:
        metadata = (
            self.ingest_file_metadata_manager.get_ingest_view_metadata_for_export_job(
                args
            )
        )
        output_path = self.generate_output_path(args)

        if (
            metadata.normalized_file_name
            and metadata.normalized_file_name != output_path.file_name
        ):
            raise ValueError(
                f"Expected output path normalized file name [{output_path.file_name}] "
                f"does not match name tracked in the operations db "
                f"[{metadata.normalized_file_name}]."
            )
        logging.info("Generated output path [%s]", output_path.uri())

        if not metadata.normalized_file_name:
            self.ingest_file_metadata_manager.register_ingest_view_export_file_name(
                metadata, output_path
            )

    def materialize_query_results(
        self,
        args: GcsfsIngestViewExportArgs,
        ingest_view: DirectIngestPreProcessedIngestView,
        query: str,
    ) -> None:
        export_configs = [
            ExportQueryConfig(
                query=query,
                query_parameters=[],
                intermediate_dataset_id=ingest_view_materialization_temp_dataset(
                    ingest_view, args.ingest_instance()
                ),
                intermediate_table_name=f"{args.ingest_view_name}_latest_export",
                output_uri=self.generate_output_path(args).uri(),
                output_format=bigquery.DestinationFormat.CSV,
            )
        ]

        logging.info("Starting export to cloud storage.")
        self.big_query_client.export_query_results_to_cloud_storage(
            export_configs=export_configs, print_header=True
        )
        logging.info("Export to cloud storage complete.")

    def mark_job_complete(self, args: GcsfsIngestViewExportArgs) -> None:
        metadata = (
            self.ingest_file_metadata_manager.get_ingest_view_metadata_for_export_job(
                args
            )
        )
        self.ingest_file_metadata_manager.mark_ingest_view_exported(metadata)

    @staticmethod
    def generate_output_path(
        ingest_view_export_args: GcsfsIngestViewExportArgs,
    ) -> GcsfsFilePath:
        output_file_name = to_normalized_unprocessed_file_name(
            f"{ingest_view_export_args.ingest_view_name}.csv",
            GcsfsDirectIngestFileType.INGEST_VIEW,
            dt=ingest_view_export_args.upper_bound_datetime_to_export,
        )

        return GcsfsFilePath.from_directory_and_file_name(
            GcsfsBucketPath(ingest_view_export_args.output_bucket_name),
            output_file_name,
        )
