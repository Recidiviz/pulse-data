# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Class responsible for loading raw files into BigQuery"""
import datetime
import logging
from typing import List, Optional

from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJob

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_data_temp_load_dataset
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_migration_collector import (
    DirectIngestRawTableMigrationCollector,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_transformation_query_builder import (
    DirectIngestTempRawTablePreMigrationTransformationQueryBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import LoadPrepSummary
from recidiviz.utils import metadata


class DirectIngestRawFileLoadManager:
    """Class responsible for loading raw files into BigQuery"""

    def __init__(
        self,
        raw_data_instance: DirectIngestInstance,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
        fs: DirectIngestGCSFileSystem,
        big_query_client: Optional[BigQueryClient] = None,
    ) -> None:
        self.region_code = region_raw_file_config.region_code
        self.state_code = StateCode(self.region_code.upper())
        self.raw_data_instance = raw_data_instance
        self.region_raw_file_config = region_raw_file_config
        self.big_query_client = big_query_client or BigQueryClientImpl()
        self.fs = fs
        self.transformation_query_builder = (
            DirectIngestTempRawTablePreMigrationTransformationQueryBuilder(
                region_raw_file_config, raw_data_instance
            )
        )

        self.raw_table_migrations = DirectIngestRawTableMigrationCollector(
            region_code=self.region_code,
            instance=self.raw_data_instance,
            regions_module_override=self.region_raw_file_config.region_module,
        )

    def _delete_temp_files(self, temp_file_paths: List[GcsfsFilePath]) -> None:
        logging.info("Deleting [%s] temp paths", len(temp_file_paths))
        for temp_file_path in temp_file_paths:
            logging.info("\tDeleting temp file [%s].", temp_file_path.abs_path())
            self.fs.delete(temp_file_path)

    def _load_paths_to_temp_table(
        self,
        file_tag: str,
        paths: List[GcsfsFilePath],
        destination_address: BigQueryAddress,
    ) -> int:
        """Loads the raw data in the list of files at the provided |paths| into into
        |destination_address|, not including recidivz-managed fields
        """
        try:
            load_job: LoadJob = self.big_query_client.load_table_from_cloud_storage_async(
                source_uris=[p.uri() for p in paths],
                destination_dataset_ref=self.big_query_client.dataset_ref_for_id(
                    destination_address.dataset_id
                ),
                destination_table_id=destination_address.table_id,
                destination_table_schema=RawDataTableBigQuerySchemaBuilder.build_bq_schmea_for_config(
                    raw_file_config=self.region_raw_file_config.raw_file_configs[
                        file_tag
                    ],
                    include_recidiviz_managed_fields=False,
                ),
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        except Exception as e:
            logging.error(
                "Failed to start load job for [%s] w/ paths: [%s]",
                destination_address.to_str(),
                paths,
            )
            self._delete_temp_files(paths)
            raise e

        try:
            logging.info(
                "[%s] Waiting for load of [%s] paths into [%s]",
                datetime.datetime.now().isoformat(),
                len(paths),
                load_job.destination,
            )
            load_job.result()
            logging.info(
                "[%s] BigQuery load of [%s] paths complete",
                datetime.datetime.now().isoformat(),
                len(paths),
            )
        except Exception as e:
            logging.error(
                "Insert job [%s] failed with errors: [%s]",
                load_job.job_id,
                load_job.errors,
            )
            raise e
        finally:
            self._delete_temp_files(paths)

        loaded_row_count = load_job.output_rows

        if loaded_row_count is None:
            raise ValueError(
                f"Insert job [{load_job.job_id}] row count indicates no rows were loaded"
            )

        return loaded_row_count

    def _apply_pre_migration_transformations(
        self,
        source_table: BigQueryAddress,
        destination_table: BigQueryAddress,
        file_tag: str,
        file_id: int,
        update_datetime: datetime.datetime,
    ) -> int:
        """Applies transformations to |source_table| necessary for raw data
        migration queries to run, creating a new table |destination_table|.
        """

        transformation_query = (
            self.transformation_query_builder.build_pre_migration_transformations_query(
                project_id=metadata.project_id(),
                file_tag=file_tag,
                source_table=source_table,
                file_id=file_id,
                update_datetime=update_datetime,
                is_deleted=False,
            )
        )

        query_job = self.big_query_client.create_table_from_query_async(
            dataset_id=destination_table.dataset_id,
            table_id=destination_table.table_id,
            query=transformation_query,
            overwrite=True,
            use_query_cache=False,
        )
        try:
            query_result = query_job.result()
        except Exception as e:
            logging.error(
                "Transformation query job [%s] failed with errors: [%s]",
                query_job.job_id,
                query_job.errors,
            )
            raise e

        return query_result.total_rows

    def _apply_migrations(self, file_tag: str, table: BigQueryAddress) -> None:
        """If relevant, applies raw data migrations for |file_tag| to |table|."""
        migration_queries = (
            self.raw_table_migrations.get_raw_table_migration_queries_for_file_tag(
                file_tag, table
            )
        )

        if not migration_queries:
            logging.info("No queries found for [%s]; continuing", file_tag)
            return

        for migration_query in migration_queries:
            query_job = self.big_query_client.run_query_async(
                query_str=migration_query, use_query_cache=False
            )
            try:
                query_job.result()
            except Exception as e:
                logging.error(
                    "Migration query job [%s] failed with errors: [%s]",
                    query_job.job_id,
                    query_job.errors,
                )
                raise e

    def load_and_prep_paths(
        self,
        file_id: int,
        file_tag: str,
        update_datetime: datetime.datetime,
        paths: List[GcsfsFilePath],
    ) -> LoadPrepSummary:
        """Loads and transforms a raw data file into a temp table, in the order of:
            (1) load raw data directly into a temp table
            (2) apply pre-migration transformations
            (3) apply raw data migrations

        After this step, we should be ready to perform raw data pruning and append to
        the current raw data table.
        """

        temp_raw_file_address = BigQueryAddress(
            dataset_id=raw_data_temp_load_dataset(
                self.state_code, self.raw_data_instance
            ),
            table_id=f"{file_tag}__{file_id}",
        )

        raw_rows_count = self._load_paths_to_temp_table(
            file_tag, paths, temp_raw_file_address
        )

        temp_raw_file_with_transformations_address = BigQueryAddress(
            dataset_id=raw_data_temp_load_dataset(
                self.state_code,
                self.raw_data_instance,
            ),
            table_id=f"{file_tag}__{file_id}__transformed",
        )

        self._apply_pre_migration_transformations(
            temp_raw_file_address,
            temp_raw_file_with_transformations_address,
            file_tag,
            file_id,
            update_datetime,
        )

        self._apply_migrations(file_tag, temp_raw_file_with_transformations_address)

        return LoadPrepSummary(
            append_ready_table_address=temp_raw_file_with_transformations_address.to_str(),
            raw_rows_count=raw_rows_count,
        )
