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
"""A CloudSQLQueryGenerator that processes raw gcs file metadata and returns 
raw big query file metadata"""
import datetime
from collections import defaultdict
from itertools import groupby
from types import ModuleType
from typing import Dict, Iterator, List, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.utils import (
    get_direct_ingest_region_raw_config,
    logger,
    partition_as_list,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadataSummary,
    RawGCSFileMetadataSummary,
)
from recidiviz.utils.string import StrictStringFormatter

ADD_ROWS = """
INSERT INTO direct_ingest_raw_big_query_file_metadata (region_code, raw_data_instance, file_tag, update_datetime, is_invalidated) 
VALUES {values}
RETURNING file_id;"""


UPDATE_GCS_FILES_WITH_FILE_ID = """
UPDATE direct_ingest_raw_gcs_file_metadata AS g SET file_id = v.file_id
FROM ( VALUES
    {values}
) AS v(gcs_file_id, file_id)
WHERE v.gcs_file_id = g.gcs_file_id;"""


class GetAllUnprocessedBQFileMetadataSqlQueryGenerator(
    CloudSqlQueryGenerator[List[str]]
):
    """Custom query generator that processes raw gcs file metadata and returns
    raw big query file metadata
    """

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
        get_all_unprocessed_gcs_file_metadata_task_id: str,
        region_module_override: Optional[ModuleType] = None,
    ) -> None:
        super().__init__()
        self._region_code = region_code
        self._raw_data_instance = raw_data_instance
        self._get_all_unprocessed_gcs_file_metadata_task_id = (
            get_all_unprocessed_gcs_file_metadata_task_id
        )
        self._region_module_override = region_module_override
        self._region_raw_file_config: Optional[DirectIngestRegionRawFileConfig] = None

    @property
    def region_raw_file_config(self) -> DirectIngestRegionRawFileConfig:
        if not self._region_raw_file_config:
            self._region_raw_file_config = get_direct_ingest_region_raw_config(
                self._region_code, region_module_override=self._region_module_override
            )
        return self._region_raw_file_config

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[str]:
        """After pulling in a list of RawGCSFileMetadataSummary frin xcom, processes and
        registers all RawGCSFileMetadataSummary not yet registered in the raw bq file
        metadata table (i.e. does not yet have a file_id). Returns serialized
        RawBigQueryFileMetadataSummary for successfully processed and grouped
        RawGCSFileMetadataSummary.
        """
        # --- get existing file info from xcom -----------------------------------------

        unprocessed_gcs_file_metadata: List[RawGCSFileMetadataSummary] = [
            RawGCSFileMetadataSummary.deserialize(xcom_metadata)
            for xcom_metadata in operator.xcom_pull(
                context,
                key="return_value",
                task_ids=self._get_all_unprocessed_gcs_file_metadata_task_id,
            )
        ]

        if not unprocessed_gcs_file_metadata:
            logger.info("Found no gcs file paths to process; skipping bq registration")
            return []

        # --- splits and process new and already seen metadata -------------------------

        (
            bq_unregistered_gcs_metadata,
            already_bq_registered_gcs_metadata,
        ) = partition_as_list(lambda x: x.file_id, unprocessed_gcs_file_metadata)

        newly_bq_registered_bq_metadata = self._register_bq_unregistered_metadata(
            postgres_hook, bq_unregistered_gcs_metadata
        )

        # --- format and write in a form xcom likes ------------------------------------

        already_bq_registered_bq_metadata = [
            RawBigQueryFileMetadataSummary.from_gcs_files(list(conceptual_file_group))
            for _, conceptual_file_group in groupby(
                already_bq_registered_gcs_metadata, lambda x: x.file_id
            )
        ]

        # --- build xcom output ----------------–––-------------------------------------

        return [
            metadata.serialize()
            for metadata in already_bq_registered_bq_metadata
            + newly_bq_registered_bq_metadata
        ]

    def _register_bq_unregistered_metadata(
        self,
        postgres_hook: PostgresHook,
        bq_unregistered_gcs_metadata: List[RawGCSFileMetadataSummary],
    ) -> List[RawBigQueryFileMetadataSummary]:
        """Uses |bq_unregistered_gcs_metadata| to build a list of RawBigQueryFileMetadataSummary
        objecst, not registering unrecognized file tags
        """
        # --- first, parse and filter to just recognized file tags ---------------------

        (
            bq_unregistered_unrecognized_gcs_metadata,
            bq_unregistered_recognized_gcs_metadata,
        ) = partition_as_list(
            lambda x: x.parts.file_tag in self.region_raw_file_config.raw_file_tags,
            bq_unregistered_gcs_metadata,
        )

        if bq_unregistered_unrecognized_gcs_metadata:
            logger.info(
                "Found unrecognized file tags that we will skip marking in "
                "direct_ingest_raw_big_query_file_metadata: [%s]",
                [
                    file.path.file_name
                    for file in bq_unregistered_unrecognized_gcs_metadata
                ],
            )

        # --- next, group and register files in bq file metadata table -----------------

        unregistered_recognized_bq_metadata = self._group_unregistered_conceptual_files(
            bq_unregistered_recognized_gcs_metadata
        )

        if not unregistered_recognized_bq_metadata:
            return []

        bq_file_ids = postgres_hook.get_records(
            self._register_new_conceptual_files_sql_query(
                unregistered_recognized_bq_metadata
            )
        )

        # --- last, update bq metadata & gcs table w/ newly created objs ---------------

        # here we assume bq_file_ids is returned in the order that we inserted the values
        registered_bq_metadata: List[RawBigQueryFileMetadataSummary] = []
        for i, metadata in enumerate(unregistered_recognized_bq_metadata):
            metadata.file_id = one(bq_file_ids[i])
            registered_bq_metadata.append(metadata)

        postgres_hook.get_records(
            self._add_file_id_to_gcs_table_sql_query(registered_bq_metadata)
        )

        return registered_bq_metadata

    def _group_unregistered_conceptual_files(
        self,
        bq_unregistered_recognized_gcs_metadata: List[RawGCSFileMetadataSummary],
    ) -> List[RawBigQueryFileMetadataSummary]:
        """Uses |bq_unregistered_recognized_gcs_metadata| to build 'conceptual'
        RawBigQueryFileMetadataSummary by grouping files whose raw file config indicates
        that they are a 'chunked file' by upload_date.
        """
        # --- group chunked files by (file_tag, upload_date) ---------------------------

        conceptual_files: List[RawBigQueryFileMetadataSummary] = []
        chunked_files: Dict[
            str, Dict[datetime.date, List[RawGCSFileMetadataSummary]]
        ] = defaultdict(lambda: defaultdict(list))

        for metadata in bq_unregistered_recognized_gcs_metadata:

            if self.region_raw_file_config.raw_file_configs[
                metadata.parts.file_tag
            ].is_chunked_file:
                chunked_files[metadata.parts.file_tag][
                    metadata.parts.utc_upload_datetime.date()
                ].append(metadata)
                continue

            conceptual_files.append(
                RawBigQueryFileMetadataSummary(
                    gcs_files=[metadata], file_tag=metadata.parts.file_tag
                )
            )

        # --- if relevant, determine if chunked file groups are complete ---------------

        if chunked_files:

            for file_tag, upload_date_to_gcs_files in chunked_files.items():

                expected_chunk_count = self.region_raw_file_config.raw_file_configs[
                    file_tag
                ].expected_number_of_chunks

                # if there is a chunked file tag that we cannot successfully chunk, we
                # also want to fail all groups more recent dates to ensure that ascending
                # insertion order for raw data remains guaranted

                incomplete_group_date: Optional[datetime.date] = None

                for upload_date in sorted(upload_date_to_gcs_files):

                    gcs_files = upload_date_to_gcs_files[upload_date]

                    if incomplete_group_date is not None:
                        logger.error(
                            "Skipping grouping for %s on %s as we could not successfully "
                            "group %s on %s and we want to guarantee upload order in big query",
                            file_tag,
                            upload_date.isoformat(),
                            file_tag,
                            incomplete_group_date.isoformat(),
                        )
                    elif expected_chunk_count == len(
                        upload_date_to_gcs_files[upload_date]
                    ):
                        logger.info(
                            "Found %s/%s paths for %s on %s -- grouping %s",
                            len(gcs_files),
                            expected_chunk_count,
                            file_tag,
                            upload_date.isoformat(),
                            [f.path.file_name for f in gcs_files],
                        )
                        conceptual_files.append(
                            RawBigQueryFileMetadataSummary(
                                gcs_files=gcs_files, file_tag=file_tag
                            )
                        )
                    else:
                        # TODO(#30170) add alerting rule to have this notification sent to
                        # either pager duty or slack
                        logger.error(
                            "Skipping grouping for %s on %s, found %s but expected %s paths: %s",
                            file_tag,
                            upload_date.isoformat(),
                            len(gcs_files),
                            expected_chunk_count,
                            [f.path.file_name for f in gcs_files],
                        )

                        incomplete_group_date = upload_date

        return conceptual_files

    def _build_insert_row_for_conceptual_file(
        self, metadata: RawBigQueryFileMetadataSummary
    ) -> str:
        row_contents = [
            self._region_code,
            self._raw_data_instance.value,
            metadata.file_tag,
            max(
                metadata.gcs_files, key=lambda x: x.parts.utc_upload_datetime
            ).parts.utc_upload_datetime.isoformat(),
            0,  # False
        ]

        return "\n(" + ", ".join([f"'{value}'" for value in row_contents]) + ")"

    def _register_new_conceptual_files_sql_query(
        self,
        unregistered_recognized_bq_metadata: List[RawBigQueryFileMetadataSummary],
    ) -> str:
        values = ",".join(
            [
                self._build_insert_row_for_conceptual_file(metadata)
                for metadata in unregistered_recognized_bq_metadata
            ]
        )
        return StrictStringFormatter().format(ADD_ROWS, values=values)

    @staticmethod
    def _build_gcs_update_rows_for_bq_file(
        metadata: RawBigQueryFileMetadataSummary,
    ) -> Iterator[str]:
        for gcs_file in metadata.gcs_files:
            yield f"({gcs_file.gcs_file_id}, {metadata.file_id})"

    def _add_file_id_to_gcs_table_sql_query(
        self, registered_bq_metadata: List[RawBigQueryFileMetadataSummary]
    ) -> str:

        values = ",".join(
            [
                row
                for metadata in registered_bq_metadata
                for row in self._build_gcs_update_rows_for_bq_file(metadata)
            ]
        )

        return StrictStringFormatter().format(
            UPDATE_GCS_FILES_WITH_FILE_ID, values=values
        )
