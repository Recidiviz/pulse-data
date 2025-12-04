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
from typing import Dict, Iterable, Iterator, List, NamedTuple, Optional, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.metadata import SKIPPED_FILE_ERRORS
from recidiviz.airflow.dags.raw_data.utils import (
    get_direct_ingest_region_raw_config,
    logger,
    partition_as_list,
)
from recidiviz.ingest.direct.raw_data.raw_data_import_chunked_file_handler import (
    RawDataImportChunkedFileHandler,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.raw_data.state_raw_file_chunking_metadata_factory import (
    StateRawFileChunkingMetadataFactory,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import (
    RawBigQueryFileMetadata,
    RawDataFilesSkippedError,
    RawGCSFileMetadata,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

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

GET_EXISTING_BQ_METADATA_INFO_FOR_GCS_ROWS = """
SELECT 
    bq.file_id, 
    bq.file_processed_time, 
    gcs.is_invalidated as gcs_is_invalidated, 
    bq.is_invalidated as bq_is_invalidated,
    gcs.normalized_file_name
FROM direct_ingest_raw_big_query_file_metadata as bq
INNER JOIN direct_ingest_raw_gcs_file_metadata as gcs
ON bq.file_id = gcs.file_id 
WHERE bq.file_id in ({file_ids});"""

ExistingGCSBQMetadata = NamedTuple(
    "ExistingGCSBQMetadata",
    [
        ("file_id", int),
        ("file_processed_time", datetime.datetime),
        ("gcs_is_invalidated", bool),
        ("bq_is_invalidated", bool),
        ("normalized_file_name", str),
    ],
)

MAX_PROCESSED_UPDATE_DATETIME_FOR_FILE_TAG = """
SELECT file_tag, MAX(update_datetime) AS max_processed_update_datetime
FROM direct_ingest_raw_big_query_file_metadata
WHERE raw_data_instance = '{raw_data_instance}' 
AND is_invalidated IS FALSE 
AND file_processed_time IS NOT NULL 
AND region_code = '{region_code}'
GROUP BY file_tag;
"""

FileTagMaxUpdateDatetime = NamedTuple(
    "FileTagMaxUpdateDatetime",
    [
        ("file_tag", str),
        ("max_processed_update_datetime", datetime.datetime),
    ],
)


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
    ) -> None:
        super().__init__()
        self._region_code = region_code.upper()
        self._raw_data_instance = raw_data_instance
        self._get_all_unprocessed_gcs_file_metadata_task_id = (
            get_all_unprocessed_gcs_file_metadata_task_id
        )
        self._region_raw_file_config: Optional[DirectIngestRegionRawFileConfig] = None
        self._chunked_file_handler = RawDataImportChunkedFileHandler(
            state_chunked_file_metadata=StateRawFileChunkingMetadataFactory.build(
                region_code=region_code
            )
        )

    @property
    def region_raw_file_config(self) -> DirectIngestRegionRawFileConfig:
        if not self._region_raw_file_config:
            self._region_raw_file_config = get_direct_ingest_region_raw_config(
                self._region_code
            )
        return self._region_raw_file_config

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[str]:
        """After pulling in a list of RawGCSFileMetadata from xcom, processes and
        registers all RawGCSFileMetadata not yet registered in the raw bq file
        metadata table (i.e. does not yet have a file_id). Returns serialized
        RawBigQueryFileMetadata for successfully processed and grouped
        RawGCSFileMetadata.
        """
        # --- get existing file info from xcom -----------------------------------------

        unprocessed_gcs_file_metadata: List[RawGCSFileMetadata] = [
            RawGCSFileMetadata.deserialize(xcom_metadata)
            for xcom_metadata in operator.xcom_pull(
                context,
                key="return_value",
                task_ids=self._get_all_unprocessed_gcs_file_metadata_task_id,
            )
        ]

        if not unprocessed_gcs_file_metadata:
            logger.info("Found no gcs file paths to process; skipping bq registration")
            # TODO(#33971) add multiple outputs here to automatically do this
            operator.xcom_push(
                context=context,
                key=SKIPPED_FILE_ERRORS,
                value=[],
            )
            return []

        # --- splits between new and already seen metadata -----------------------------

        (
            bq_unregistered_gcs_metadata,
            already_bq_registered_gcs_metadata,
        ) = partition_as_list(lambda x: x.file_id, unprocessed_gcs_file_metadata)

        # --- process new metadata and reconcile already seen metadata -----------------

        (
            newly_bq_registered_bq_metadata,
            skipped_unregistered_gcs_metadata,
        ) = self._register_bq_unregistered_metadata(
            postgres_hook, bq_unregistered_gcs_metadata
        )

        (
            already_bq_registered_bq_metadata,
            skipped_registered_gcs_metadata,
        ) = self._reconcile_already_registered_files(
            postgres_hook, already_bq_registered_gcs_metadata
        )

        # -- ensure upload order by update_datetime is enforced ------------------------

        all_registered_bq_metadata = [
            *newly_bq_registered_bq_metadata,
            *already_bq_registered_bq_metadata,
        ]
        # TODO(#33879) add alerting infra to notify folks of skipped files
        skipped_file_errors = [
            *skipped_unregistered_gcs_metadata,
            *skipped_registered_gcs_metadata,
        ]

        file_tag_max_update_datetime = self._get_file_tag_max_processed_update_datetime(
            postgres_hook
        )

        (
            all_import_ready_bq_metadata,
            all_skipped_file_errors,
        ) = self._filter_registered_bq_metadata_by_skipped_files_and_max_update_datetime(
            all_registered_bq_metadata,
            skipped_file_errors,
            file_tag_max_update_datetime,
        )

        # --- build xcom output ----------------–––-------------------------------------

        # TODO(#33971) add multiple outputs here to automatically do this
        operator.xcom_push(
            context=context,
            key=SKIPPED_FILE_ERRORS,
            value=[
                skipped_error.serialize() for skipped_error in all_skipped_file_errors
            ],
        )

        return [metadata.serialize() for metadata in all_import_ready_bq_metadata]

    def _register_bq_unregistered_metadata(
        self,
        postgres_hook: PostgresHook,
        bq_unregistered_gcs_metadata: List[RawGCSFileMetadata],
    ) -> Tuple[List[RawBigQueryFileMetadata], List[RawDataFilesSkippedError]]:
        """Uses |bq_unregistered_gcs_metadata| to build a list of RawBigQueryFileMetadata
        objects, not registering unrecognized file tags. Returns both our newly
        registered RawBigQueryFileMetadata and any RawGCSFileMetadata we didn't register
        with the operations db.
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

        (
            unregistered_recognized_bq_metadata,
            skipped_groups,
        ) = self._group_unregistered_conceptual_files(
            bq_unregistered_recognized_gcs_metadata
        )

        if not unregistered_recognized_bq_metadata:
            return [], skipped_groups

        bq_file_ids = postgres_hook.get_records(
            self._register_new_conceptual_files_sql_query(
                unregistered_recognized_bq_metadata
            )
        )

        # --- last, update bq metadata & gcs table w/ newly created objs ---------------

        # here we assume bq_file_ids is returned in the order that we inserted the values
        registered_bq_metadata: List[RawBigQueryFileMetadata] = []
        for i, metadata in enumerate(unregistered_recognized_bq_metadata):
            metadata.file_id = one(bq_file_ids[i])
            registered_bq_metadata.append(metadata)

        postgres_hook.get_records(
            self._add_file_id_to_gcs_table_sql_query(registered_bq_metadata)
        )

        return registered_bq_metadata, skipped_groups

    def _group_unregistered_conceptual_files(
        self,
        bq_unregistered_recognized_gcs_metadata: List[RawGCSFileMetadata],
    ) -> Tuple[List[RawBigQueryFileMetadata], List[RawDataFilesSkippedError]]:
        """Uses |bq_unregistered_recognized_gcs_metadata| to build 'conceptual'
        RawBigQueryFileMetadata by grouping files whose raw file config indicates
        that they are a 'chunked file' by upload_date. Returns both our newly grouped
        conceptual RawBigQueryFileMetadata and any RawGCSFileMetadata we skipped
        grouping.
        """
        # --- group chunked files by (file_tag, upload_date) ---------------------------

        skipped_files: List[RawDataFilesSkippedError] = []
        conceptual_files: List[RawBigQueryFileMetadata] = []
        chunked_files: Dict[
            str, Dict[datetime.date, List[RawGCSFileMetadata]]
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
                RawBigQueryFileMetadata(
                    gcs_files=[metadata],
                    file_tag=metadata.parts.file_tag,
                    update_datetime=metadata.parts.utc_upload_datetime,
                )
            )

        # TODO(#30138) make the mechanism of knowing when we have all file chunks less brittle
        # (i.e. if two files are split between two days 11:59 and 12:01 we won't group
        # them properly).
        # --- if relevant, determine if chunked file groups are complete ---------------

        if chunked_files:

            for file_tag, upload_date_to_gcs_files in chunked_files.items():
                for upload_date, gcs_files in upload_date_to_gcs_files.items():

                    logger.info(
                        "Found [%s] files for [%s] on [%s]; attempting to coalesce...",
                        len(gcs_files),
                        file_tag,
                        upload_date,
                    )

                    (
                        conceptual_files_for_file_tag,
                        skipped_errors_for_file_tag,
                    ) = self._chunked_file_handler.coalesce_chunked_files(
                        file_tag=file_tag, gcs_files=gcs_files
                    )

                    conceptual_files.extend(conceptual_files_for_file_tag)
                    skipped_files.extend(skipped_errors_for_file_tag)
                    if skipped_errors_for_file_tag:
                        for skipped_error_for_file_tag in skipped_errors_for_file_tag:
                            logger.error(skipped_error_for_file_tag.skipped_message)

        return conceptual_files, skipped_files

    def _reconcile_already_registered_files(
        self,
        postgres_hook: PostgresHook,
        already_bq_registered_gcs_metadata: List[RawGCSFileMetadata],
    ) -> Tuple[List[RawBigQueryFileMetadata], List[RawDataFilesSkippedError]]:
        """Reconciles |already_bq_registered_gcs_metadata| against with the state of the
        operations database.

        If a conceptual file is missing paths or has already been processed, we will
        skip importing this path.

        Returns all successfully reconciled RawBigQueryFileMetadata and any RawGCSFileMetadata
        that we cannot import.
        """

        if not already_bq_registered_gcs_metadata:
            return [], []

        gcs_files_by_file_id = {
            file_id: list(conceptual_file_group)
            for file_id, conceptual_file_group in groupby(
                sorted(
                    already_bq_registered_gcs_metadata,
                    key=lambda x: assert_type(x.file_id, int),
                ),
                key=lambda x: x.file_id,
            )
        }

        existing_gcs_and_bq_rows = [
            ExistingGCSBQMetadata(*row)
            for row in postgres_hook.get_records(
                self._build_existing_bq_metadata_info(gcs_files_by_file_id.keys())
            )
        ]

        existing_gcs_and_bq_rows_by_file_id = {
            file_id: list(existing_gcs_and_bq_rows)
            for file_id, existing_gcs_and_bq_rows in groupby(
                sorted(
                    existing_gcs_and_bq_rows,
                    key=lambda x: x.file_id,
                ),
                key=lambda x: x.file_id,
            )
        }

        valid_unprocessed_bq_metadata: List[RawBigQueryFileMetadata] = []
        skipped_files: List[RawDataFilesSkippedError] = []

        for file_id, gcs_files in gcs_files_by_file_id.items():
            existing_gcs_bq_metadata = existing_gcs_and_bq_rows_by_file_id[file_id]

            paths_in_bucket = {gcs_file.path.blob_name for gcs_file in gcs_files}
            paths_in_operations_db = {
                metadata.normalized_file_name for metadata in existing_gcs_bq_metadata
            }

            if paths_only_in_one := paths_in_bucket ^ paths_in_operations_db:

                skipped_error = RawDataFilesSkippedError(
                    file_paths=[gcs_file.path for gcs_file in gcs_files],
                    file_tag=gcs_files[0].parts.file_tag,
                    update_datetime=max(
                        gcs_file.parts.utc_upload_datetime for gcs_file in gcs_files
                    ),
                    skipped_message=(
                        f"Skipping import for file_id [{file_id}], file_tag [{gcs_files[0].parts.file_tag}]: "
                        f"mismatched grouped paths [{paths_only_in_one}] \n\t - paths in bucket: "
                        f"[{paths_in_bucket}] \n\t - paths in operations db: [{paths_in_operations_db}]"
                    ),
                )

                logger.error(skipped_error.skipped_message)
                skipped_files.append(skipped_error)
                continue

            if already_processed := [
                metadata.normalized_file_name
                for metadata in existing_gcs_bq_metadata
                if metadata.file_processed_time is not None
            ]:
                # not adding to skipped files here, since we already have processed these
                # files, they are not important in determining if they are blocking
                # other imports. we assume this means that we failed to import the
                # last time
                logger.error(
                    "Skipping import for file_id [%s] as [%s] is already been processed",
                    file_id,
                    already_processed,
                )
                continue

            if any(
                metadata.parts.file_tag not in self.region_raw_file_config.raw_file_tags
                for metadata in gcs_files
            ):
                logger.error(
                    "Skipping import for file_id [%s] as [%s] is no longer a valid file tag",
                    file_id,
                    gcs_files[0].parts.file_tag,
                )
                skipped_files.append(
                    RawDataFilesSkippedError(
                        file_paths=[gcs_file.path for gcs_file in gcs_files],
                        file_tag=gcs_files[0].parts.file_tag,
                        update_datetime=max(
                            gcs_file.parts.utc_upload_datetime for gcs_file in gcs_files
                        ),
                        skipped_message=(
                            f"Skipping import for file_id [{file_id}], file_tag [{gcs_files[0].parts.file_tag}]: "
                            f"file_tag [{gcs_files[0].parts.file_tag}] no longer exists!"
                        ),
                    )
                )
                continue

            valid_unprocessed_bq_metadata.append(
                RawBigQueryFileMetadata.from_gcs_files(list(gcs_files))
            )

        return valid_unprocessed_bq_metadata, skipped_files

    @staticmethod
    def _filter_registered_bq_metadata_by_skipped_files_and_max_update_datetime(
        all_registered_bq_metadata: List[RawBigQueryFileMetadata],
        previous_skipped_file_errors: List[RawDataFilesSkippedError],
        file_tag_max_update_datetime: Dict[str, FileTagMaxUpdateDatetime],
    ) -> Tuple[List[RawBigQueryFileMetadata], List[RawDataFilesSkippedError]]:
        """Filters out registered bq metadata that have an gcs file that was skipped
        with the same file_tag and an update_datetime before it, and any files that
        have an update_datetime before their file_tag's max processed update_datetime.
        """

        file_tag_to_min_skipped_update_datetime = {
            file_tag: min(group, key=lambda x: x.update_datetime)
            for file_tag, group in groupby(
                sorted(previous_skipped_file_errors, key=lambda x: x.file_tag),
                lambda x: x.file_tag,
            )
        }

        non_blocked_registered_bq_metadata: List[RawBigQueryFileMetadata] = []
        all_skipped_errors: List[RawDataFilesSkippedError] = [
            *previous_skipped_file_errors
        ]

        for bq_metadata in all_registered_bq_metadata:
            if (
                bq_metadata.file_tag in file_tag_to_min_skipped_update_datetime
                and file_tag_to_min_skipped_update_datetime[
                    bq_metadata.file_tag
                ].update_datetime
                < bq_metadata.update_datetime
            ):
                skipped_error = RawDataFilesSkippedError(
                    file_paths=[gcs_file.path for gcs_file in bq_metadata.gcs_files],
                    file_tag=bq_metadata.file_tag,
                    update_datetime=bq_metadata.update_datetime,
                    skipped_message=(
                        f"Skipping import for file_id [{bq_metadata.file_id}], file_tag [{bq_metadata.file_tag}]: "
                        f"path {[path.file_name for path in file_tag_to_min_skipped_update_datetime[bq_metadata.file_tag].file_paths]} was previously skipped"
                    ),
                )

                logger.error(skipped_error.skipped_message)
                all_skipped_errors.append(skipped_error)
                continue

            if (
                bq_metadata.file_tag in file_tag_max_update_datetime
                and bq_metadata.update_datetime
                < file_tag_max_update_datetime[
                    bq_metadata.file_tag
                ].max_processed_update_datetime
            ):
                max_update_datetime_str = file_tag_max_update_datetime[
                    bq_metadata.file_tag
                ].max_processed_update_datetime.isoformat()
                skipped_error = RawDataFilesSkippedError(
                    file_paths=[gcs_file.path for gcs_file in bq_metadata.gcs_files],
                    file_tag=bq_metadata.file_tag,
                    update_datetime=bq_metadata.update_datetime,
                    skipped_message=(
                        f"Skipping import for file_id [{bq_metadata.file_id}], file_tag "
                        f"[{bq_metadata.file_tag}]: update_datetime [{bq_metadata.update_datetime.isoformat()}] "
                        f"is before max processed update_datetime [{max_update_datetime_str}]. "
                        f"In order to import this file, you must invalidate and re-import "
                        f"data from [{bq_metadata.update_datetime.isoformat()}] to present"
                    ),
                )

                logger.error(skipped_error.skipped_message)
                all_skipped_errors.append(skipped_error)
                continue

            non_blocked_registered_bq_metadata.append(bq_metadata)

        return non_blocked_registered_bq_metadata, all_skipped_errors

    def _build_insert_row_for_conceptual_file(
        self, metadata: RawBigQueryFileMetadata
    ) -> str:
        row_contents = [
            self._region_code,
            self._raw_data_instance.value,
            metadata.file_tag,
            metadata.update_datetime.isoformat(),
            0,  # "0"::bool evaluates to False
        ]

        return "\n(" + ", ".join([f"'{value}'" for value in row_contents]) + ")"

    def _register_new_conceptual_files_sql_query(
        self,
        unregistered_recognized_bq_metadata: List[RawBigQueryFileMetadata],
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
        metadata: RawBigQueryFileMetadata,
    ) -> Iterator[str]:
        for gcs_file in metadata.gcs_files:
            yield f"({gcs_file.gcs_file_id}, {metadata.file_id})"

    def _add_file_id_to_gcs_table_sql_query(
        self, registered_bq_metadata: List[RawBigQueryFileMetadata]
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

    @staticmethod
    def _build_existing_bq_metadata_info(file_ids: Iterable[int]) -> str:
        file_ids_str = ",".join(str(file_id) for file_id in file_ids)
        return StrictStringFormatter().format(
            GET_EXISTING_BQ_METADATA_INFO_FOR_GCS_ROWS, file_ids=file_ids_str
        )

    def _build_max_update_datetime_query(self) -> str:
        return StrictStringFormatter().format(
            MAX_PROCESSED_UPDATE_DATETIME_FOR_FILE_TAG,
            raw_data_instance=self._raw_data_instance.value,
            region_code=self._region_code,
        )

    def _get_file_tag_max_processed_update_datetime(
        self, postgres_hook: PostgresHook
    ) -> Dict[str, FileTagMaxUpdateDatetime]:
        max_update_datetimes = [
            FileTagMaxUpdateDatetime(*row)
            for row in postgres_hook.get_records(
                self._build_max_update_datetime_query()
            )
        ]

        return {
            max_update_datetime.file_tag: max_update_datetime
            for max_update_datetime in max_update_datetimes
        }
