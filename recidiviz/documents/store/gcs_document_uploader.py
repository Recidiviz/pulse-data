# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Uploads documents from BQ temp tables to GCS, writing upload successes and failures to CSV."""

import logging
from collections.abc import Iterator
from datetime import datetime
from functools import partial

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_csv_part_writer import GcsCsvPartWriter
from recidiviz.cloud_storage.gcs_file_system import TEXT_CONTENT_TYPE, GCSFileSystem
from recidiviz.cloud_storage.gcsfs_factory import POOL_MAXSIZE as GCSFS_POOL_MAXSIZE
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
    gcs_path_for_task_output,
)
from recidiviz.documents.store.document_store_types import DocumentUploadBatch
from recidiviz.documents.store.document_upload_status_table import (
    DOCUMENT_UPLOAD_FAILURE,
    DOCUMENT_UPLOAD_SUCCESS,
    DocumentUploadStatusTable,
)
from recidiviz.utils.future_executor import map_with_bounded_concurrency
from recidiviz.utils.types import assert_type

MAX_UPLOAD_WORKERS = int(GCSFS_POOL_MAXSIZE / 2)

# Number of upload status rows buffered in memory before being flushed to a
# status CSV part file in GCS. Bounds the status CSV's contribution to peak
# memory regardless of how many documents a batch contains.
STATUS_CSV_ROWS_PER_PART = 100_000

# Maximum number of failed upload results retained (and included in the raised
# error message) per batch, so a batch where every upload fails cannot itself
# exhaust memory accumulating failure details.
MAX_FAILED_UPLOADS_REPORTED = 25


@attr.define(frozen=True, kw_only=True)
class NewDocumentContentsRow:
    document_contents_id: str = attr.ib(validator=attr_validators.is_str)
    document_text: str = attr.ib(validator=attr_validators.is_str)
    document_length_bytes: int = attr.ib(validator=attr_validators.is_int)


@attr.define(frozen=True, kw_only=True)
class DocumentUploadResult:
    document_contents_id: str = attr.ib(validator=attr_validators.is_str)
    document_length_bytes: int = attr.ib(validator=attr_validators.is_int)
    error_message: str | None = attr.ib(validator=attr_validators.is_opt_str)

    @property
    def status(self) -> str:
        return (
            DOCUMENT_UPLOAD_SUCCESS
            if self.error_message is None
            else DOCUMENT_UPLOAD_FAILURE
        )


@attr.define(frozen=True, kw_only=True)
class GcsDocumentUploader:
    """Uploads documents from BQ temp tables to GCS."""

    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)
    project_id: str = attr.ib(validator=attr_validators.is_str)
    big_query_client: BigQueryClient = attr.ib()
    fs: GCSFileSystem = attr.ib()
    # Unique identifier for the DAG run; used in GCS output paths and status CSV rows.
    run_id: str = attr.ib(validator=attr_validators.is_str)
    # Index of this task within the DAG run's mapped task group; used to
    # namespace output CSVs so parallel tasks don't collide.
    task_index: int = attr.ib(validator=attr_validators.is_int)
    # Timestamp written to status CSV rows as the upload time for all
    # documents processed by this task.
    upload_datetime: datetime = attr.ib(validator=attr_validators.is_datetime)
    # Sandbox run prefix; when set, document text is uploaded to the state's
    # sandbox blob-storage bucket, namespaced by this prefix. None in production.
    sandbox_prefix: str | None = attr.ib(validator=attr_validators.is_opt_str)

    def run(self, upload_batches: list[DocumentUploadBatch]) -> None:
        """For each `DocumentUploadBatch` specifying a batch of documents within a `temp_new_document_contents_`
        table to process:
         1. query the `document_contents_id`s and `document_text`s from BQ
         2. upload `document_text`s to GCS concurrently
         3. write CSV part files with the document upload statuses to be subsequently uploaded to the `document_upload_status` BQ table.

        Documents are streamed from the BQ result with a bounded number of
        uploads in flight, so peak memory is independent of batch size.
        """
        errors: list[str] = []

        for batch_index, upload_batch in enumerate(upload_batches):
            try:
                self._process_batch(upload_batch, batch_index)
            except Exception as e:
                errors.append(f"{self._log_prefix(upload_batch.collection_name)} {e}")

        if errors:
            raise RuntimeError(
                f"Document upload completed with "
                f"{len(errors)} error(s):\n" + "\n".join(errors)
            )

    def _log_prefix(self, collection_name: str) -> str:
        return f"[{self.state_code.value}] Collection [{collection_name}]:"

    def _process_batch(
        self,
        upload_batch: DocumentUploadBatch,
        batch_index: int,
    ) -> None:
        """Queries the batch's document rows from BQ and uploads each document
        text to GCS with bounded concurrency, streaming upload status rows to
        CSV part files as uploads complete. Peak memory is proportional to
        `MAX_UPLOAD_WORKERS` plus one CSV part buffer, independent of how many
        documents the batch contains."""
        collection_name = upload_batch.collection_name
        document_contents_rows = self._query_documents(upload_batch)

        num_succeeded = 0
        num_failed = 0
        failed_upload_samples: list[DocumentUploadResult] = []

        with GcsCsvPartWriter(
            fs=self.fs,
            path_for_part=partial(
                self._gcs_path_for_batch_part,
                collection_name=collection_name,
                batch_index=batch_index,
            ),
            rows_per_part=STATUS_CSV_ROWS_PER_PART,
        ) as status_csv_writer:
            with map_with_bounded_concurrency(
                work_fn=partial(self._upload_document, collection_name),
                items=document_contents_rows,
                max_concurrency=MAX_UPLOAD_WORKERS,
            ) as completed_items:
                for completed in completed_items:
                    result = assert_type(completed.result, DocumentUploadResult)
                    status_csv_writer.write_row(
                        self._status_csv_row_for_upload_result(
                            collection_name=collection_name, result=result
                        )
                    )
                    if result.error_message is None:
                        num_succeeded += 1
                    else:
                        num_failed += 1
                        if len(failed_upload_samples) < MAX_FAILED_UPLOADS_REPORTED:
                            failed_upload_samples.append(result)

        logging.info(
            "%s successfully uploaded %d documents. %d documents failed to upload.",
            self._log_prefix(collection_name),
            num_succeeded,
            num_failed,
        )
        if num_failed:
            failure_details = "\n -".join(str(f) for f in failed_upload_samples)
            if num_failed > len(failed_upload_samples):
                failure_details += (
                    f"\n ... and {num_failed - len(failed_upload_samples)} more"
                )
            raise RuntimeError(
                f"{num_failed} documents failed to upload:\n" + failure_details
            )

    def _query_documents(
        self,
        upload_batch: DocumentUploadBatch,
    ) -> Iterator[NewDocumentContentsRow]:
        """Queries `document_contents_id` and `document_text` from the
        `temp_new_document_contents_` table for the given batch number.
        Returns a lazy iterator over the batch's rows — the underlying BQ row
        iterator fetches pages as they are consumed, so the full batch is never
        materialized in memory."""
        query = f"""
            SELECT
                {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
                {DOCUMENT_TEXT_COLUMN_NAME},
                {DOCUMENT_LENGTH_BYTES_COLUMN_NAME}
            FROM `{upload_batch.temp_new_document_contents_table_address.to_str()}`
            WHERE {DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME} = {upload_batch.batch_number}
        """
        query_job = self.big_query_client.run_query_async(
            query_str=query,
            use_query_cache=False,
        )
        # The outermost iterable of a generator expression is evaluated when the
        # expression is created, so a query failure raises here — before any
        # uploads or status CSV writes start — not mid-iteration.
        return (
            NewDocumentContentsRow(
                document_contents_id=row[DOCUMENT_CONTENTS_ID_COLUMN_NAME],
                document_text=row[DOCUMENT_TEXT_COLUMN_NAME],
                document_length_bytes=row[DOCUMENT_LENGTH_BYTES_COLUMN_NAME],
            )
            for row in query_job.result()
        )

    def _upload_document(
        self, collection_name: str, document_contents_row: NewDocumentContentsRow
    ) -> DocumentUploadResult:
        error_msg = None
        try:
            path = gcs_path_for_document(
                project_id=self.project_id,
                state_code=self.state_code,
                collection_name=collection_name,
                document_contents_id=document_contents_row.document_contents_id,
                sandbox_prefix=self.sandbox_prefix,
            )
            self.fs.upload_from_string(
                path=path,
                contents=document_contents_row.document_text,
                content_type=TEXT_CONTENT_TYPE,
            )
        except Exception as e:
            error_msg = str(e)
            logging.error(
                "%s Failed to upload document [%s]: %s",
                self._log_prefix(collection_name),
                document_contents_row.document_contents_id,
                error_msg,
            )

        return DocumentUploadResult(
            document_contents_id=document_contents_row.document_contents_id,
            document_length_bytes=document_contents_row.document_length_bytes,
            error_message=error_msg,
        )

    def _status_csv_row_for_upload_result(
        self, *, collection_name: str, result: DocumentUploadResult
    ) -> tuple[str, str, str, str, str, int, str | None]:
        """Returns a tuple containing datat that can be written to a document
        upload status table CSV.
        """
        return DocumentUploadStatusTable.to_csv_row(
            document_contents_id=result.document_contents_id,
            collection_name=collection_name,
            run_id=self.run_id,
            upload_datetime=self.upload_datetime,
            status=result.status,
            document_length_bytes=result.document_length_bytes,
            error_message=result.error_message,
        )

    def _gcs_path_for_batch_part(
        self, part_index: int, *, collection_name: str, batch_index: int
    ) -> GcsfsFilePath:
        """Returns the GCS path for one status CSV part file of a batch."""
        return gcs_path_for_task_output(
            project_id=self.project_id,
            state_code=self.state_code,
            run_id=self.run_id,
            collection_name=collection_name,
            task_index=self.task_index,
            batch_index=batch_index,
            part_index=part_index,
        )
