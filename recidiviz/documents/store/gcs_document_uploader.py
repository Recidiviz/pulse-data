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
import csv
import io
import logging
from concurrent import futures
from datetime import datetime

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import (
    CSV_CONTENT_TYPE,
    TEXT_CONTENT_TYPE,
    GCSFileSystem,
)
from recidiviz.cloud_storage.gcsfs_factory import POOL_MAXSIZE as GCSFS_POOL_MAXSIZE
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    SEQUENCE_NUM_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_document,
    gcs_path_for_task_output,
)
from recidiviz.documents.store.document_upload_status_table import (
    DOCUMENT_UPLOAD_FAILURE,
    DOCUMENT_UPLOAD_SUCCESS,
    DocumentUploadStatusTable,
)
from recidiviz.documents.store.new_document_discovery import DocumentBatchRange

MAX_UPLOAD_WORKERS = int(GCSFS_POOL_MAXSIZE / 2)
# TODO(#73430) Compute a more precise timeout
BATCH_UPLOAD_TIMEOUT_SECONDS = 60 * 60


@attr.define(frozen=True, kw_only=True)
class NewDocumentContentsRow:
    document_contents_id: str = attr.ib(validator=attr_validators.is_str)
    document_text: str = attr.ib(validator=attr_validators.is_str)


@attr.define(frozen=True, kw_only=True)
class DocumentUploadResult:
    document_contents_id: str = attr.ib(validator=attr_validators.is_str)
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
    job_id: str = attr.ib(validator=attr_validators.is_str)
    # Index of this task within the DAG run's mapped task group; used to
    # namespace output CSVs so parallel tasks don't collide.
    task_index: int = attr.ib(validator=attr_validators.is_int)
    # Timestamp written to status CSV rows as the upload time for all
    # documents processed by this task.
    upload_datetime: datetime = attr.ib(validator=attr_validators.is_datetime)

    def run(self, batch_ranges: list[DocumentBatchRange]) -> None:
        """For each `DocumentBatchRange` specifying a range of documents within a `temp_new_document_contents_`
        table to process:
         1. query the `document_contents_id`s and `document_text`s from BQ
         2. upload `document_text`s to GCS concurrently
         3. write a CSV file with the document upload statuses to be subsequently uploaded to the `document_upload_status` BQ table.
        """
        errors: list[str] = []

        # TODO(#73430) Build subbatches within each DocumentBatchRange in new_document_discovery
        for batch_index, batch_range in enumerate(batch_ranges):
            try:
                self._process_batch(
                    batch_range,
                    batch_index,
                    batch_timeout_seconds=BATCH_UPLOAD_TIMEOUT_SECONDS,
                )
            except Exception as e:
                errors.append(f"{self._log_prefix(batch_range.collection_name)} {e}")

        if errors:
            raise RuntimeError(
                f"Document upload completed with "
                f"{len(errors)} error(s):\n" + "\n".join(errors)
            )

    def _log_prefix(self, collection_name: str) -> str:
        return f"[{self.state_code.value}] Collection [{collection_name}]:"

    def _process_batch(
        self,
        batch_range: DocumentBatchRange,
        batch_index: int,
        batch_timeout_seconds: int,
    ) -> None:
        document_contents_rows = self._query_documents(batch_range)

        results = self._upload_documents(
            batch_range.collection_name,
            document_contents_rows,
            timeout_seconds=batch_timeout_seconds,
        )

        self._write_status_csv(results, batch_index)

        failed_uploads = [r for r in results if r.error_message]
        logging.info(
            "%s successfully uploaded %d documents. %d documents failed to upload.",
            self._log_prefix(batch_range.collection_name),
            len(results) - len(failed_uploads),
            len(failed_uploads),
        )
        if failed_uploads:
            raise RuntimeError(
                f"{len(failed_uploads)} documents failed to upload:\n"
                + "\n -".join(str(f) for f in failed_uploads)
            )

    def _query_documents(
        self,
        batch_range: DocumentBatchRange,
    ) -> list[NewDocumentContentsRow]:
        """Queries `document_contents_id` and `document_text` from the
        `temp_new_document_contents_` table for the given sequence_num range."""
        query = f"""
            SELECT
                {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
                {DOCUMENT_TEXT_COLUMN_NAME}
            FROM `{batch_range.temp_new_document_contents_table_address.to_str()}`
            WHERE {SEQUENCE_NUM_COLUMN_NAME} >= {batch_range.start_sequence_num_inclusive}
              AND {SEQUENCE_NUM_COLUMN_NAME} < {batch_range.end_sequence_num_exclusive}
        """
        query_job = self.big_query_client.run_query_async(
            query_str=query,
            use_query_cache=False,
        )
        return [
            NewDocumentContentsRow(
                document_contents_id=row[DOCUMENT_CONTENTS_ID_COLUMN_NAME],
                document_text=row[DOCUMENT_TEXT_COLUMN_NAME],
            )
            for row in query_job.result()
        ]

    def _upload_documents(
        self,
        collection_name: str,
        document_content_rows: list[NewDocumentContentsRow],
        timeout_seconds: int,
    ) -> list[DocumentUploadResult]:
        """Uploads documents concurrently, returning a list of upload results.
        If the batch upload process exceeds the specified timeout, any remaining uploads that have not
        completed will be marked as failed."""
        # Not using `with` because its __exit__ calls shutdown(wait=True),
        # which would block on still-running futures after a timeout.
        executor = futures.ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS)
        upload_futures = [
            executor.submit(self._upload_document, collection_name, row)
            for row in document_content_rows
        ]

        results: list[DocumentUploadResult] = []
        try:
            for future in futures.as_completed(upload_futures, timeout=timeout_seconds):
                results.append(future.result())
        except futures.TimeoutError:
            logging.error(
                "%s Batch upload timed out after %ds; "
                "marking incomplete uploads as failed",
                self._log_prefix(collection_name),
                timeout_seconds,
            )
            completed = {r.document_contents_id for r in results}
            for row in document_content_rows:
                if row.document_contents_id not in completed:
                    results.append(
                        DocumentUploadResult(
                            document_contents_id=row.document_contents_id,
                            error_message=f"Batch timed out after {timeout_seconds}s",
                        )
                    )

        executor.shutdown(wait=False, cancel_futures=True)
        return results

    def _upload_document(
        self, collection_name: str, document_contents_row: NewDocumentContentsRow
    ) -> DocumentUploadResult:
        error_msg = None
        try:
            path = gcs_path_for_document(
                self.project_id,
                self.state_code,
                document_contents_row.document_contents_id,
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
            error_message=error_msg,
        )

    def _write_status_csv(
        self,
        results: list[DocumentUploadResult],
        batch_index: int,
    ) -> None:
        """Writes a CSV file to GCS containing upload status for a batch of documents.
        CSV file matches `document_upload_status` table schema."""
        buf = io.StringIO()
        writer = csv.writer(buf)
        for result in results:
            writer.writerow(
                DocumentUploadStatusTable.to_csv_row(
                    document_contents_id=result.document_contents_id,
                    job_id=self.job_id,
                    upload_datetime=self.upload_datetime,
                    status=result.status,
                    error_message=result.error_message,
                )
            )

        output_path = gcs_path_for_task_output(
            project_id=self.project_id,
            state_code=self.state_code,
            job_id=self.job_id,
            task_index=self.task_index,
            batch_index=batch_index,
        )
        self.fs.upload_from_string(
            path=output_path, contents=buf.getvalue(), content_type=CSV_CONTENT_TYPE
        )
