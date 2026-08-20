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
"""Tests for gcs_document_uploader.py."""
import csv
import io
import unittest
from collections.abc import Iterator
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.cloud_storage.gcs_file_system import (
    CSV_CONTENT_TYPE,
    TEXT_CONTENT_TYPE,
    GCSFileSystem,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_path_for_task_output,
)
from recidiviz.documents.store.document_store_types import DocumentUploadBatch
from recidiviz.documents.store.document_upload_status_table import (
    COLLECTION_NAME,
    DOCUMENT_CONTENTS_ID,
    DOCUMENT_UPLOAD_FAILURE,
    DOCUMENT_UPLOAD_SUCCESS,
    STATUS,
    DocumentUploadStatusTable,
)
from recidiviz.documents.store.gcs_document_uploader import (
    MAX_UPLOAD_WORKERS,
    GcsDocumentUploader,
)


def _raise_for_text_uploads(
    path: GcsfsFilePath,
    contents: str,  # pylint: disable=unused-argument
    content_type: str,
) -> None:
    """upload_from_string side effect that fails every document text upload
    while letting status CSV uploads through."""
    if content_type == TEXT_CONTENT_TYPE:
        raise RuntimeError(f"upload failed for {path.blob_name}")


def _make_new_document_contents_row(
    document_contents_id: str, document_text: str
) -> bigquery.table.Row:
    document_length_bytes = len(document_text.encode("utf-8"))
    return bigquery.table.Row(
        [document_contents_id, document_text, document_length_bytes],
        {
            DOCUMENT_CONTENTS_ID_COLUMN_NAME: 0,
            DOCUMENT_TEXT_COLUMN_NAME: 1,
            DOCUMENT_LENGTH_BYTES_COLUMN_NAME: 2,
        },
    )


class TestGcsDocumentUploader(unittest.TestCase):
    """Tests for gcs_document_uploader.py."""

    def setUp(self) -> None:
        self.project_id = "recidiviz-testing"
        self.state_code = StateCode.US_XX
        self.run_id = "test_run_123"
        self.temp_table = ProjectSpecificBigQueryAddress(
            project_id=self.project_id,
            dataset_id="us_xx_document_store_temp",
            table_id="temp_document_test_collection_test_run_123",
        )
        self.bq_client = MagicMock()
        self.fs = MagicMock(spec=GCSFileSystem)
        self.upload_datetime = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        self.uploader = GcsDocumentUploader(
            state_code=self.state_code,
            project_id=self.project_id,
            big_query_client=self.bq_client,
            fs=self.fs,
            run_id=self.run_id,
            task_index=0,
            upload_datetime=self.upload_datetime,
            sandbox_prefix=None,
        )

    def _make_upload_batch(
        self, batch_number: int, collection_name: str = "test_collection"
    ) -> DocumentUploadBatch:
        return DocumentUploadBatch(
            collection_name=collection_name,
            temp_new_document_contents_table_address=self.temp_table,
            batch_number=batch_number,
        )

    def test_uploads_documents_success(self) -> None:
        batch_1_job = MagicMock()
        batch_1_job.result.return_value = [
            _make_new_document_contents_row("doc_a", "text a"),
            _make_new_document_contents_row("doc_b", "text b"),
        ]
        batch_2_job = MagicMock()
        batch_2_job.result.return_value = [
            _make_new_document_contents_row("doc_c", "text c"),
        ]
        self.bq_client.run_query_async.side_effect = [batch_1_job, batch_2_job]

        ranges = [
            self._make_upload_batch(batch_number=0),
            self._make_upload_batch(batch_number=1),
        ]
        self.uploader.run(ranges)

        self.assertEqual(self.bq_client.run_query_async.call_count, 2)

        upload_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == TEXT_CONTENT_TYPE
        ]
        uploaded_paths = {c.kwargs["path"].blob_name for c in upload_calls}
        self.assertEqual(
            uploaded_paths,
            {
                "test_collection/doc_a.txt",
                "test_collection/doc_b.txt",
                "test_collection/doc_c.txt",
            },
        )

        csv_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == CSV_CONTENT_TYPE
        ]
        self.assertEqual(len(csv_calls), 2)
        csv_paths = [c.kwargs["path"] for c in csv_calls]
        expected_paths = [
            gcs_path_for_task_output(
                project_id=self.project_id,
                state_code=self.state_code,
                run_id=self.run_id,
                collection_name="test_collection",
                task_index=0,
                batch_index=i,
                part_index=0,
            )
            for i in range(2)
        ]
        self.assertEqual(csv_paths, expected_paths)

        statuses: dict[str, tuple[str, str]] = {}
        for csv_call in csv_calls:
            reader = csv.DictReader(
                io.StringIO(csv_call.kwargs["contents"]),
                fieldnames=DocumentUploadStatusTable.column_names(),
            )
            for row in reader:
                statuses[row[DOCUMENT_CONTENTS_ID]] = (
                    row[COLLECTION_NAME],
                    row[STATUS],
                )
        self.assertEqual(
            statuses["doc_a"], ("test_collection", DOCUMENT_UPLOAD_SUCCESS)
        )
        self.assertEqual(
            statuses["doc_b"], ("test_collection", DOCUMENT_UPLOAD_SUCCESS)
        )
        self.assertEqual(
            statuses["doc_c"], ("test_collection", DOCUMENT_UPLOAD_SUCCESS)
        )

    def test_document_upload_failure(self) -> None:
        query_job = MagicMock()
        query_job.result.return_value = [
            _make_new_document_contents_row("doc_ok_1", "text ok 1"),
            _make_new_document_contents_row("doc_fail", "text fail"),
            _make_new_document_contents_row("doc_ok_2", "text ok 2"),
        ]
        self.bq_client.run_query_async.return_value = query_job

        def upload_side_effect(
            path: GcsfsFilePath,
            contents: str,  # pylint: disable=unused-argument
            content_type: str,
        ) -> None:
            if (
                content_type == TEXT_CONTENT_TYPE
                and path.blob_name == "test_collection/doc_fail.txt"
            ):
                raise RuntimeError("gcp upload error")

        self.fs.upload_from_string.side_effect = upload_side_effect

        upload_batch = self._make_upload_batch(batch_number=0)
        with self.assertRaisesRegex(
            RuntimeError,
            r"Document upload completed with 1 error\(s\):\n"
            r"\[US_XX\] Collection \[test_collection\]: 1 documents failed to upload:\n"
            r"DocumentUploadResult\(document_contents_id='doc_fail', document_length_bytes=9, error_message='gcp upload error'\)",
        ):
            self.uploader.run([upload_batch])

        upload_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == TEXT_CONTENT_TYPE
        ]
        uploaded_paths = {c.kwargs["path"].blob_name for c in upload_calls}
        self.assertEqual(
            uploaded_paths,
            {
                "test_collection/doc_ok_1.txt",
                "test_collection/doc_fail.txt",
                "test_collection/doc_ok_2.txt",
            },
        )

        csv_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == CSV_CONTENT_TYPE
        ]
        self.assertEqual(len(csv_calls), 1)
        csv_content = csv_calls[0].kwargs["contents"]
        reader = csv.DictReader(
            io.StringIO(csv_content),
            fieldnames=DocumentUploadStatusTable.column_names(),
        )
        statuses = {row[DOCUMENT_CONTENTS_ID]: row[STATUS] for row in reader}
        self.assertEqual(statuses["doc_ok_1"], DOCUMENT_UPLOAD_SUCCESS)
        self.assertEqual(statuses["doc_ok_2"], DOCUMENT_UPLOAD_SUCCESS)
        self.assertEqual(statuses["doc_fail"], DOCUMENT_UPLOAD_FAILURE)

    def test_empty_input(self) -> None:
        self.uploader.run([])

        self.bq_client.run_query_async.assert_not_called()
        self.fs.upload_from_string.assert_not_called()

    def test_query_documents_failure_continues_with_next_batch(self) -> None:
        failing_job = MagicMock()
        failing_job.result.side_effect = RuntimeError("BQ query failed")
        ok_job = MagicMock()
        ok_job.result.return_value = [
            _make_new_document_contents_row("doc_a", "text a")
        ]
        self.bq_client.run_query_async.side_effect = [failing_job, ok_job]

        ranges = [
            self._make_upload_batch(
                batch_number=0, collection_name="failing_collection"
            ),
            self._make_upload_batch(batch_number=0, collection_name="ok_collection"),
        ]
        with self.assertRaisesRegex(
            RuntimeError,
            r"Document upload completed with 1 error\(s\):\n"
            r"\[US_XX\] Collection \[failing_collection\]: BQ query failed",
        ):
            self.uploader.run(ranges)

        upload_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == TEXT_CONTENT_TYPE
        ]
        uploaded_paths = {c.kwargs["path"].blob_name for c in upload_calls}
        self.assertEqual(uploaded_paths, {"ok_collection/doc_a.txt"})

        csv_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == CSV_CONTENT_TYPE
        ]
        self.assertEqual(len(csv_calls), 1)

    def test_write_status_csv_failure_continues_with_next_batch(self) -> None:
        job_1 = MagicMock()
        job_1.result.return_value = [_make_new_document_contents_row("doc_a", "text a")]
        job_2 = MagicMock()
        job_2.result.return_value = [_make_new_document_contents_row("doc_b", "text b")]
        self.bq_client.run_query_async.side_effect = [job_1, job_2]

        def upload_side_effect(
            path: GcsfsFilePath,  # pylint: disable=unused-argument
            contents: str,
            content_type: str,
        ) -> None:
            if content_type == CSV_CONTENT_TYPE and "doc_a" in contents:
                raise RuntimeError("GCS write failed")

        self.fs.upload_from_string.side_effect = upload_side_effect

        ranges = [
            self._make_upload_batch(batch_number=0),
            self._make_upload_batch(batch_number=1),
        ]
        with self.assertRaisesRegex(
            RuntimeError,
            r"Document upload completed with 1 error\(s\):\n"
            r"\[US_XX\] Collection \[test_collection\]: GCS write failed",
        ):
            self.uploader.run(ranges)

        # Both documents should have been uploaded despite the CSV failure
        upload_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == TEXT_CONTENT_TYPE
        ]
        self.assertEqual(len(upload_calls), 2)

    @patch(
        "recidiviz.documents.store.gcs_document_uploader.STATUS_CSV_ROWS_PER_PART", 2
    )
    def test_status_csv_flushed_in_parts(self) -> None:
        """Status rows are flushed to successive part files every
        STATUS_CSV_ROWS_PER_PART rows, with every document's status present
        across the parts."""
        query_job = MagicMock()
        query_job.result.return_value = [
            _make_new_document_contents_row(f"doc_{i}", f"text {i}") for i in range(5)
        ]
        self.bq_client.run_query_async.return_value = query_job

        self.uploader.run([self._make_upload_batch(batch_number=0)])

        csv_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == CSV_CONTENT_TYPE
        ]
        # 5 rows with 2 rows per part -> parts of 2, 2, and 1 rows.
        self.assertEqual(len(csv_calls), 3)
        self.assertEqual(
            [c.kwargs["path"].blob_name for c in csv_calls],
            [
                "test_run_123/test_collection/task_0_0_0.csv",
                "test_run_123/test_collection/task_0_0_1.csv",
                "test_run_123/test_collection/task_0_0_2.csv",
            ],
        )
        statuses = {}
        for csv_call in csv_calls:
            reader = csv.DictReader(
                io.StringIO(csv_call.kwargs["contents"]),
                fieldnames=DocumentUploadStatusTable.column_names(),
            )
            for row in reader:
                statuses[row[DOCUMENT_CONTENTS_ID]] = row[STATUS]
        self.assertEqual(
            statuses,
            {f"doc_{i}": DOCUMENT_UPLOAD_SUCCESS for i in range(5)},
        )

    @patch(
        "recidiviz.documents.store.gcs_document_uploader.MAX_FAILED_UPLOADS_REPORTED", 2
    )
    def test_failure_details_truncated_beyond_reporting_cap(self) -> None:
        """When more uploads fail than MAX_FAILED_UPLOADS_REPORTED, the raised
        error reports the true failure count but only retains details for the
        first few failures."""
        query_job = MagicMock()
        query_job.result.return_value = [
            _make_new_document_contents_row(f"doc_{i}", f"text {i}") for i in range(4)
        ]
        self.bq_client.run_query_async.return_value = query_job
        self.fs.upload_from_string.side_effect = _raise_for_text_uploads

        with self.assertRaisesRegex(
            RuntimeError,
            r"Document upload completed with 1 error\(s\):\n"
            r"\[US_XX\] Collection \[test_collection\]: 4 documents failed to upload:\n"
            r"(.|\n)*\.\.\. and 2 more",
        ):
            self.uploader.run([self._make_upload_batch(batch_number=0)])

        # All statuses are still recorded in the CSV, regardless of the
        # reporting cap.
        csv_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == CSV_CONTENT_TYPE
        ]
        self.assertEqual(len(csv_calls), 1)
        reader = csv.DictReader(
            io.StringIO(csv_calls[0].kwargs["contents"]),
            fieldnames=DocumentUploadStatusTable.column_names(),
        )
        statuses = {row[DOCUMENT_CONTENTS_ID]: row[STATUS] for row in reader}
        self.assertEqual(
            statuses,
            {f"doc_{i}": DOCUMENT_UPLOAD_FAILURE for i in range(4)},
        )

    def test_batch_failing_before_first_flush_writes_no_status_csvs(self) -> None:
        """A batch whose document row stream raises before the first status CSV
        flush writes no status CSVs at all — any statuses already collected for
        the batch are discarded rather than flushed as a final part.

        This pins the case where a collection can end up with zero status CSVs
        even though document uploads may have started. The record step still
        runs after upload task failures (the DAG sequences it behind an
        ALL_DONE trigger rule to capture partial results), and its `*.csv`
        wildcard load of the collection's output directory has zero files to
        match in this case.
        """

        def rows_then_stream_failure() -> Iterator[bigquery.table.Row]:
            yield _make_new_document_contents_row("doc_a", "text a")
            yield _make_new_document_contents_row("doc_b", "text b")
            raise RuntimeError("BQ stream failed mid-batch")

        query_job = MagicMock()
        query_job.result.return_value = rows_then_stream_failure()
        self.bq_client.run_query_async.return_value = query_job

        with self.assertRaisesRegex(
            RuntimeError,
            r"Document upload completed with 1 error\(s\):\n"
            r"\[US_XX\] Collection \[test_collection\]: BQ stream failed mid-batch",
        ):
            self.uploader.run([self._make_upload_batch(batch_number=0)])

        csv_calls = [
            c
            for c in self.fs.upload_from_string.call_args_list
            if c.kwargs.get("content_type") == CSV_CONTENT_TYPE
        ]
        self.assertEqual(csv_calls, [])

    def test_documents_streamed_not_materialized(self) -> None:
        """The uploader pulls document rows from the BQ result iterator lazily
        as upload slots free up, never draining it up front."""
        pulled = 0

        def row_generator() -> Iterator[bigquery.table.Row]:
            nonlocal pulled
            for i in range(500):
                pulled += 1
                yield _make_new_document_contents_row(f"doc_{i}", f"text {i}")

        query_job = MagicMock()
        query_job.result.return_value = row_generator()
        self.bq_client.run_query_async.return_value = query_job

        pulled_at_first_upload: int | None = None

        def upload_side_effect(
            path: GcsfsFilePath,  # pylint: disable=unused-argument
            contents: str,  # pylint: disable=unused-argument
            content_type: str,  # pylint: disable=unused-argument
        ) -> None:
            nonlocal pulled_at_first_upload
            if pulled_at_first_upload is None:
                pulled_at_first_upload = pulled

        self.fs.upload_from_string.side_effect = upload_side_effect

        self.uploader.run([self._make_upload_batch(batch_number=0)])

        self.assertEqual(pulled, 500)
        # The first upload must have started while most of the 500 rows were
        # still unread (only up to the initial in-flight window is pulled
        # before work begins); a materializing implementation would read all
        # 500 rows before any upload runs.
        assert pulled_at_first_upload is not None
        self.assertLessEqual(pulled_at_first_upload, MAX_UPLOAD_WORKERS + 1)
