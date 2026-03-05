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
"""Uploads documents to GCS from a BigQuery table."""

import logging

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_ID_COLUMN_NAME,
    DocumentCollectionConfig,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_store_utils import (
    DocumentId,
    gcs_path_for_document,
)


@attr.define
class DocumentUploadResult:
    """Result of uploading a batch of documents."""

    collection_config: DocumentCollectionConfig
    successes: dict[DocumentId, GcsfsFilePath]
    failures: dict[DocumentId, Exception]


@attr.define
class DocumentUploader:
    """Uploads documents to GCS from a BigQuery table containing document data."""

    gcs_fs: GCSFileSystem
    big_query_client: BigQueryClient
    project_id: str
    sandbox_bucket: str | None = None

    def upload_document_batch(
        self,
        document_collection_config: DocumentCollectionConfig,
        new_documents_address: BigQueryAddress,
        start_index: int,
        batch_size: int,
    ) -> DocumentUploadResult:
        """Uploads a batch of documents from a BQ table to GCS.

        Args:
            document_collection_config: Configuration for the document collection.
            new_documents_address: Address of the BQ table containing documents.
                Expected columns: document_id, document_text, sequence_num.
            start_index: The sequence_num to start from (1-indexed).
            batch_size: Maximum number of documents to upload.

        Returns:
            DocumentUploadResult containing successful and failed uploads.
        """
        if document_collection_config.state_code is None:
            raise ValueError(
                "Cannot upload documents for state-agnostic collection. "
                f"Collection: {document_collection_config.collection_name}"
            )

        state_code = document_collection_config.state_code

        # Query for the batch of documents
        query = f"""
        SELECT
            {DOCUMENT_ID_COLUMN_NAME},
            document_text
        FROM `{self.project_id}.{new_documents_address.dataset_id}.{new_documents_address.table_id}`
        WHERE sequence_num >= {start_index} AND sequence_num < {start_index + batch_size}
        ORDER BY sequence_num
        """

        query_job = self.big_query_client.run_query_async(
            query_str=query,
            use_query_cache=True,
        )
        results = query_job.result()

        successes: dict[DocumentId, GcsfsFilePath] = {}
        failures: dict[DocumentId, Exception] = {}

        for row in results:
            document_id = row[DOCUMENT_ID_COLUMN_NAME]
            document_text = row["document_text"]

            gcs_path = gcs_path_for_document(
                project_id=self.project_id,
                state_code=state_code,
                document_id=document_id,
                sandbox_bucket=self.sandbox_bucket,
            )

            try:
                self.gcs_fs.upload_from_string(
                    path=gcs_path,
                    contents=document_text,
                    content_type="text/plain",
                )
                successes[document_id] = gcs_path
                logging.info("Uploaded document %s to %s", document_id, gcs_path.uri())
            except Exception as e:
                failures[document_id] = e
                logging.error("Failed to upload document %s: %s", document_id, str(e))

        return DocumentUploadResult(
            collection_config=document_collection_config,
            successes=successes,
            failures=failures,
        )

    def upload_all_documents(
        self,
        document_collection_config: DocumentCollectionConfig,
        new_documents_address: BigQueryAddress,
        batch_size: int = 100,
    ) -> DocumentUploadResult:
        """Uploads all documents from a BQ table to GCS in batches.

        Args:
            document_collection_config: Configuration for the document collection.
            new_documents_address: Address of the BQ table containing documents.
            batch_size: Number of documents to upload per batch.

        Returns:
            Combined DocumentUploadResult for all batches.
        """
        # Get total count of documents
        count_query = f"""
        SELECT COUNT(*) as total
        FROM `{self.project_id}.{new_documents_address.dataset_id}.{new_documents_address.table_id}`
        """
        count_result = self.big_query_client.run_query_async(
            query_str=count_query,
            use_query_cache=True,
        ).result()

        total_docs = list(count_result)[0]["total"]
        logging.info(
            "Starting upload of %d documents for collection %s",
            total_docs,
            document_collection_config.collection_name,
        )

        all_successes: dict[DocumentId, GcsfsFilePath] = {}
        all_failures: dict[DocumentId, Exception] = {}

        # sequence_num is 1-indexed (from SQL ROW_NUMBER())
        start_index = 1
        while start_index <= total_docs:
            batch_result = self.upload_document_batch(
                document_collection_config=document_collection_config,
                new_documents_address=new_documents_address,
                start_index=start_index,
                batch_size=batch_size,
            )
            all_successes.update(batch_result.successes)
            all_failures.update(batch_result.failures)

            logging.info(
                "Batch complete: %d/%d documents uploaded (%d successes, %d failures in batch)",
                start_index + batch_size - 1
                if start_index + batch_size - 1 < total_docs
                else total_docs,
                total_docs,
                len(batch_result.successes),
                len(batch_result.failures),
            )
            start_index += batch_size

        return DocumentUploadResult(
            collection_config=document_collection_config,
            successes=all_successes,
            failures=all_failures,
        )
