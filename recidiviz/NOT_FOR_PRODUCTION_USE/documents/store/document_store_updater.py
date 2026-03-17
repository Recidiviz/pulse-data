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
"""Orchestrates the document store update process."""

import logging
from datetime import datetime, timezone

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_collection_config import (
    DOCUMENT_ID_COLUMN_NAME,
    UPLOAD_DATETIME_COLUMN_NAME,
    DocumentCollectionConfig,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.document_uploader import (
    DocumentUploader,
    DocumentUploadResult,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.store.new_document_identifier import (
    NewDocumentIdentifier,
)


@attr.define
class DocumentStoreUpdater:
    """Orchestrates the document store update process.

    Given a collection config, this class:
    1. Identifies documents that haven't been uploaded yet
    2. Uploads them to GCS
    3. Records successful uploads to the metadata table
    """

    big_query_client: BigQueryClient
    gcs_fs: GCSFileSystem
    project_id: str
    temp_dataset_id: str
    metadata_dataset_override: str | None = None
    sandbox_bucket: str | None = None

    def update_document_collection(
        self,
        collection_config: DocumentCollectionConfig,
        sample_size: int | None = None,
        sample_entity_count: int | None = None,
        active_in_compartment: str | None = None,
        batch_size: int = 100,
        lookback_days: int | None = None,
        person_ids: list[int] | None = None,
    ) -> DocumentUploadResult:
        """Updates a document collection by uploading new documents and recording metadata.

        Args:
            collection_config: Configuration for the document collection.
            sample_size: If provided, limits the number of new documents to process.
            sample_entity_count: If provided, samples N distinct entities (e.g., people)
                and returns ALL documents for those entities. Cannot be used with sample_size
                or person_ids.
            active_in_compartment: If provided, restricts entity sampling to people
                with an active session in the given compartment (e.g., SUPERVISION).
                Cannot be used together with person_ids.
            batch_size: Number of documents to upload per batch.
            lookback_days: If provided, only includes documents whose
                document_update_datetime is within the last N days.
            person_ids: If provided, restricts documents to those belonging to the
                given person IDs. Cannot be used together with active_in_compartment
                or sample_entity_count.

        Returns:
            DocumentUploadResult containing successful and failed uploads.
        """
        # Step 1: Find new documents
        logging.info(
            "Identifying new documents for collection %s",
            collection_config.collection_name,
        )
        new_doc_identifier = NewDocumentIdentifier(
            big_query_client=self.big_query_client,
            project_id=self.project_id,
            temp_dataset_id=self.temp_dataset_id,
            metadata_dataset_override=self.metadata_dataset_override,
        )
        new_documents_address = new_doc_identifier.find_new_documents_in_collection(
            collection_config=collection_config,
            sample_size=sample_size,
            sample_entity_count=sample_entity_count,
            active_in_compartment=active_in_compartment,
            lookback_days=lookback_days,
            person_ids=person_ids,
        )
        logging.info(
            "New documents table: %s.%s",
            new_documents_address.dataset_id,
            new_documents_address.table_id,
        )

        # Step 2: Upload documents to GCS
        logging.info("Uploading documents to GCS")
        uploader = DocumentUploader(
            gcs_fs=self.gcs_fs,
            big_query_client=self.big_query_client,
            project_id=self.project_id,
            sandbox_bucket=self.sandbox_bucket,
        )
        upload_result = uploader.upload_all_documents(
            document_collection_config=collection_config,
            new_documents_address=new_documents_address,
            batch_size=batch_size,
        )

        # Step 3: Record successful uploads to metadata table
        if upload_result.successes:
            logging.info(
                "Recording %d successful uploads to metadata table",
                len(upload_result.successes),
            )
            self._record_uploads_to_metadata(
                collection_config=collection_config,
                new_documents_address=new_documents_address,
                successful_document_ids=list(upload_result.successes.keys()),
            )

        return upload_result

    def _record_uploads_to_metadata(
        self,
        collection_config: DocumentCollectionConfig,
        new_documents_address: BigQueryAddress,
        successful_document_ids: list[str],
    ) -> None:
        """Records successful uploads to the collection's metadata table.

        Assumes the metadata table already exists (should be created by the caller).
        """
        metadata_address = collection_config.metadata_table_address(
            dataset_override=self.metadata_dataset_override
        )

        # Get column names from metadata schema, excluding upload_datetime which we add
        all_columns = [
            field.name
            for field in collection_config.metadata_table_schema()
            if field.name != UPLOAD_DATETIME_COLUMN_NAME
        ]
        columns_str = ", ".join(all_columns)

        upload_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Format document IDs as SQL string literals for the IN clause
        # Document IDs are SHA256 hex strings, so safe to embed directly
        ids_sql = ", ".join(f"'{doc_id}'" for doc_id in successful_document_ids)

        # Insert successful uploads into metadata table
        insert_query = f"""
        INSERT INTO `{self.project_id}.{metadata_address.dataset_id}.{metadata_address.table_id}`
            ({columns_str}, {UPLOAD_DATETIME_COLUMN_NAME})
        SELECT
            {columns_str},
            TIMESTAMP('{upload_time}') AS {UPLOAD_DATETIME_COLUMN_NAME}
        FROM `{self.project_id}.{new_documents_address.dataset_id}.{new_documents_address.table_id}`
        WHERE {DOCUMENT_ID_COLUMN_NAME} IN ({ids_sql})
        """

        self.big_query_client.run_query_async(
            query_str=insert_query,
            use_query_cache=False,
        ).result()
