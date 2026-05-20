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
"""Loads document upload results to BQ and inserts successfully uploaded
documents into their collection metadata tables."""
import logging
from concurrent import futures
from datetime import datetime

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE, BigQueryClient
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_metadata_updates_query_builder import (
    DocumentMetadataUpdatesQueryBuilder,
)
from recidiviz.documents.store.document_store_gcs_path_utils import (
    gcs_directory_for_task_output,
)
from recidiviz.documents.store.document_store_types import (
    SingleCollectionDocumentDiscoveryResult,
)
from recidiviz.documents.store.document_upload_status_table import (
    DocumentUploadStatusTable,
)


@attr.define
class DocumentUploadResultRecorder:
    """Writes all new document collection metadata rows for successfully uploaded documents to the collection
    metadata tables, and records upload status for all new documents in the document upload status table.
    """

    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)
    project_id: str = attr.ib(validator=attr_validators.is_str)
    big_query_client: BigQueryClient = attr.ib()
    # Unique identifier for the DAG run.
    run_id: str = attr.ib(validator=attr_validators.is_str)
    # The datetime to use for the row_create_datetime column value when inserting new metadata rows.
    metadata_row_create_datetime: datetime = attr.ib(
        validator=attr_validators.is_datetime
    )

    def _log_prefix(self, collection_name: str) -> str:
        return f"[{self.state_code.value}] Collection [{collection_name}]:"

    def run(
        self,
        collection_discovery_results: list[SingleCollectionDocumentDiscoveryResult],
    ) -> None:
        """Loads document upload status CSVs from GCS into the BQ upload status table, then inserts
        rows for successfully uploaded documents into each collection's metadata table.
        """
        if not collection_discovery_results:
            raise ValueError("Expected at least one collection discovery result")

        has_new_document_contents = any(
            r.num_new_document_contents_rows > 0 for r in collection_discovery_results
        )
        if has_new_document_contents:
            self._load_upload_status_to_bq()
        else:
            logging.info(
                "No collections have new document contents; "
                "skipping upload status CSV load."
            )

        errors: list[tuple[str, Exception]] = []
        with futures.ThreadPoolExecutor(
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
        ) as executor:
            record_futures = {
                executor.submit(
                    self._record_for_collection,
                    collection_result=collection_result,
                ): collection_result.collection_name
                for collection_result in collection_discovery_results
            }
            for future in futures.as_completed(record_futures):
                collection_name = record_futures[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(
                        "%s failed to record results: %s",
                        self._log_prefix(collection_name),
                        e,
                    )
                    errors.append((collection_name, e))

        if errors:
            raise ExceptionGroup(
                f"Failed to record results for collections: {[name for name, _ in errors]}",
                [e for _, e in errors],
            )

    def _load_upload_status_to_bq(self) -> None:
        """Loads all upload status CSVs for this run from GCS into the document_upload_status BQ table."""
        output_dir = gcs_directory_for_task_output(
            self.project_id, self.state_code, self.run_id
        )
        source_uri = f"{output_dir.uri()}/*.csv"
        upload_status_table_address = DocumentUploadStatusTable.get_table_address(
            project_id=self.project_id, state_code=self.state_code
        )

        logging.info(
            "Loading upload status CSVs from [%s] into [%s]",
            source_uri,
            upload_status_table_address.to_str(),
        )
        load_job = self.big_query_client.load_table_from_cloud_storage(
            source_uris=[source_uri],
            destination_address=upload_status_table_address.to_project_agnostic_address(),
            destination_table_schema=DocumentUploadStatusTable.schema(),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job.result()

    def _record_for_collection(
        self,
        collection_result: SingleCollectionDocumentDiscoveryResult,
    ) -> None:
        """Inserts rows from the temp metadata updates table into the collection metadata table for
        documents that were successfully uploaded, then cleans up temp tables if all uploads succeeded.
        We do not clean up temp tables if some uploads failed to preserve information for debugging.
        """
        metadata_addr = collection_result.config.metadata_table_address(self.project_id)
        query = DocumentMetadataUpdatesQueryBuilder(
            project_id=self.project_id, state_code=self.state_code
        ).build_successful_uploads_metadata_insert_query(
            config=collection_result.config,
            metadata_table_address=metadata_addr,
            temp_document_metadata_updates_address=collection_result.temp_document_metadata_updates_address,
            row_create_datetime=self.metadata_row_create_datetime,
        )

        logging.info(
            "%s inserting results into [%s]",
            self._log_prefix(collection_result.collection_name),
            metadata_addr.to_str(),
        )
        query_job = self.big_query_client.run_query_async(
            query_str=query,
            use_query_cache=False,
        )
        result = query_job.result()

        rows_inserted = result.num_dml_affected_rows
        expected_rows = collection_result.num_document_metadata_updates_rows
        if rows_inserted == expected_rows:
            logging.info(
                "%s successfully inserted all %d metadata rows; "
                "deleting temp tables.",
                self._log_prefix(collection_result.collection_name),
                rows_inserted,
            )
            self._delete_temp_tables(collection_result)
        else:
            logging.warning(
                "%s inserted %s of %d metadata rows; "
                "temp tables will not be deleted.",
                self._log_prefix(collection_result.collection_name),
                rows_inserted,
                expected_rows,
            )

    def _delete_temp_tables(
        self,
        collection_result: SingleCollectionDocumentDiscoveryResult,
    ) -> None:
        self.big_query_client.delete_table(
            collection_result.temp_document_metadata_updates_address.to_project_agnostic_address(),
            not_found_ok=True,
        )
        self.big_query_client.delete_table(
            collection_result.temp_new_document_contents_address.to_project_agnostic_address(),
            not_found_ok=True,
        )
