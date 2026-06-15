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
"""Loads document upload results for a single collection to BQ and inserts
successfully uploaded documents into the collection metadata table."""
import logging
from datetime import datetime

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
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
    """Loads upload status CSVs for a single collection into the
    document_upload_status table, inserts metadata rows for the collection's
    successfully uploaded documents into the collection metadata table, and
    cleans up the collection's temp tables when all uploads succeeded.
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
        collection_discovery_result: SingleCollectionDocumentDiscoveryResult,
    ) -> None:
        """Loads this collection's upload status CSVs from GCS into the BQ
        upload status table, then inserts rows for successfully uploaded
        documents into the collection metadata table."""
        if collection_discovery_result.num_new_document_contents_rows > 0:
            self._load_upload_status_to_bq(collection_discovery_result.collection_name)
        else:
            logging.info(
                "%s no new document contents; skipping upload status CSV load.",
                self._log_prefix(collection_discovery_result.collection_name),
            )

        self._record_for_collection(collection_discovery_result)

    def _load_upload_status_to_bq(self, collection_name: str) -> None:
        """Loads this collection's upload status CSVs for this run from GCS
        into the document_upload_status BQ table."""
        output_dir = gcs_directory_for_task_output(
            self.project_id, self.state_code, self.run_id, collection_name
        )
        source_uri = f"{output_dir.uri()}/*.csv"
        upload_status_table_address = DocumentUploadStatusTable.get_table_address(
            project_id=self.project_id, state_code=self.state_code
        )

        logging.info(
            "%s loading upload status CSVs from [%s] into [%s]",
            self._log_prefix(collection_name),
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
