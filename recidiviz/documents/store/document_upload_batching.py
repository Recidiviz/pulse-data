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
"""Builds DocumentUploadBatches from a collection's discovery result and
distributes them across upload task instances."""

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_types import (
    DocumentUploadBatch,
    SingleCollectionDocumentDiscoveryResult,
)


def build_document_batches(
    collection_result: SingleCollectionDocumentDiscoveryResult,
    num_upload_task_instances: int,
    big_query_client: BigQueryClient,
) -> list[list[DocumentUploadBatch]]:
    """Reads the distinct batch numbers from the collection's temp new document
    contents table and distributes them round-robin across
    |num_upload_task_instances| task instances."""
    batches: list[list[DocumentUploadBatch]] = [
        [] for _ in range(num_upload_task_instances)
    ]
    if collection_result.num_new_document_contents_rows == 0:
        return batches

    batch_numbers_query = (
        f"SELECT DISTINCT {DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME} "
        f"FROM {collection_result.temp_new_document_contents_address.format_address_for_query()}"
    )
    batch_numbers_result = big_query_client.run_query_async(
        query_str=batch_numbers_query,
        use_query_cache=False,
    )
    upload_batches = [
        DocumentUploadBatch(
            collection_name=collection_result.collection_name,
            temp_new_document_contents_table_address=collection_result.temp_new_document_contents_address,
            batch_number=row[DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME],
        )
        for row in batch_numbers_result.result()
    ]
    for i, upload_batch in enumerate(upload_batches):
        batches[i % num_upload_task_instances].append(upload_batch)
    return batches
