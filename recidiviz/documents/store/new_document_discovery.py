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
"""Discovers new documents across all collections for a state and writes them
to temporary BQ tables, returning upload batches for downstream processing."""

import logging
from concurrent import futures

import attr

from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE, BigQueryClient
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    collect_document_collection_configs,
)
from recidiviz.documents.store.document_collection_query_builder import (
    DocumentCollectionDiffQueryBuilder,
)
from recidiviz.documents.store.document_metadata_updates_query_builder import (
    DocumentMetadataUpdatesQueryBuilder,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_types import (
    DocumentUploadBatch,
    SingleCollectionDocumentDiscoveryResult,
)
from recidiviz.utils.string import StrictStringFormatter

# TODO(#75610) move this constant to DAG definition file once implemented
UPLOAD_TASK_INSTANCE_COUNT = 10
# TODO(#73430) Put some thought behind what the batch size should be
DEFAULT_TARGET_UPLOAD_BATCH_BYTES = 1_000_000_000  # 1 GB


@attr.define(frozen=True)
class DocumentDiscoveryResult:
    """Result of running document discovery for a state.

    Attributes:
        document_batches: Nested list of DocumentUploadBatches to process. Includes collections with new document contents only.
        collection_results: Discovery results for each collection. Includes collections with new document contents and
            collections with new metadata rows but no new documents.
    """

    document_batches: list[list[DocumentUploadBatch]]
    collection_results: list[SingleCollectionDocumentDiscoveryResult]


def build_document_batches(
    collection_results: list[SingleCollectionDocumentDiscoveryResult],
    num_upload_task_instances: int,
    big_query_client: BigQueryClient,
) -> list[list[DocumentUploadBatch]]:
    """Creates DocumentUploadBatches for each collection, then distributes all
    ranges round-robin across |num_upload_task_instances| task instances."""
    batch_numbers_query_template = f"""
SELECT DISTINCT {DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME}
FROM {{temp_new_document_contents_table_address}}"""

    all_upload_batches: list[DocumentUploadBatch] = []
    for result in collection_results:
        if result.num_new_document_contents_rows == 0:
            continue

        batch_numbers_result = big_query_client.run_query_async(
            query_str=StrictStringFormatter().format(
                batch_numbers_query_template,
                temp_new_document_contents_table_address=result.temp_new_document_contents_address.format_address_for_query(),
            ),
            use_query_cache=False,
        )
        all_upload_batches.extend(
            DocumentUploadBatch(
                collection_name=result.config.name,
                temp_new_document_contents_table_address=result.temp_new_document_contents_address,
                batch_number=row[DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME],
            )
            for row in batch_numbers_result.result()
        )

    batches: list[list[DocumentUploadBatch]] = [
        [] for _ in range(num_upload_task_instances)
    ]
    for i, upload_batch in enumerate(all_upload_batches):
        batches[i % num_upload_task_instances].append(upload_batch)

    return batches


@attr.define
class NewDocumentDiscoverer:
    """Discovers new documents across all collections for a state and writes
    them to temporary BQ tables for downstream processing."""

    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)
    project_id: str = attr.ib(validator=attr_validators.is_str)
    big_query_client: BigQueryClient = attr.ib()
    job_id: str = attr.ib(validator=attr_validators.is_str)
    upload_task_instance_count: int = attr.ib(
        validator=attr_validators.is_positive_int, default=UPLOAD_TASK_INSTANCE_COUNT
    )
    target_upload_batch_bytes: int = attr.ib(
        validator=attr_validators.is_positive_int,
        default=DEFAULT_TARGET_UPLOAD_BATCH_BYTES,
    )
    diff_query_builder: DocumentCollectionDiffQueryBuilder = attr.ib(
        init=False,
        validator=attr.validators.instance_of(DocumentCollectionDiffQueryBuilder),
    )

    def __attrs_post_init__(self) -> None:
        self.diff_query_builder = DocumentCollectionDiffQueryBuilder(
            project_id=self.project_id
        )

    def run(self) -> DocumentDiscoveryResult:
        """Discovers new documents across all collections for a state by diffing
        each collection's document_generation_query results against the current
        metadata table state, then writes results to temporary BQ tables for
        downstream processing.

        For each document collection config:
          1. Runs the collection's document_generation_query.
          2. Diffs the results against the collection's "latest" metadata view
             (which returns the most up-to-date row per document primary key) to
             identify added, updated, and deleted documents. Writes the
             diff results to a temp document metadata updates table.
          3. From the temp document metadata updates table, selects distinct
             (document_contents_id, document_text) pairs whose
             document_contents_id does not already have a SUCCESS entry in the
             document_upload_status table. Each row is assigned a batch number
             based on cumulative byte size and written to a temp new document
             contents table.

        The temp new document contents tables' batch numbers are then used to
        create DocumentUploadBatches that are distributed round-robin across
        upload task instances.

        Returns a DocumentDiscoveryResult containing the upload batches (used for
        document upload to gcs) and the temp document metadata updates table
        addresses (used to update the collection metadata tables).
        """
        configs = collect_document_collection_configs(self.state_code)

        collection_results: list[SingleCollectionDocumentDiscoveryResult] = []

        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2),
        ) as executor:
            document_discovery_futures = [
                executor.submit(
                    self._discover_for_collection,
                    config=config,
                )
                for config in configs.values()
            ]
            for future in futures.as_completed(document_discovery_futures):
                collection_results.append(future.result())

        collections_with_new_metadata_rows = [
            result
            for result in collection_results
            if result.num_document_metadata_updates_rows > 0
        ]

        return DocumentDiscoveryResult(
            document_batches=build_document_batches(
                collections_with_new_metadata_rows,
                self.upload_task_instance_count,
                self.big_query_client,
            ),
            collection_results=collections_with_new_metadata_rows,
        )

    def _discover_for_collection(
        self,
        config: DocumentCollectionConfig,
    ) -> SingleCollectionDocumentDiscoveryResult:
        """Runs document discovery for a single collection. Writes two temp
        tables and returns a SingleCollectionDocumentDiscoveryResult."""
        temp_metadata_address = config.temp_document_metadata_updates_table_address(
            self.project_id, self.job_id
        )
        diff_query = self.diff_query_builder.build_document_diff_query(config)

        logging.info(
            "Writing diff results for collection [%s] to [%s]",
            config.name,
            temp_metadata_address.to_str(),
        )
        metadata_row_iterator = self.big_query_client.create_table_from_query(
            address=temp_metadata_address.to_project_agnostic_address(),
            query=diff_query,
            use_query_cache=False,
            overwrite=True,
        )

        temp_document_address = config.temp_new_document_contents_table_address(
            self.project_id, self.job_id
        )
        new_documents_query = DocumentMetadataUpdatesQueryBuilder(
            project_id=self.project_id, state_code=config.state_code
        ).build_new_documents_query(
            temp_document_metadata_updates_address=temp_metadata_address,
            target_batch_bytes=self.target_upload_batch_bytes,
        )

        logging.info(
            "Writing new documents for collection [%s] to [%s]",
            config.name,
            temp_document_address.to_str(),
        )
        row_iterator = self.big_query_client.create_table_from_query(
            address=temp_document_address.to_project_agnostic_address(),
            query=new_documents_query,
            use_query_cache=False,
            overwrite=True,
        )

        return SingleCollectionDocumentDiscoveryResult(
            config=config,
            temp_document_metadata_updates_address=temp_metadata_address,
            temp_new_document_contents_address=temp_document_address,
            num_new_document_contents_rows=row_iterator.total_rows,
            num_document_metadata_updates_rows=metadata_row_iterator.total_rows,
        )
