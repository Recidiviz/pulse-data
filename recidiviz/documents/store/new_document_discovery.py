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
"""Discovers new documents for a single document collection and writes them
to temporary BQ tables for downstream processing.
"""

import logging
from functools import cached_property

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_hydrator import (
    EntityResolutionEntrySourceMapHydrator,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_collection_diff_query_builder import (
    DocumentCollectionDiffQueryBuilder,
)
from recidiviz.documents.store.document_generation_query_builder import (
    DocumentGenerationQueryBuilder,
    build_document_generation_query_builder,
)
from recidiviz.documents.store.document_metadata_updates_query_builder import (
    DocumentMetadataUpdatesQueryBuilder,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)
from recidiviz.documents.store.document_store_types import (
    SingleCollectionDocumentDiscoveryResult,
)

# GcsDocumentUploader streams each batch (bounded upload concurrency, status
# CSVs flushed in parts), so its peak memory does not scale with batch size.
# Batch size instead trades per-batch overhead (each batch runs its own query
# against the temp new-document-contents table) against load balancing: batches
# are round-robined across UPLOAD_TASK_INSTANCE_COUNT upload pods, so a run
# needs at least that many batches to keep every pod busy. 500MB keeps
# multi-GB backfill runs (e.g. the ~3.5GB US_CO backfill) spread across all
# pods while keeping the batch count, and thus repeated temp-table scans, low.
DEFAULT_TARGET_UPLOAD_BATCH_BYTES = 500_000_000  # 500 MB


@attr.define
class NewDocumentDiscoverer:
    """Discovers new documents for a single document collection and writes
    them to temporary BQ tables for downstream processing."""

    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)
    collection_name: str = attr.ib(validator=attr_validators.is_str)
    project_id: str = attr.ib(validator=attr_validators.is_str)
    big_query_client: BigQueryClient = attr.ib()
    run_id: str = attr.ib(validator=attr_validators.is_str)
    target_upload_batch_bytes: int = attr.ib(
        validator=attr_validators.is_positive_int,
        default=DEFAULT_TARGET_UPLOAD_BATCH_BYTES,
    )
    generation_query_builder: DocumentGenerationQueryBuilder = attr.ib(
        init=False,
        validator=attr.validators.instance_of(DocumentGenerationQueryBuilder),
    )
    sandbox_context: DocumentStoreSandboxContext | None = attr.ib(
        default=None,
        validator=attr_validators.is_opt(DocumentStoreSandboxContext),
    )

    diff_query_builder: DocumentCollectionDiffQueryBuilder = attr.ib(
        init=False,
        validator=attr.validators.instance_of(DocumentCollectionDiffQueryBuilder),
    )

    def __attrs_post_init__(self) -> None:
        self.generation_query_builder = build_document_generation_query_builder(
            config=self.config,
            project_id=self.project_id,
            sandbox_context=self.sandbox_context,
        )
        self.diff_query_builder = DocumentCollectionDiffQueryBuilder()

    @cached_property
    def config(self) -> DocumentCollectionConfig:
        return get_document_collection_config(self.state_code, self.collection_name)

    def _materialize_document_generation_output(
        self,
    ) -> ProjectSpecificBigQueryAddress:
        """Runs the collection's document_generation_query and materializes its
        full output — every document for every root entity — to a run-scoped
        temp table, returning that table's address. This single snapshot is what
        every consumer of one run's generation output reads.
        """
        document_generation_output_address = (
            self.config.temp_document_generation_output_table_address(
                self.run_id
            ).to_project_specific_address(self.project_id)
        )
        generation_query = self.generation_query_builder.build_query()

        logging.info(
            "Materializing generation output for collection [%s] to [%s]",
            self.config.name,
            document_generation_output_address.to_str(),
        )
        self.big_query_client.create_table_from_query(
            address=document_generation_output_address.to_project_agnostic_address(),
            query=generation_query,
            use_query_cache=False,
            overwrite=True,
        )
        return document_generation_output_address

    def run(self) -> SingleCollectionDocumentDiscoveryResult | None:
        """Discovers new documents for the collection by diffing the
        collection's document_generation_query results against the current
        metadata table state, then writes results to temporary BQ tables for
        downstream processing.

        Returns the discovery result, or None if the collection has no new
        metadata rows.

        Steps:
          1. Materializes the results of the collection's full document_generation_query
             to a temp table. This single snapshot is what every consumer of one run's
             generation output reads.
          2. For an entity-resolution collection, rebuilds its entry→source map
             table from that output. This runs before the diff and regardless of its
             outcome because a composite's map can change while its text, and
             therefore its document_contents_id, does not.
          3. Diffs the generation output against the collection's "latest"
             metadata view (which returns the most up-to-date row per document
             primary key) to identify added, updated, and deleted documents.
             Writes the diff results to a temp document metadata updates table.
          4. From the temp document metadata updates table, selects distinct
             (document_contents_id, document_text) pairs whose
             document_contents_id is not already present in the collection's
             persistent document_contents table. Each row is assigned a batch
             number based on cumulative byte size and written to a temp new
             document contents table.
          5. Deletes the generation-output temp table: it is consumed entirely
             within discovery. If discovery raises before deletion, the table
             deliberately survives for debugging and the temp dataset's table
             expiration reaps it.
        """

        document_generation_output_address = (
            self._materialize_document_generation_output()
        )
        # The tables this run writes for the collection (the entry->source map, and the
        # metadata/contents rows it records downstream) go under the output prefix; the
        # existing state discovery diffs against — the metadata table below and the
        # contents-existence check further down — is read from the read prefix, which is
        # the production document store (None) for a first-order collection that already
        # exists there.
        output_prefix = (
            self.sandbox_context.output_prefix_for_writing(self.config.name)
            if self.sandbox_context is not None
            else None
        )
        read_prefix = (
            self.sandbox_context.diff_read_prefix_for_document_collection(
                self.config.name
            )
            if self.sandbox_context is not None
            else None
        )

        if isinstance(self.config, EntityResolutionDocumentCollectionConfig):
            EntityResolutionEntrySourceMapHydrator(
                project_id=self.project_id,
                big_query_client=self.big_query_client,
                config=self.config,
                output_sandbox_prefix=output_prefix,
            ).run(document_generation_output_address)

        temp_metadata_address = (
            self.config.temp_document_metadata_updates_table_address(
                self.run_id
            ).to_project_specific_address(self.project_id)
        )
        metadata_table_address = self.config.metadata_table_address(
            sandbox_dataset_prefix=read_prefix
        ).to_project_specific_address(self.project_id)
        diff_query = self.diff_query_builder.build_document_diff_query(
            config=self.config,
            document_generation_output_address=document_generation_output_address,
            metadata_table_address=metadata_table_address,
        )

        logging.info(
            "Writing diff results for collection [%s] to [%s]",
            self.config.name,
            temp_metadata_address.to_str(),
        )
        metadata_row_iterator = self.big_query_client.create_table_from_query(
            address=temp_metadata_address.to_project_agnostic_address(),
            query=diff_query,
            use_query_cache=False,
            overwrite=True,
        )

        if metadata_row_iterator.total_rows == 0:
            logging.info(
                "No new metadata rows for collection [%s]; skipping new "
                "document contents discovery.",
                self.config.name,
            )
            self._delete_document_generation_output_table(
                document_generation_output_address
            )
            return None

        temp_document_address = self.config.temp_new_document_contents_table_address(
            self.run_id
        ).to_project_specific_address(self.project_id)
        new_documents_query = DocumentMetadataUpdatesQueryBuilder().build_new_documents_query(
            temp_document_metadata_updates_address=temp_metadata_address,
            document_contents_table_address=self.config.document_contents_table_address(
                sandbox_dataset_prefix=read_prefix
            ).to_project_specific_address(self.project_id),
            target_batch_bytes=self.target_upload_batch_bytes,
        )

        logging.info(
            "Writing new documents for collection [%s] to [%s]",
            self.config.name,
            temp_document_address.to_str(),
        )
        row_iterator = self.big_query_client.create_table_from_query(
            address=temp_document_address.to_project_agnostic_address(),
            query=new_documents_query,
            use_query_cache=False,
            overwrite=True,
        )

        self._delete_document_generation_output_table(
            document_generation_output_address
        )

        return SingleCollectionDocumentDiscoveryResult(
            state_code=self.state_code,
            collection_name=self.config.name,
            temp_document_metadata_updates_address=temp_metadata_address,
            temp_new_document_contents_address=temp_document_address,
            num_new_document_contents_rows=row_iterator.total_rows,
            num_document_metadata_updates_rows=metadata_row_iterator.total_rows,
        )

    def _delete_document_generation_output_table(
        self, document_generation_output_address: ProjectSpecificBigQueryAddress
    ) -> None:
        """Deletes the run's generation-output temp table, which has no
        consumers outside discovery."""
        self.big_query_client.delete_table(
            document_generation_output_address.to_project_agnostic_address(),
            not_found_ok=True,
        )
