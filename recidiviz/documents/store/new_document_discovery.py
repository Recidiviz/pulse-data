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

+TODO(OBT-42680) During a sandbox run for a first order document collection, the sandbox
+will fail for a brand new collection because the metadata table will not exist yet.
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
from recidiviz.documents.store.document_collection_query_builders import (
    DocumentCollectionDiffQueryBuilder,
    DocumentCollectionGenerationQueryBuilder,
)
from recidiviz.documents.store.document_metadata_updates_query_builder import (
    DocumentMetadataUpdatesQueryBuilder,
)
from recidiviz.documents.store.document_store_types import (
    SingleCollectionDocumentDiscoveryResult,
)

# The upload task holds every document in a batch in memory at once, with a
# fixed per-document overhead (~2.5KB of row object and upload-future
# machinery) that dwarfs the text itself for small documents — so peak memory
# scales with document count more than with batch bytes. 50MB keeps the worst
# observed profile (~275B/doc, i.e. ~180K docs per batch, ~500MB peak)
# comfortably under the 2Gi limit on DocumentUploadEntrypoint pods (see
# recidiviz_kubernetes_resources.yaml).
# TODO(OBT-41816): Once GcsDocumentUploader bounds its in-flight upload memory,
# revisit this value — batch size can then be raised for throughput without
# OOM risk.
DEFAULT_TARGET_UPLOAD_BATCH_BYTES = 50_000_000  # 50 MB


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
    generation_query_builder: DocumentCollectionGenerationQueryBuilder = attr.ib(
        init=False,
        validator=attr.validators.instance_of(DocumentCollectionGenerationQueryBuilder),
    )
    diff_query_builder: DocumentCollectionDiffQueryBuilder = attr.ib(
        init=False,
        validator=attr.validators.instance_of(DocumentCollectionDiffQueryBuilder),
    )

    def __attrs_post_init__(self) -> None:
        self.generation_query_builder = DocumentCollectionGenerationQueryBuilder(
            project_id=self.project_id
        )
        self.diff_query_builder = DocumentCollectionDiffQueryBuilder(
            project_id=self.project_id
        )

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
        generation_query = (
            self.generation_query_builder.build_document_generation_query(self.config)
        )

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

        if isinstance(self.config, EntityResolutionDocumentCollectionConfig):
            EntityResolutionEntrySourceMapHydrator(
                project_id=self.project_id,
                big_query_client=self.big_query_client,
                config=self.config,
            ).run(document_generation_output_address)

        temp_metadata_address = (
            self.config.temp_document_metadata_updates_table_address(
                self.run_id
            ).to_project_specific_address(self.project_id)
        )
        diff_query = self.diff_query_builder.build_document_diff_query(
            config=self.config,
            document_generation_output_address=document_generation_output_address,
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
            # TODO(OBT-42680) Thread sandbox prefix through
            document_contents_table_address=self.config.document_contents_table_address(
                sandbox_dataset_prefix=None
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
