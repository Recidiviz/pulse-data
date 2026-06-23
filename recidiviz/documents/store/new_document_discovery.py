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
to temporary BQ tables for downstream processing."""

import logging
from functools import cached_property

import attr

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    get_document_collection_config,
)
from recidiviz.documents.store.document_collection_query_builder import (
    DocumentCollectionDiffQueryBuilder,
)
from recidiviz.documents.store.document_metadata_updates_query_builder import (
    DocumentMetadataUpdatesQueryBuilder,
)
from recidiviz.documents.store.document_store_types import (
    SingleCollectionDocumentDiscoveryResult,
)

# TODO(#73430) Put some thought behind what the batch size should be
# and align kubernetes pod resource requests/limits
DEFAULT_TARGET_UPLOAD_BATCH_BYTES = 1_000_000_000  # 1 GB


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
    diff_query_builder: DocumentCollectionDiffQueryBuilder = attr.ib(
        init=False,
        validator=attr.validators.instance_of(DocumentCollectionDiffQueryBuilder),
    )

    def __attrs_post_init__(self) -> None:
        self.diff_query_builder = DocumentCollectionDiffQueryBuilder(
            project_id=self.project_id
        )

    @cached_property
    def config(self) -> DocumentCollectionConfig:
        return get_document_collection_config(self.state_code, self.collection_name)

    def run(self) -> SingleCollectionDocumentDiscoveryResult | None:
        """Discovers new documents for the collection by diffing the
        collection's document_generation_query results against the current
        metadata table state, then writes results to temporary BQ tables for
        downstream processing.

        Returns the discovery result, or None if the collection has no new
        metadata rows.

        Steps:
          1. Runs the collection's document_generation_query.
          2. Diffs the results against the collection's "latest" metadata view
             (which returns the most up-to-date row per document primary key) to
             identify added, updated, and deleted documents. Writes the
             diff results to a temp document metadata updates table.
          3. From the temp document metadata updates table, selects distinct
             (document_contents_id, document_text) pairs whose
             document_contents_id is not already present in the collection's
             persistent document_contents table. Each row is assigned a batch
             number based on cumulative byte size and written to a temp new
             document contents table.
        """
        temp_metadata_address = (
            self.config.temp_document_metadata_updates_table_address(
                self.project_id, self.run_id
            )
        )
        diff_query = self.diff_query_builder.build_document_diff_query(self.config)

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
            return None

        temp_document_address = self.config.temp_new_document_contents_table_address(
            self.project_id, self.run_id
        )
        new_documents_query = DocumentMetadataUpdatesQueryBuilder(
            project_id=self.project_id, state_code=self.state_code
        ).build_new_documents_query(
            temp_document_metadata_updates_address=temp_metadata_address,
            document_contents_table_address=self.config.document_contents_table_address(
                self.project_id
            ),
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

        return SingleCollectionDocumentDiscoveryResult(
            state_code=self.state_code,
            collection_name=self.config.name,
            temp_document_metadata_updates_address=temp_metadata_address,
            temp_new_document_contents_address=temp_document_address,
            num_new_document_contents_rows=row_iterator.total_rows,
            num_document_metadata_updates_rows=metadata_row_iterator.total_rows,
        )
