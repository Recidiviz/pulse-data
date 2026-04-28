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
to temporary BQ tables, returning batch ranges for downstream processing."""
import logging
import math
from concurrent import futures

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
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
from recidiviz.documents.store.document_upload_status_table import (
    DocumentUploadStatusTable,
)

DEFAULT_NUM_BATCHES = 10


@attr.define(frozen=True, kw_only=True)
class DocumentBatchRange:
    """A range of documents within a temp table to process. The table at
    |temp_new_document_contents_table_address| is expected to have both document_contents_id and
    sequence_num columns."""

    collection_name: str = attr.ib(validator=attr_validators.is_str)
    temp_new_document_contents_table_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    start_sequence_num_inclusive: int = attr.ib(validator=attr_validators.is_int)
    end_sequence_num_exclusive: int = attr.ib(validator=attr_validators.is_int)


@attr.define(frozen=True)
class SingleCollectionDocumentDiscoveryResult:
    """Result of running document discovery for a single collection."""

    config: DocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(DocumentCollectionConfig)
    )
    temp_document_metadata_updates_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    temp_new_document_contents_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    num_new_document_contents_rows: int = attr.ib(validator=attr_validators.is_int)


@attr.define(frozen=True)
class DocumentDiscoveryResult:
    """Result of running document discovery for a state.

    Attributes:
        document_batches: Nested list of DocumentBatchRanges to process.
        temp_document_metadata_updates_addresses: A mapping from collection name
            to the address of the temp document metadata updates table containing
            diff results for that collection. The table contains all rows where
            there were any changes to document_contents_id or another metadata
            column compared to the latest metadata table state.
    """

    document_batches: list[list[DocumentBatchRange]]
    temp_document_metadata_updates_addresses: dict[str, ProjectSpecificBigQueryAddress]


def build_collection_new_document_batches(
    collection_name: str,
    temp_new_documents_table_address: ProjectSpecificBigQueryAddress,
    new_documents_table_row_count: int,
    num_batches: int,
) -> list[DocumentBatchRange]:
    """Divides rows in the table at |temp_new_documents_table_address| into
    |num_batches| even ranges. Ranges are 0-indexed."""
    batch_ranges: list[DocumentBatchRange] = []
    batch_size = math.ceil(new_documents_table_row_count / num_batches)
    for i in range(num_batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, new_documents_table_row_count)
        if start >= new_documents_table_row_count:
            break
        batch_ranges.append(
            DocumentBatchRange(
                collection_name=collection_name,
                temp_new_document_contents_table_address=temp_new_documents_table_address,
                start_sequence_num_inclusive=start,
                end_sequence_num_exclusive=end,
            )
        )
    return batch_ranges


def build_document_batches(
    collection_results: list[SingleCollectionDocumentDiscoveryResult],
    num_batches: int,
) -> list[list[DocumentBatchRange]]:
    """Divides each collection's new document contents rows into |num_batches|
    even ranges, then groups ranges across collections by batch index."""
    batches: list[list[DocumentBatchRange]] = [[] for _ in range(num_batches)]

    for result in collection_results:
        collection_batch_ranges = build_collection_new_document_batches(
            collection_name=result.config.name,
            temp_new_documents_table_address=result.temp_new_document_contents_address,
            new_documents_table_row_count=result.num_new_document_contents_rows,
            num_batches=num_batches,
        )
        for i, batch_range in enumerate(collection_batch_ranges):
            batches[i].append(batch_range)

    return batches


@attr.define
class NewDocumentDiscoverer:
    """Discovers new documents across all collections for a state and writes
    them to temporary BQ tables for downstream processing."""

    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)
    project_id: str = attr.ib(validator=attr_validators.is_str)
    big_query_client: BigQueryClient = attr.ib()
    job_id: str = attr.ib(validator=attr_validators.is_str)
    num_batches: int = attr.ib(validator=attr_validators.is_int)
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
             document_upload_status table. Each row is assigned a sequence_num
             and written to a temp new document contents table.

        The temp new document contents tables are then divided into |num_batches|
        even ranges based on sequence_num, producing a list of batches where each
        batch contains a DocumentBatchRange per collection with documents to
        process.

        Returns a DocumentDiscoveryResult containing the batch ranges (used for
        document upload to gcs) and the temp document metadata updates table
        addresses (used to update the collection metadata tables).
        """
        configs = collect_document_collection_configs(self.state_code)

        collection_results: list[SingleCollectionDocumentDiscoveryResult] = []
        temp_document_metadata_updates_addresses: dict[
            str, ProjectSpecificBigQueryAddress
        ] = {}

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
                result = future.result()
                temp_document_metadata_updates_addresses[
                    result.config.name
                ] = result.temp_document_metadata_updates_address
                if result.num_new_document_contents_rows > 0:
                    collection_results.append(result)

        return DocumentDiscoveryResult(
            document_batches=build_document_batches(
                collection_results, self.num_batches
            ),
            temp_document_metadata_updates_addresses=temp_document_metadata_updates_addresses,
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
        self.big_query_client.create_table_from_query(
            address=temp_metadata_address.to_project_agnostic_address(),
            query=diff_query,
            use_query_cache=False,
            overwrite=True,
        )

        temp_document_address = config.temp_new_document_contents_table_address(
            self.project_id, self.job_id
        )
        new_documents_query = (
            DocumentCollectionDiffQueryBuilder.build_new_documents_query(
                temp_document_metadata_updates_address=temp_metadata_address,
                upload_status_address=DocumentUploadStatusTable.get_table_address(
                    project_id=self.project_id, state_code=config.state_code
                ),
            )
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
        )
