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
"""Builds the SQL that selects an extractor's eligible document_contents_ids."""

import attr

from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorDocumentFilterConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_metadata_table_query_builder import (
    DocumentCollectionMetadataTableQueryBuilder,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
)
from recidiviz.utils.string_formatting import fix_indent


@attr.define(frozen=True, kw_only=True)
class LLMExtractionEligibleDocumentQueryBuilder:
    """Builds the SQL that selects an extractor's eligible
    document_contents_ids.

    The authored filter query returns only document_contents_id; this builder
    joins that result to the input collection's metadata and document_contents
    tables to capture each document's document_length_bytes and
    document_update_datetime.
    """

    # The document-selection config: the authored filter template plus any
    # sandbox-only narrowing.
    document_filter: LLMExtractorDocumentFilterConfig = attr.ib(
        validator=attr.validators.instance_of(LLMExtractorDocumentFilterConfig)
    )
    # The collection the filter selects documents from; supplies the
    # metadata and document_contents table addresses and the root-entity ID
    # column that root_entity_ids narrowing matches against.
    input_document_collection: DocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(DocumentCollectionConfig)
    )

    def build_query(self, *, project_id: str) -> str:
        """Returns the SQL that selects this extractor's eligible documents, one
        row per document_contents_id with its document_length_bytes and
        document_update_datetime. Identical text shared across root entities maps
        to one document_contents_id; the query keeps the oldest such document.
        """
        metadata_address = self.input_document_collection.metadata_table_address.to_project_specific_address(
            project_id
        )
        filter_query = self.document_filter.build_document_metadata_filter_query(
            input_document_collection_metadata_address=metadata_address,
        )

        contents_address = self.input_document_collection.document_contents_table_address.to_project_specific_address(
            project_id
        )

        latest_metadata_query = DocumentCollectionMetadataTableQueryBuilder(
            project_id=project_id
        ).build_latest_documents_query(
            self.input_document_collection, document_filter=self.document_filter
        )

        # The QUALIFY dedupes to one row per document_contents_id; the
        # document_limit ORDER BY/LIMIT runs last so the cap applies to deduped
        # rows.
        # TODO(OBT-42802): Avoid the three-way join here: store
        # document_length_bytes on the metadata row to drop the document_contents
        # join, and have the filter query read from latest_metadata directly so
        # the filter and dedup compose into one scan instead of a separate join.
        query = f"""
WITH eligible_documents AS (
{fix_indent(filter_query, indent_level=4)}
),
latest_metadata AS (
{fix_indent(latest_metadata_query, indent_level=4)}
)
SELECT
    eligible_documents.{DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    document_contents.{DOCUMENT_LENGTH_BYTES_COLUMN_NAME},
    latest_metadata.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}
FROM eligible_documents
JOIN latest_metadata
    USING ({DOCUMENT_CONTENTS_ID_COLUMN_NAME})
JOIN {contents_address.format_address_for_query()} document_contents
    USING ({DOCUMENT_CONTENTS_ID_COLUMN_NAME})
-- A single document_contents_id can appear for multiple root entities (identical
-- text shared across the same or different people), so this QUALIFY dedupes down
-- to exactly one row per document_contents_id. The document_update_datetime we
-- keep is used only to sort documents when batching processing across jobs, so
-- it does not matter which of the shared documents' datetimes we associate with
-- the id; we keep the oldest.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
    ORDER BY latest_metadata.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}
) = 1
{self._order_by_clause()}
{self._limit_clause()}"""

        return fix_indent(query, indent_level=0)

    def _order_by_clause(self) -> str:
        """Returns the sandbox-only ORDER BY clause (with a leading newline), or
        empty string when no document_limit is set (production). Ordering
        oldest-first makes the document_limit cap deterministic and prioritizes
        the oldest documents.
        """
        if self.document_filter.document_limit is None:
            return ""
        return f"ORDER BY latest_metadata.{DOCUMENT_UPDATE_DATETIME_COLUMN_NAME}"

    def _limit_clause(self) -> str:
        """Returns the sandbox-only LIMIT clause, or empty string when no
        document_limit is set (production).
        """
        if (document_limit := self.document_filter.document_limit) is None:
            return ""
        return f"LIMIT {document_limit}"
