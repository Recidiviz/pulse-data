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
"""Builds queries related to document collection metadata tables."""

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorDocumentFilterConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
)


@attr.define
class DocumentCollectionMetadataTableQueryBuilder:
    """Builder for queries related to document collection metadata tables"""

    def build_latest_documents_query(
        self,
        config: DocumentCollectionConfig,
        metadata_table_address: ProjectSpecificBigQueryAddress,
        *,
        document_filter: LLMExtractorDocumentFilterConfig | None,
    ) -> str:
        """Builds a query to select the latest version of each document in the
        collection, based on document primary keys and the
        row_create_datetime column. Returns the primary key columns,
        other metadata columns, document_contents_id, and
        document_update_datetime for each document.

        Only documents with a non-null document_contents_id are returned, since a
        null document_contents_id indicates that the document has been deleted in
        the source data.

        |document_filter| specifies how the documents should be further narrowed (i.e.
        down to documents pertaining to a specific set of root entities), if relevant.
        """
        root_entity_narrowing_join_sql = (
            document_filter.build_root_entity_narrowing_join_sql(
                root_entity_id_type=config.root_entity_id_type, indent_level=4
            )
            if document_filter is not None
            else ""
        )

        output_columns = [
            *config.primary_key_column_names,
            *config.other_metadata_column_names,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME,
            DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
        ]

        return f"""
    SELECT
        {list_to_query_string(output_columns)}
    FROM
        {metadata_table_address.format_address_for_query()}{root_entity_narrowing_join_sql}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {list_to_query_string(config.primary_key_column_names)}
        ORDER BY {ROW_CREATE_DATETIME_COLUMN_NAME} DESC
    ) = 1
        -- Filter out documents that have been deleted
        AND {DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NOT NULL"""
