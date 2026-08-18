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
"""Builds the query that diffs a document collection's freshly generated documents
against the latest state in its metadata table."""

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_utils import null_sql_cast_clause_for_schema_field
from recidiviz.calculator.query.bq_utils import (
    join_on_columns_fragment,
    list_to_query_string,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_metadata_table_query_builder import (
    DocumentCollectionMetadataTableQueryBuilder,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)


@attr.define
class DocumentCollectionDiffQueryBuilder:
    """Builds queries to diff document collection generations against the latest metadata table state."""

    def build_document_diff_query(
        self,
        *,
        config: DocumentCollectionConfig,
        document_generation_output_address: ProjectSpecificBigQueryAddress,
        metadata_table_address: ProjectSpecificBigQueryAddress,
    ) -> str:
        """Builds a query that diffs freshly generated documents against the
        latest state in the metadata table. Reads |document_generation_output_address|,
        the run's already-materialized document_generation_query output, and
        |metadata_table_address|, the collection's metadata table.
        Returns temp table schema rows for:
        - Added documents: primary key exists in new but not current
        - Updated documents: primary key exists in both but document_contents_id or metadata differs
        - Deleted documents: primary key exists in current but not new, with
          all non-pk fields set to NULL

        The two sides are compared for presence, not just for changes, so they must
        always cover the same root entities. Narrowing one side without the other
        reports every root entity missing from it as wholesale deletions (or, in the
        other direction, as additions).
        """
        latest_query = DocumentCollectionMetadataTableQueryBuilder().build_latest_documents_query(
            config,
            metadata_table_address=metadata_table_address,
            # TODO(OBT-32176): Revisit if we need to pass a filter config through
            # here when doing ER document discovery in a sandbox.
            document_filter=None,
        )

        temp_table_schema = config.build_bq_document_generation_output_schema()

        generation_output_columns = list_to_query_string(
            [col.name for col in temp_table_schema]
        )
        generation_output_query = document_generation_output_address.select_query(
            select_statement=f"SELECT {generation_output_columns}"
        )

        join_clause = join_on_columns_fragment(
            config.primary_key_column_names, table1="new_docs", table2="current_docs"
        )

        new_doc_select_columns = list_to_query_string(
            [col.name for col in temp_table_schema],
            table_prefix="new_docs",
        )

        def _deleted_col_expr(col: bigquery.SchemaField) -> str:
            if col.name in config.primary_key_column_names:
                return f"current_docs.{col.name}"
            # A deleted document has no value for any non-PK column.
            return null_sql_cast_clause_for_schema_field(col)

        deleted_doc_select_columns = list_to_query_string(
            [_deleted_col_expr(col) for col in temp_table_schema]
        )

        deleted_where_clause = " AND ".join(
            f"new_docs.{col} IS NULL" for col in config.primary_key_column_names
        )

        # Columns present in both new_docs and current_docs that we compare
        # to detect updates (metadata or document content changes).
        comparable_columns = [
            *config.other_metadata_column_names,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME,
        ]
        changed_clause = " OR ".join(
            f"current_docs.{col} IS DISTINCT FROM new_docs.{col}"
            for col in comparable_columns
        )

        output_columns = list_to_query_string([col.name for col in temp_table_schema])

        return f"""
WITH new_docs AS (
    {generation_output_query}
),
current_docs AS (
    {latest_query}
),
added_or_updated AS (
    SELECT
        {new_doc_select_columns}
    FROM new_docs
    LEFT JOIN current_docs
    ON {join_clause}
    WHERE current_docs.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NULL
       OR {changed_clause}
),
deleted AS (
    SELECT
        {deleted_doc_select_columns}
    FROM current_docs
    LEFT JOIN new_docs
    ON {join_clause}
    WHERE {deleted_where_clause}
)
SELECT {output_columns} FROM added_or_updated
UNION ALL
SELECT {output_columns} FROM deleted"""
