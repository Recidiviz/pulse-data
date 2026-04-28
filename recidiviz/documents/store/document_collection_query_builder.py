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
"""Builds queries for document collections."""

import attr
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    join_on_columns_fragment,
    list_to_query_string,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_metadata_table_query_builder import (
    DocumentCollectionMetadataTableQueryBuilder,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
)
from recidiviz.utils.string import StrictStringFormatter


@attr.define
class DocumentCollectionDiffQueryBuilder:
    """Builds queries to diff document collection generations against the latest metadata table state."""

    project_id: str = attr.ib(validator=attr_validators.is_str)

    @staticmethod
    def _document_contents_id_sql_clause(state_code: StateCode) -> str:
        """Builds the sql to compute the document_contents_id for a given
        |state_code|."""
        return f"TO_HEX(SHA256(CONCAT('{state_code.value}', '|', {DOCUMENT_TEXT_COLUMN_NAME})))"

    def build_document_generation_query(
        self,
        config: DocumentCollectionConfig,
    ) -> str:
        """Wraps the config's document_generation_query_template to produce
        all columns needed for downstream processing: the original query output
        columns plus a computed document_contents_id.
        """
        inner_query = StrictStringFormatter().format(
            config.document_generation_query_template,
            project_id=self.project_id,
        )

        return f"""
SELECT
    {self._document_contents_id_sql_clause(config.state_code)} AS {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    {DOCUMENT_TEXT_COLUMN_NAME},
    {DOCUMENT_UPDATE_DATETIME_COLUMN_NAME},
    {list_to_query_string(config.primary_key_column_names + config.other_metadata_column_names)}
FROM ({inner_query})"""

    def build_document_diff_query(
        self,
        config: DocumentCollectionConfig,
    ) -> str:
        """Builds a query that diffs freshly generated documents against the
        latest state in the metadata table. Returns temp table schema rows for:
        - Added documents: primary key exists in new but not current
        - Updated documents: primary key exists in both but document_contents_id or metadata differs
        - Deleted documents: primary key exists in current but not new, with
          all non-pk fields set to NULL
        """
        generation_query = self.build_document_generation_query(config)
        latest_query = DocumentCollectionMetadataTableQueryBuilder(
            project_id=self.project_id,
        ).build_latest_documents_query(config)

        temp_table_schema = config.build_bq_temp_document_metadata_updates_schema()

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
            return f"CAST(NULL AS {col.field_type}) AS {col.name}"

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
    {generation_query}
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
