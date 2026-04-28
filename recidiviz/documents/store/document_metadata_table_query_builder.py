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

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common import attr_validators
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
)


@attr.define
class DocumentCollectionMetadataTableQueryBuilder:
    """Builder for queries related to document collection metadata tables"""

    project_id: str = attr.ib(validator=attr_validators.is_str)

    def build_latest_documents_query(
        self,
        config: DocumentCollectionConfig,
    ) -> str:
        """Builds a query to select the latest version of each document in the
        collection, based on document primary keys and the
        row_create_datetime column. Returns the primary key columns,
        other metadata columns, and document_contents_id for each document.

        Only documents with a non-null document_contents_id are returned, since a
        null document_contents_id indicates that the document has been deleted in
        the source data.
        """
        address = config.metadata_table_address(self.project_id)

        output_columns = [
            *config.primary_key_column_names,
            *config.other_metadata_column_names,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME,
        ]

        return f"""
    SELECT
        {list_to_query_string(output_columns)}
    FROM
        {address.format_address_for_query()}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {list_to_query_string(config.primary_key_column_names)}
        ORDER BY {ROW_CREATE_DATETIME_COLUMN_NAME} DESC
    ) = 1
        -- Filter out documents that have been deleted
        AND {DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NOT NULL"""
