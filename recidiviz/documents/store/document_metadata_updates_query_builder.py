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
"""Builds queries related to temp document metadata updates table."""
from datetime import datetime

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
    SEQUENCE_NUM_COLUMN_NAME,
)
from recidiviz.documents.store.document_upload_status_table import (
    DOCUMENT_UPLOAD_SUCCESS,
    DocumentUploadStatusTable,
)


@attr.define
class DocumentMetadataUpdatesQueryBuilder:
    """Builder for queries related to temp document metadata updates table."""

    project_id: str = attr.ib(validator=attr_validators.is_str)
    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)

    @property
    def upload_status_table_address(self) -> ProjectSpecificBigQueryAddress:
        """Returns the BigQuery address for the document upload status table."""
        return DocumentUploadStatusTable.get_table_address(
            project_id=self.project_id, state_code=self.state_code
        )

    def build_new_documents_query(
        self,
        temp_document_metadata_updates_address: ProjectSpecificBigQueryAddress,
    ) -> str:
        """Builds a query that selects distinct (document_contents_id, document_text)
        pairs from the temp metadata diff table that have not already been
        successfully uploaded in the state, and assigns each a 0-indexed sequence_num.
        """
        return f"""
SELECT
    {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    {DOCUMENT_TEXT_COLUMN_NAME},
    ROW_NUMBER() OVER (ORDER BY {DOCUMENT_CONTENTS_ID_COLUMN_NAME}) - 1 AS {SEQUENCE_NUM_COLUMN_NAME}
FROM (
    SELECT DISTINCT
        {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
        {DOCUMENT_TEXT_COLUMN_NAME}
    FROM {temp_document_metadata_updates_address.format_address_for_query()}
    WHERE {DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NOT NULL
)
WHERE {DOCUMENT_CONTENTS_ID_COLUMN_NAME} NOT IN (
    SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
    FROM {self.upload_status_table_address.format_address_for_query()}
    WHERE status = '{DOCUMENT_UPLOAD_SUCCESS}'
)"""

    def build_successful_uploads_metadata_insert_query(
        self,
        config: DocumentCollectionConfig,
        temp_document_metadata_updates_address: ProjectSpecificBigQueryAddress,
        row_create_datetime: datetime,
    ) -> str:
        """Builds a query that selects rows from the temp metadata updates table
        to insert into the collection metadata table. Includes rows where
        document_contents_id is NULL (deleted documents) and rows whose
        document_contents_id has a SUCCESS entry in the upload status table."""
        temp_metadata_columns_select = [
            col.name
            for col in config.build_bq_metadata_schema()
            if col.name != ROW_CREATE_DATETIME_COLUMN_NAME
        ]

        return f"""
SELECT
    {list_to_query_string(temp_metadata_columns_select, table_prefix="temp")},
    TIMESTAMP('{row_create_datetime.isoformat()}') AS {ROW_CREATE_DATETIME_COLUMN_NAME}
FROM {temp_document_metadata_updates_address.format_address_for_query()} temp
WHERE temp.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} IS NULL
   OR temp.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} IN (
        SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
        FROM {self.upload_status_table_address.format_address_for_query()}
        WHERE status = '{DOCUMENT_UPLOAD_SUCCESS}'
    )"""
