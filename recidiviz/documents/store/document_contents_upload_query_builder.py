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
"""Builds queries related to a collection's document_contents table."""

from datetime import datetime

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
)
from recidiviz.documents.store.document_upload_status_table import (
    COLLECTION_NAME,
    DOCUMENT_UPLOAD_SUCCESS,
    DocumentUploadStatusTable,
)


@attr.define
class DocumentContentsUploadQueryBuilder:
    """Builder for queries related to a collection's document_contents table."""

    project_id: str = attr.ib(validator=attr_validators.is_str)
    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)
    collection_name: str = attr.ib(validator=attr_validators.is_upper_snake_case)

    def build_document_contents_insert_query(
        self,
        document_contents_table_address: ProjectSpecificBigQueryAddress,
        temp_new_document_contents_address: ProjectSpecificBigQueryAddress,
        row_create_datetime: datetime,
    ) -> str:
        """Builds a DML INSERT query that inserts a row for each
        document_contents_id in the temp new document contents table whose
        upload for this collection completed successfully in this run, and
        which is not already present in the document_contents table.
        """
        upload_status_table = DocumentUploadStatusTable.get_table_address(
            project_id=self.project_id, state_code=self.state_code
        )
        return f"""
INSERT INTO {document_contents_table_address.format_address_for_query()} (
    {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    {DOCUMENT_TEXT_COLUMN_NAME},
    {DOCUMENT_LENGTH_BYTES_COLUMN_NAME},
    {ROW_CREATE_DATETIME_COLUMN_NAME}
)
SELECT
    temp.{DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    temp.{DOCUMENT_TEXT_COLUMN_NAME},
    temp.{DOCUMENT_LENGTH_BYTES_COLUMN_NAME},
    TIMESTAMP('{row_create_datetime.isoformat()}') AS {ROW_CREATE_DATETIME_COLUMN_NAME}
FROM {temp_new_document_contents_address.format_address_for_query()} temp
WHERE temp.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} IN (
    SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
    FROM {upload_status_table.format_address_for_query()}
    WHERE status = '{DOCUMENT_UPLOAD_SUCCESS}'
      AND {COLLECTION_NAME} = '{self.collection_name}'
)
AND temp.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} NOT IN (
    SELECT {DOCUMENT_CONTENTS_ID_COLUMN_NAME}
    FROM {document_contents_table_address.format_address_for_query()}
)"""
