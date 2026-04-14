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

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
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
