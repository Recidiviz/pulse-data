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
"""The BigQuery tables an entity-resolution golden eval run creates in its sandbox
to stand in for the production tables the entity-resolution SQL reads.
"""
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_golden_eval_results_dataset,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GOLDEN_DOCUMENT_ID_COLUMN_NAME,
    STATE_CODE_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_columns import DOCUMENT_TEXT_COLUMN_NAME


class GoldenEvalDocumentsBQTable:
    """The golden eval documents table: one row per document in a first-order
    extractor collection's entity-resolution eval sheet, holding the sheet's text
    as written. Each run replaces the rows.

    For example, a sheet with these two rows:

        unit_1: "<p>Assigned to <b>dish duty</b>.</p>"
        unit_2: "No assignment record here."

    produces:

        state_code,golden_document_id,document_text
        US_XX,unit_1,"<p>Assigned to <b>dish duty</b>.</p>"
        US_XX,unit_2,"No assignment record here."
    """

    @staticmethod
    def table_id(collection_name: str) -> str:
        return f"{collection_name.lower()}__documents"

    @classmethod
    def address(
        cls, *, collection_name: str, sandbox_prefix: str | None
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=document_extraction_golden_eval_results_dataset(sandbox_prefix),
            table_id=cls.table_id(collection_name),
        )

    @staticmethod
    def description(*, collection_name: str) -> str:
        return (
            f"Documents from the [{collection_name}] collection's entity-resolution "
            "golden eval sheet, with text as written in the sheet. One row per "
            "golden document. Each run replaces the rows."
        )

    @staticmethod
    def schema() -> list[SchemaField]:
        return [
            SchemaField(
                name=STATE_CODE_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="State code of the extractor being evaluated",
            ),
            SchemaField(
                name=GOLDEN_DOCUMENT_ID_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The sheet's id for the document, unique within a state",
            ),
            SchemaField(
                name=DOCUMENT_TEXT_COLUMN_NAME,
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The document text as written in the sheet",
            ),
        ]

    @staticmethod
    def to_row(
        *, state_code: StateCode, golden_document_id: str, document_text: str
    ) -> dict[str, str]:
        """Returns the BigQuery row for one golden document."""
        return {
            STATE_CODE_COLUMN_NAME: state_code.value,
            GOLDEN_DOCUMENT_ID_COLUMN_NAME: golden_document_id,
            DOCUMENT_TEXT_COLUMN_NAME: document_text,
        }
