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
to stand in for the production tables the entity-resolution SQL reads: the golden
eval documents table, and the sandbox copies of the first-order collection's
__pre_resolution materialized tables.
"""
import datetime
from typing import Any

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_golden_eval_results_dataset,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GOLDEN_DOCUMENT_ID_COLUMN_NAME,
    STATE_CODE_COLUMN_NAME,
)
from recidiviz.documents.extraction.extraction_results_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    EXTRACTOR_VERSION_ID_COLUMN_NAME,
    SOURCE_ARRAY_INDEX_COLUMN_NAME,
    VALIDATION_DATETIME_UTC_COLUMN_NAME,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.views.companion_metadata_column_configs import (
    COMPANION_METADATA_COLUMN_CONFIGS,
)
from recidiviz.documents.extraction.views.llm_extractor_array_level_results_view_builders import (
    LLMExtractorPreResolutionArrayFieldResultsViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_doc_level_results_view_builders import (
    LLMExtractorPreResolutionResultsViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_view_builder_utils import (
    parsed_llm_extractor_results_clustering_fields,
    parsed_llm_extractor_results_schema,
)
from recidiviz.documents.extraction.views.llm_extractor_view_columns import (
    parsed_data_fields,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
)


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


class GoldenEvalCompositeDocumentsBQTable:
    """The composite documents table of one entity-resolution golden eval run: the
    full generation output of the ER collection's composite SQL over the seeded
    sandbox tables, one row per test group. Each run replaces the rows.

    For example, test group 1 with two relevant documents produces (text abridged):

        person_id,document_contents_id,document_text,entry_source_map
        1,3f9c...,"=== Source document — 2026-03-01 === [Entry 1] ...",[{entry_num: 1, ...}, {entry_num: 2, ...}]
    """

    @staticmethod
    def table_id(er_collection: EntityResolutionDocumentCollectionConfig) -> str:
        return (
            f"{er_collection.state_code.value}_{er_collection.name}"
            "__composite_documents"
        ).lower()

    @classmethod
    def address(
        cls,
        *,
        er_collection: EntityResolutionDocumentCollectionConfig,
        sandbox_prefix: str | None,
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=document_extraction_golden_eval_results_dataset(sandbox_prefix),
            table_id=cls.table_id(er_collection),
        )

    @staticmethod
    def description(*, er_collection: EntityResolutionDocumentCollectionConfig) -> str:
        return (
            f"Composite documents the [{er_collection.name}] collection's generation "
            f"query built from golden first-order values during an entity-resolution "
            f"golden eval run in [{er_collection.state_code.value}]. One row per "
            f"test group, with person_id holding the test_group_id. Each run "
            f"replaces the rows."
        )

    @staticmethod
    def schema(
        *, er_collection: EntityResolutionDocumentCollectionConfig
    ) -> list[SchemaField]:
        """Returns the schema of the collection's generation output."""
        return er_collection.build_bq_document_generation_output_schema()


class GoldenEvalPreResolutionBQTable:
    """One sandbox copy of a first-order collection's __pre_resolution materialized
    table, holding the golden first-order values of an entity-resolution golden
    eval run at the deployed view's address and schema. One table per level: the
    doc-level table when array_field is None, else the array-level table for
    array_field.

    For example, golden document unit_1 in test group 1, dated 2026-03-01, whose
    assignments__expected cell holds two elements, produces these rows in the
    assignments-level table (companion metadata columns omitted, all NULL):

        person_id,document_contents_id,document_update_datetime,source_array_index,assignment_name
        1,abc123,2026-03-01 00:00:00+00,0,Dish duty
        1,abc123,2026-03-01 00:00:00+00,1,Laundry
    """

    @staticmethod
    def address(
        *,
        first_order_config: LLMExtractorConfig,
        array_field: ArrayOfStructLLMRequestOutputSchemaField | None,
        sandbox_prefix: str | None,
    ) -> BigQueryAddress:
        """Returns the address of the level's __pre_resolution materialized table,
        under sandbox_prefix when given.
        """
        if array_field is None:
            return LLMExtractorPreResolutionResultsViewBuilder.materialized_address_for_config(
                config=first_order_config, sandbox_dataset_prefix=sandbox_prefix
            )
        return LLMExtractorPreResolutionArrayFieldResultsViewBuilder.materialized_address_for_array_field(
            config=first_order_config,
            array_field=array_field,
            sandbox_dataset_prefix=sandbox_prefix,
        )

    @staticmethod
    def schema(
        *,
        first_order_config: LLMExtractorConfig,
        array_field: ArrayOfStructLLMRequestOutputSchemaField | None,
    ) -> list[SchemaField]:
        """Returns the schema of the level's __pre_resolution view."""
        return [
            column.as_schema_field()
            for column in parsed_llm_extractor_results_schema(
                first_order_config,
                array_field=array_field,
            )
        ]

    @staticmethod
    def clustering_fields(first_order_config: LLMExtractorConfig) -> list[str]:
        """Returns the clustering fields of every __pre_resolution view of
        first_order_config.
        """
        return parsed_llm_extractor_results_clustering_fields(first_order_config)

    @staticmethod
    def description(
        *,
        first_order_config: LLMExtractorConfig,
        array_field: ArrayOfStructLLMRequestOutputSchemaField | None,
    ) -> str:
        """Returns the description of the level's sandbox table."""
        level = (
            "document-level"
            if array_field is None
            else f"[{array_field.name}] array-level"
        )
        return (
            f"Sandbox copy of the {level} __pre_resolution table of the "
            f"[{first_order_config.extractor_collection.name}] collection, holding "
            f"golden first-order values from the entity-resolution golden eval "
            f"sheet."
        )

    @staticmethod
    def to_row(
        *,
        first_order_config: LLMExtractorConfig,
        array_field: ArrayOfStructLLMRequestOutputSchemaField | None,
        root_entity_id: int,
        document_contents_id: str,
        document_update_datetime: datetime.datetime,
        validation_datetime_utc: datetime.datetime,
        source_array_index: int | None,
        field_values: dict[str, Any],
    ) -> dict[str, Any]:
        """Returns the BigQuery row for one mention at the level of array_field.

        Args:
            first_order_config: The first-order extractor whose table this row
                belongs to.
            array_field: None for the doc-level table, else the array field of the
                array-level table.
            root_entity_id: The internal root entity id (person_id or staff_id)
                the mention belongs to.
            document_contents_id: The source document's content hash.
            document_update_datetime: The source document's update datetime.
            validation_datetime_utc: The datetime stamped on every row of the run.
            source_array_index: The element's position in array_field's value;
                None for the doc-level table.
            field_values: The level's values keyed by field name: the document's
                top-level values for the doc-level table, or one array element's
                sub-field values for an array-level table.
        """
        if (array_field is None) != (source_array_index is None):
            array_field_name = array_field.name if array_field is not None else None
            raise ValueError(
                f"source_array_index [{source_array_index}] must be set exactly "
                f"when array_field [{array_field_name}] is set."
            )
        root_entity_id_column_name = (
            first_order_config.input_document_collection.root_entity_id_type.resolved_internal_id_column_name
        )
        row: dict[str, Any] = {
            STATE_CODE_COLUMN_NAME: first_order_config.state_code.value,
            root_entity_id_column_name: root_entity_id,
            DOCUMENT_CONTENTS_ID_COLUMN_NAME: document_contents_id,
            DOCUMENT_UPDATE_DATETIME_COLUMN_NAME: document_update_datetime,
            VALIDATION_DATETIME_UTC_COLUMN_NAME: validation_datetime_utc,
            EXTRACTOR_VERSION_ID_COLUMN_NAME: first_order_config.extractor_version_id,
        }
        if array_field is not None:
            row[SOURCE_ARRAY_INDEX_COLUMN_NAME] = source_array_index
        fields = parsed_data_fields(first_order_config, array_field=array_field)
        for field in fields:
            # Absent means the value was not expected to be present.
            row[field.name] = field_values.get(field.name)
        if any(field.is_inferred_field for field in fields):
            for companion in COMPANION_METADATA_COLUMN_CONFIGS:
                row[companion.column_name] = None
        return row
