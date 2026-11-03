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
"""Helpers that move entity-resolution golden eval data between the eval sheet
and the BigQuery tables the production entity-resolution SQL reads: the
first-order collection's document contents table and its __pre_resolution tables.
"""
import datetime
from collections.abc import Sequence
from typing import Any

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.eval.golden_eval_document import (
    GoldenEvalDocument,
    GoldenEvalEntityResolutionDocument,
)
from recidiviz.documents.extraction.eval.golden_eval_entity_resolution_tables import (
    GoldenEvalDocumentsBQTable,
    GoldenEvalPreResolutionBQTable,
)
from recidiviz.documents.extraction.eval.golden_eval_results_table import (
    GOLDEN_DOCUMENT_ID_COLUMN_NAME,
)
from recidiviz.documents.extraction.llm_extraction_view_graph_stage import (
    LLMExtractionViewGraphStage,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    IS_RELEVANT_FIELD_NAME,
)
from recidiviz.documents.store.document_generation_query_builder import (
    DocumentGenerationQueryBuilder,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
)
from recidiviz.documents.store.document_text_normalization import (
    build_normalized_document_text_sql,
)
from recidiviz.utils.string_formatting import fix_indent
from recidiviz.utils.types import assert_type


def first_order_config_for(config: LLMExtractorConfig) -> LLMExtractorConfig:
    """Returns the first-order extractor config that entity-resolution extractor
    |config| was generated from.
    """
    if config.extraction_stage != LLMExtractionViewGraphStage.ENTITY_RESOLUTION:
        raise ValueError(
            f"Extractor [{config.extractor_id}] is not an entity-resolution "
            f"extractor, so it has no parent first-order extractor."
        )
    return assert_type(
        config.input_document_collection, EntityResolutionDocumentCollectionConfig
    ).first_order_config


def seed_golden_eval_document_contents(
    *,
    bq_client: BigQueryClient,
    sandbox_prefix: str,
    first_order_config: LLMExtractorConfig,
    documents: Sequence[GoldenEvalDocument],
) -> dict[str, str]:
    """Prepares document contents in the format that the ER compound-document process
    expects. Loads |documents| into a sandbox table and normalizes them into a
    document_contents table. Returns each document's document_contents_id keyed by its
    golden_document_id.
    """
    load_golden_eval_documents(
        bq_client=bq_client,
        sandbox_prefix=sandbox_prefix,
        first_order_config=first_order_config,
        documents=documents,
    )
    return normalize_golden_eval_documents(
        bq_client=bq_client,
        sandbox_prefix=sandbox_prefix,
        first_order_config=first_order_config,
    )


def load_golden_eval_documents(
    *,
    bq_client: BigQueryClient,
    sandbox_prefix: str,
    first_order_config: LLMExtractorConfig,
    documents: Sequence[GoldenEvalDocument],
) -> None:
    """Replaces the first_order_config's sandbox golden eval documents
    table with the raw document texts of the |documents|.
    """
    documents_address = _golden_eval_documents_address(
        first_order_config=first_order_config, sandbox_prefix=sandbox_prefix
    )
    bq_client.delete_from_table_async(documents_address).result()
    bq_client.stream_into_table(
        documents_address,
        rows=[
            GoldenEvalDocumentsBQTable.to_row(
                state_code=document.state_code,
                golden_document_id=document.golden_document_id,
                document_text=document.document_text,
            )
            for document in documents
        ],
    )


def normalize_golden_eval_documents(
    *,
    bq_client: BigQueryClient,
    sandbox_prefix: str,
    first_order_config: LLMExtractorConfig,
) -> dict[str, str]:
    """Replaces the rows of the sandbox document_contents table of
    first_order_config's input document collection with one row per distinct
    normalized document text in its sandbox golden eval documents table, and
    returns each document's document_contents_id keyed by its golden_document_id.
    """
    document_collection = first_order_config.input_document_collection
    normalized_documents_query = _build_normalized_documents_query(
        documents_address=_golden_eval_documents_address(
            first_order_config=first_order_config, sandbox_prefix=sandbox_prefix
        ).to_project_specific_address(bq_client.project_id),
        document_contents_id_sql=DocumentGenerationQueryBuilder.document_contents_id_sql_clause(
            document_collection.state_code
        ),
    )

    rows = bq_client.run_query_async(
        query_str=normalized_documents_query, use_query_cache=False
    ).result()
    document_contents_ids_by_golden_document_id: dict[str, str] = {}
    empty_golden_document_ids: list[str] = []
    for row in rows:
        golden_document_id = row[GOLDEN_DOCUMENT_ID_COLUMN_NAME]
        if not row[DOCUMENT_TEXT_COLUMN_NAME]:
            empty_golden_document_ids.append(golden_document_id)
            continue
        document_contents_ids_by_golden_document_id[golden_document_id] = row[
            DOCUMENT_CONTENTS_ID_COLUMN_NAME
        ]
    if empty_golden_document_ids:
        raise ValueError(
            f"Golden eval documents [{', '.join(sorted(empty_golden_document_ids))}] "
            f"have no text "
            f"left after normalization, which strips HTML tags and whitespace. Give "
            f"each of these rows real document_text in the eval sheet."
        )

    contents_address = document_collection.document_contents_table_address(
        sandbox_dataset_prefix=sandbox_prefix
    )
    bq_client.delete_from_table_async(contents_address).result()
    bq_client.insert_into_table_from_query(
        destination_address=contents_address,
        query=_build_document_contents_rows_query(normalized_documents_query),
        use_query_cache=False,
    )
    return document_contents_ids_by_golden_document_id


def _golden_eval_documents_address(
    *, first_order_config: LLMExtractorConfig, sandbox_prefix: str
) -> BigQueryAddress:
    """Returns the address of first_order_config's sandbox golden eval documents
    table.
    """
    return GoldenEvalDocumentsBQTable.address(
        collection_name=first_order_config.extractor_collection.name,
        sandbox_prefix=sandbox_prefix,
    )


def _build_document_contents_rows_query(normalized_documents_query: str) -> str:
    """Returns a query that outputs one document_contents row per distinct
    document_contents_id that |normalized_documents_query| outputs.
    """
    query = f"""
SELECT DISTINCT
    {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    {DOCUMENT_TEXT_COLUMN_NAME},
    {DOCUMENT_LENGTH_BYTES_COLUMN_NAME},
    CURRENT_TIMESTAMP() AS {ROW_CREATE_DATETIME_COLUMN_NAME}
FROM (
{fix_indent(normalized_documents_query, indent_level=4)}
)
"""
    return fix_indent(query, indent_level=0)


def _build_normalized_documents_query(
    *, documents_address: ProjectSpecificBigQueryAddress, document_contents_id_sql: str
) -> str:
    """Returns a query that outputs one row per row of the golden eval documents
    table at |documents_address|, with its normalized text, that text's byte
    length, and the document_contents_id computed by |document_contents_id_sql|.
    """
    normalized_document_text = build_normalized_document_text_sql(
        DOCUMENT_TEXT_COLUMN_NAME
    )
    query = f"""
SELECT
    {GOLDEN_DOCUMENT_ID_COLUMN_NAME},
    {document_contents_id_sql} AS {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    {DOCUMENT_TEXT_COLUMN_NAME},
    BYTE_LENGTH({DOCUMENT_TEXT_COLUMN_NAME}) AS {DOCUMENT_LENGTH_BYTES_COLUMN_NAME}
FROM (
    SELECT
        {GOLDEN_DOCUMENT_ID_COLUMN_NAME},
        {normalized_document_text} AS {DOCUMENT_TEXT_COLUMN_NAME}
    FROM {documents_address.format_address_for_query()}
)
"""
    return fix_indent(query, indent_level=0)


def seed_golden_eval_pre_resolution_tables(
    *,
    bq_client: BigQueryClient,
    sandbox_prefix: str,
    first_order_config: LLMExtractorConfig,
    documents: Sequence[GoldenEvalEntityResolutionDocument],
    first_order_values_by_golden_document_id: dict[str, dict[str, Any]],
    document_contents_id_by_golden_document_id: dict[str, str],
    validation_datetime_utc: datetime.datetime,
) -> None:
    """Loads first-order expected values into the format that the ER compound-document
    process expects. Creates one __pre_resolution table per level the collection's
    entity groups are sourced from.
    """
    for (
        array_field
    ) in first_order_config.extractor_collection.entity_group_source_array_fields():
        rows = build_pre_resolution_rows(
            first_order_config=first_order_config,
            array_field=array_field,
            documents=documents,
            first_order_values_by_golden_document_id=first_order_values_by_golden_document_id,
            document_contents_id_by_golden_document_id=document_contents_id_by_golden_document_id,
            validation_datetime_utc=validation_datetime_utc,
        )
        address = GoldenEvalPreResolutionBQTable.address(
            first_order_config=first_order_config,
            array_field=array_field,
            sandbox_prefix=sandbox_prefix,
        )
        bq_client.delete_from_table_async(address).result()
        if rows:
            bq_client.stream_into_table(address, rows=rows)


def build_pre_resolution_rows(
    *,
    first_order_config: LLMExtractorConfig,
    array_field: ArrayOfStructLLMRequestOutputSchemaField | None,
    documents: Sequence[GoldenEvalEntityResolutionDocument],
    first_order_values_by_golden_document_id: dict[str, dict[str, Any]],
    document_contents_id_by_golden_document_id: dict[str, str],
    validation_datetime_utc: datetime.datetime,
) -> list[dict[str, Any]]:
    """Returns the rows of a __pre_resolution table for one level of |documents|: at
    doc-level one row per relevant document, at array-level one row per element
    of |array_field|'s value.

    Args:
        first_order_config: The first-order extractor whose table the rows belong to.
        array_field: None for the doc-level table, else the array field of the
            array-level table.
        documents: The entity-resolution golden eval documents to build rows for.
        first_order_values_by_golden_document_id: Each document's first-order
            values keyed by field name, in the shape of
            GoldenEvalDocument.expected_values, keyed by golden_document_id.
        document_contents_id_by_golden_document_id: Each document's
            document_contents_id, keyed by golden_document_id.
        validation_datetime_utc: The datetime stamped on every row.
    """
    has_is_relevant_field = (
        first_order_config.extractor_collection.output_schema.is_relevant_field
        is not None
    )
    rows: list[dict[str, Any]] = []
    for document in documents:
        golden_document_id = document.golden_document_id
        if golden_document_id not in first_order_values_by_golden_document_id:
            raise ValueError(
                f"Golden eval document [{golden_document_id}] has no first-order "
                f"values."
            )
        if golden_document_id not in document_contents_id_by_golden_document_id:
            raise ValueError(
                f"Golden eval document [{golden_document_id}] has no "
                f"document_contents_id; seed the document contents table first."
            )
        values = first_order_values_by_golden_document_id[golden_document_id]
        if has_is_relevant_field and values[IS_RELEVANT_FIELD_NAME] is not True:
            continue
        document_contents_id = document_contents_id_by_golden_document_id[
            golden_document_id
        ]
        document_update_datetime = datetime.datetime.combine(
            document.document_date, datetime.time.min, tzinfo=datetime.UTC
        )
        if array_field is None:
            field_values_by_source_array_index: list[
                tuple[int | None, dict[str, Any]]
            ] = [(None, values)]
        else:
            elements = values[array_field.name]
            field_values_by_source_array_index = (
                [] if elements is None else list(enumerate(elements))
            )
        for source_array_index, field_values in field_values_by_source_array_index:
            rows.append(
                GoldenEvalPreResolutionBQTable.to_row(
                    first_order_config=first_order_config,
                    array_field=array_field,
                    root_entity_id=document.test_group_id,
                    document_contents_id=document_contents_id,
                    document_update_datetime=document_update_datetime,
                    validation_datetime_utc=validation_datetime_utc,
                    source_array_index=source_array_index,
                    field_values=field_values,
                )
            )
    return rows
