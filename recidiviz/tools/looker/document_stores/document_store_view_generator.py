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
"""Generates LookML view files for document store metadata, document contents,
extraction results, and array-level extraction results.

One metadata view and one document-contents view are generated per
(state, document_collection) pair. One extraction-results view (doc-level) plus
one array-level view per ARRAY_OF_STRUCT field are generated per
(state, extractor_collection).
"""
import os

from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.documents.extraction.extraction_results_columns import (
    EXTRACTOR_VERSION_ID_COLUMN_NAME,
    STATE_CODE_COLUMN_NAME,
    VALIDATION_DATETIME_UTC_COLUMN_NAME,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
    ScalarValuedLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    CITATION_TEXT_FIELD_NAME,
)
from recidiviz.documents.extraction.views.companion_metadata_column_configs import (
    COMPANION_METADATA_COLUMN_CONFIGS,
    CitationsCompanionMetadataColumnConfig,
    ScalarCompanionMetadataColumnConfig,
)
from recidiviz.documents.extraction.views.llm_extractor_array_level_results_view_builders import (
    LLMExtractorArrayFieldResultsViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_doc_level_results_view_builders import (
    LLMExtractorResultsViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_view_columns import (
    SOURCE_ARRAY_INDEX_COLUMN_NAME,
    parsed_data_fields,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
)
from recidiviz.looker.lookml_bq_utils import (
    DATETIME_TIMEFRAMES,
    lookml_field_type_for_bq_type,
    lookml_view_field_for_schema_field,
)
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLViewField,
    MeasureLookMLViewField,
    TimeDimensionGroupLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldDatatype,
    LookMLFieldParameter,
    LookMLFieldType,
)
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable

DOCUMENT_STORES_VIEWS_SUBDIR = "document_stores"

# ---------------------------------------------------------------------------
# View name helpers — exported so the explore generator can reference views
# by name without regenerating them.
# ---------------------------------------------------------------------------


def metadata_view_name(collection_config: DocumentCollectionConfig) -> str:
    """Returns the LookML view name for a document collection's metadata table."""
    state = collection_config.state_code.value.lower()
    return f"{state}_{collection_config.name.lower()}_metadata"


def document_contents_view_name(collection_config: DocumentCollectionConfig) -> str:
    """Returns the LookML view name for a document collection's contents table."""
    state = collection_config.state_code.value.lower()
    return f"{state}_{collection_config.name.lower()}_document_contents"


def extraction_results_view_name(extractor_config: LLMExtractorConfig) -> str:
    """Returns the LookML view name for an extractor's doc-level results table."""
    state = extractor_config.state_code.value.lower()
    return f"{state}_{extractor_config.extractor_collection.name.lower()}"


def array_field_view_name(
    extractor_config: LLMExtractorConfig,
    array_field: ArrayOfStructLLMRequestOutputSchemaField,
) -> str:
    """Returns the LookML view name for one ARRAY_OF_STRUCT field's fanout table."""
    state = extractor_config.state_code.value.lower()
    return f"{state}_{extractor_config.extractor_collection.name.lower()}_{array_field.name}"


# ---------------------------------------------------------------------------
# Field builders
# ---------------------------------------------------------------------------


def _timestamp_dimension_group(
    column_name: str,
    description: str,
) -> TimeDimensionGroupLookMLViewField:
    """Returns a timestamp dimension group for a known TIMESTAMP column."""
    return TimeDimensionGroupLookMLViewField.for_column(
        column_name=column_name,
        datatype=LookMLFieldDatatype.TIMESTAMP,
        timeframe_options=DATETIME_TIMEFRAMES,
        custom_params=[LookMLFieldParameter.description(description)],
    )


def _scalar_field_dimension(
    field: ScalarValuedLLMRequestOutputSchemaField,
) -> DimensionLookMLViewField:
    """Returns a dimension for one scalar extracted field."""
    field_type = lookml_field_type_for_bq_type(
        field.field_type.primitive_scalar_value_big_query_type()
    )
    params = (
        [LookMLFieldParameter.description(field.base_description)]
        if field.base_description
        else None
    )
    return DimensionLookMLViewField.for_column(field.name, field_type, params)


def _companion_sub_field_dimensions(
    fields: list[ScalarValuedLLMRequestOutputSchemaField],
) -> list[DimensionLookMLViewField]:
    """Returns dimensions for companion metadata STRUCTs (confidence levels, null
    reasons, adversarial interpretations, citations). Scalar companions are surfaced
    directly; citations (REPEATED RECORD) are aggregated into a single string via
    STRING_AGG on the citation text sub-field."""
    inferred = [f for f in fields if f.is_inferred_field]
    if not inferred:
        return []
    dims = []
    for companion in COMPANION_METADATA_COLUMN_CONFIGS:
        for field in inferred:
            if isinstance(companion, ScalarCompanionMetadataColumnConfig):
                sql = f"${{TABLE}}.{companion.column_name}.{field.name}"
            elif isinstance(companion, CitationsCompanionMetadataColumnConfig):
                sql = (
                    f"(SELECT STRING_AGG(c.{CITATION_TEXT_FIELD_NAME}, ' | ') "
                    f"FROM UNNEST(${{TABLE}}.{companion.column_name}.{field.name}) AS c "
                    f"WHERE c.{CITATION_TEXT_FIELD_NAME} IS NOT NULL)"
                )
            else:
                raise ValueError(
                    f"Unexpected companion metadata config type [{type(companion)}]"
                )
            dims.append(
                DimensionLookMLViewField(
                    field_name=f"{companion.column_name}__{field.name}",
                    parameters=[
                        LookMLFieldParameter.type(LookMLFieldType.STRING),
                        LookMLFieldParameter.group_label(
                            snake_to_title(companion.column_name)
                        ),
                        LookMLFieldParameter.label(snake_to_title(field.name)),
                        LookMLFieldParameter.description(
                            companion.field_description(field.name)
                        ),
                        LookMLFieldParameter.sql(sql),
                    ],
                )
            )
    return dims


# ---------------------------------------------------------------------------
# View builders
# ---------------------------------------------------------------------------


def _metadata_view(collection_config: DocumentCollectionConfig) -> LookMLView:
    """Returns a LookMLView for a document collection's metadata table."""
    address = collection_config.metadata_table_address(sandbox_dataset_prefix=None)
    fields: list[LookMLViewField] = []
    for field in collection_config.build_bq_metadata_schema():
        custom_params = []
        if field.name == DOCUMENT_CONTENTS_ID_COLUMN_NAME:
            custom_params.append(LookMLFieldParameter.primary_key(True))
        if field.description:
            custom_params.append(LookMLFieldParameter.description(field.description))
        fields.append(lookml_view_field_for_schema_field(field, custom_params or None))
    fields.append(
        MeasureLookMLViewField(
            field_name="count",
            parameters=[LookMLFieldParameter.type(LookMLFieldType.COUNT)],
        )
    )
    return LookMLView(
        view_name=metadata_view_name(collection_config),
        table=LookMLViewSourceTable.sql_table_address(address),
        fields=fields,
    )


def _document_contents_view(collection_config: DocumentCollectionConfig) -> LookMLView:
    """Returns a LookMLView for a document collection's document contents table."""
    address = collection_config.document_contents_table_address(
        sandbox_dataset_prefix=None
    )
    fields: list[LookMLViewField] = []
    for field in collection_config.build_bq_document_contents_schema():
        custom_params = []
        if field.name == DOCUMENT_CONTENTS_ID_COLUMN_NAME:
            custom_params.append(LookMLFieldParameter.primary_key(True))
        if field.name == DOCUMENT_TEXT_COLUMN_NAME:
            custom_params.append(
                LookMLFieldParameter.drill_fields(
                    [DOCUMENT_CONTENTS_ID_COLUMN_NAME, DOCUMENT_TEXT_COLUMN_NAME]
                )
            )
        if field.description:
            custom_params.append(LookMLFieldParameter.description(field.description))
        fields.append(lookml_view_field_for_schema_field(field, custom_params or None))
    return LookMLView(
        view_name=document_contents_view_name(collection_config),
        table=LookMLViewSourceTable.sql_table_address(address),
        fields=fields,
    )


def _extraction_results_view(extractor_config: LLMExtractorConfig) -> LookMLView:
    """Returns a LookMLView for an extractor's doc-level results table."""
    address = LLMExtractorResultsViewBuilder.materialized_address_for_config(
        config=extractor_config, sandbox_dataset_prefix=None
    )
    root_id_col = (
        extractor_config.input_document_collection.root_entity_id_type.resolved_internal_id_column_name
    )
    scalar_fields = (
        extractor_config.extractor_collection.output_schema.scalar_valued_user_fields
    )
    fields: list[LookMLViewField] = [
        DimensionLookMLViewField.for_column(
            STATE_CODE_COLUMN_NAME,
            LookMLFieldType.STRING,
            [
                LookMLFieldParameter.hidden(True),
                LookMLFieldParameter.description("State code."),
            ],
        ),
        DimensionLookMLViewField.for_column(
            root_id_col,
            LookMLFieldType.NUMBER,
            [
                LookMLFieldParameter.hidden(True),
                LookMLFieldParameter.description(f"Internal Recidiviz {root_id_col}."),
            ],
        ),
        DimensionLookMLViewField.for_column(
            DOCUMENT_CONTENTS_ID_COLUMN_NAME,
            LookMLFieldType.STRING,
            [
                LookMLFieldParameter.primary_key(True),
                LookMLFieldParameter.description(
                    "Unique identifier of the document text that was extracted."
                ),
            ],
        ),
        _timestamp_dimension_group(
            DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
            "When the source document was last updated.",
        ),
        _timestamp_dimension_group(
            VALIDATION_DATETIME_UTC_COLUMN_NAME,
            "When this extraction result was validated.",
        ),
        DimensionLookMLViewField.for_column(
            EXTRACTOR_VERSION_ID_COLUMN_NAME,
            LookMLFieldType.STRING,
            [
                LookMLFieldParameter.description(
                    "Version of the extractor configuration that produced this result."
                )
            ],
        ),
        *[_scalar_field_dimension(f) for f in scalar_fields],
        *_companion_sub_field_dimensions(scalar_fields),
    ]
    return LookMLView(
        view_name=extraction_results_view_name(extractor_config),
        table=LookMLViewSourceTable.sql_table_address(address),
        fields=fields,
    )


def _array_field_view(
    extractor_config: LLMExtractorConfig,
    array_field: ArrayOfStructLLMRequestOutputSchemaField,
) -> LookMLView:
    """Returns a LookMLView for one ARRAY_OF_STRUCT field's fanout table."""
    address = (
        LLMExtractorArrayFieldResultsViewBuilder.materialized_address_for_array_field(
            config=extractor_config,
            array_field=array_field,
            sandbox_dataset_prefix=None,
        )
    )
    root_id_col = (
        extractor_config.input_document_collection.root_entity_id_type.resolved_internal_id_column_name
    )
    array_sub_fields = parsed_data_fields(extractor_config, array_field=array_field)
    fields: list[LookMLViewField] = [
        DimensionLookMLViewField.for_column(
            STATE_CODE_COLUMN_NAME,
            LookMLFieldType.STRING,
            [
                LookMLFieldParameter.hidden(True),
                LookMLFieldParameter.description("State code."),
            ],
        ),
        DimensionLookMLViewField.for_column(
            root_id_col,
            LookMLFieldType.NUMBER,
            [
                LookMLFieldParameter.hidden(True),
                LookMLFieldParameter.description(f"Internal Recidiviz {root_id_col}."),
            ],
        ),
        DimensionLookMLViewField.for_column(
            DOCUMENT_CONTENTS_ID_COLUMN_NAME,
            LookMLFieldType.STRING,
            [LookMLFieldParameter.description("Document this element belongs to.")],
        ),
        _timestamp_dimension_group(
            DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
            "When the source document was last updated.",
        ),
        _timestamp_dimension_group(
            VALIDATION_DATETIME_UTC_COLUMN_NAME,
            "When this extraction result was validated.",
        ),
        DimensionLookMLViewField.for_column(
            EXTRACTOR_VERSION_ID_COLUMN_NAME,
            LookMLFieldType.STRING,
            [
                LookMLFieldParameter.description(
                    "Version of the extractor configuration that produced this result."
                )
            ],
        ),
        DimensionLookMLViewField.for_column(
            SOURCE_ARRAY_INDEX_COLUMN_NAME,
            LookMLFieldType.NUMBER,
            [
                LookMLFieldParameter.description(
                    "0-indexed position of this element in the source array."
                )
            ],
        ),
        *[_scalar_field_dimension(f) for f in array_sub_fields],
        *_companion_sub_field_dimensions(array_sub_fields),
    ]
    return LookMLView(
        view_name=array_field_view_name(extractor_config, array_field),
        table=LookMLViewSourceTable.sql_table_address(address),
        fields=fields,
    )


# ---------------------------------------------------------------------------
# Public write functions
# ---------------------------------------------------------------------------


def write_metadata_view(
    *,
    output_dir: str,
    collection_config: DocumentCollectionConfig,
    source_script_path: str,
) -> None:
    """Writes the metadata view LKml file for |collection_config|."""
    state = collection_config.state_code.value.lower()
    view_output_dir = os.path.join(output_dir, DOCUMENT_STORES_VIEWS_SUBDIR, state)
    _metadata_view(collection_config).write(view_output_dir, source_script_path)


def write_document_contents_view(
    *,
    output_dir: str,
    collection_config: DocumentCollectionConfig,
    source_script_path: str,
) -> None:
    """Writes the document contents view LKml file for |collection_config|."""
    state = collection_config.state_code.value.lower()
    view_output_dir = os.path.join(output_dir, DOCUMENT_STORES_VIEWS_SUBDIR, state)
    _document_contents_view(collection_config).write(
        view_output_dir, source_script_path
    )


def write_extraction_results_view(
    *,
    output_dir: str,
    extractor_config: LLMExtractorConfig,
    source_script_path: str,
) -> None:
    """Writes the doc-level extraction results view LKml file for |extractor_config|."""
    state = extractor_config.state_code.value.lower()
    view_output_dir = os.path.join(output_dir, DOCUMENT_STORES_VIEWS_SUBDIR, state)
    _extraction_results_view(extractor_config).write(
        view_output_dir, source_script_path
    )


def write_array_field_view(
    *,
    output_dir: str,
    extractor_config: LLMExtractorConfig,
    array_field: ArrayOfStructLLMRequestOutputSchemaField,
    source_script_path: str,
) -> None:
    """Writes the array-level fanout view LKml file for |array_field|."""
    state = extractor_config.state_code.value.lower()
    view_output_dir = os.path.join(output_dir, DOCUMENT_STORES_VIEWS_SUBDIR, state)
    _array_field_view(extractor_config, array_field).write(
        view_output_dir, source_script_path
    )
