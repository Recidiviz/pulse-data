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
"""Generates a LookML explore per (state, document_collection) that joins:
  - the collection's metadata view (base)
  - the collection's document-contents view
  - for each extractor that reads this collection:
    - the extractor's doc-level extraction-results view
    - one fanout view per ARRAY_OF_STRUCT field in that extractor's schema

All joins use document_contents_id as the key. Contents and extraction-results
views join many_to_one (multiple metadata rows can share the same contents hash);
array-level views join one_to_many (each doc-level row fans out to its elements).
"""
import os

from recidiviz.common.str_field_utils import snake_to_title
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    DocumentRootEntityIdType,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    PERSON_EXTERNAL_ID_COLUMN_NAME,
    PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME,
)
from recidiviz.looker.lookml_explore import LookMLExplore
from recidiviz.looker.lookml_explore_parameter import (
    JoinCardinality,
    JoinType,
    LookMLExploreParameter,
    LookMLJoinParameter,
)
from recidiviz.looker.lookml_utils import write_lookml_file
from recidiviz.tools.looker.document_stores.document_store_view_generator import (
    array_field_view_name,
    document_contents_view_name,
    extraction_results_view_name,
    metadata_view_name,
)

DOCUMENT_STORES_EXPLORES_SUBDIR = "document_stores"


def _explore_base_name(collection_config: DocumentCollectionConfig) -> str:
    """Returns the base explore name used for the file name and the concrete model entry."""
    state = collection_config.state_code.value.lower()
    collection = collection_config.name.lower()
    return f"{state}_{collection}_documents"


def _explore_template_name(collection_config: DocumentCollectionConfig) -> str:
    """Returns the template explore name (with _template suffix, extension: required)."""
    return f"{_explore_base_name(collection_config)}_template"


def _explore_label(collection_config: DocumentCollectionConfig) -> str:
    state = collection_config.state_code.value.upper()
    return f"{state} {snake_to_title(collection_config.name)}"


_STATE_PERSON_EXTERNAL_ID_VIEW = "state_person_external_id"
_STATE_PERSON_VIEW = "state_person"


def _doc_join(
    *,
    joined_view: str,
    view_label: str,
    base_view: str,
    relationship: JoinCardinality,
) -> LookMLExploreParameter:
    """Returns a join parameter linking |joined_view| to |base_view| on document_contents_id."""
    sql_on_text = (
        f"${{{base_view}.{DOCUMENT_CONTENTS_ID_COLUMN_NAME}}} = "
        f"${{{joined_view}.{DOCUMENT_CONTENTS_ID_COLUMN_NAME}}}"
    )
    return LookMLExploreParameter.join(
        joined_view,
        [
            LookMLJoinParameter.type(JoinType.LEFT_OUTER),
            LookMLJoinParameter.view_label(view_label),
            LookMLJoinParameter.relationship(relationship),
            LookMLJoinParameter.sql_on(sql_on_text),
        ],
    )


def _person_join_parameters(base_view: str) -> list[LookMLExploreParameter]:
    """Returns join parameters linking base_view → state_person_external_id → state_person.

    Both joins use the "Person" view label so all person fields appear together in
    the field picker. The external-id join exposes only external_id and id_type so
    that _filters references in state_person's derived-table SQL resolve correctly,
    while keeping its other fields out of the explore.
    """
    external_id_sql_on = (
        f"${{{base_view}.{PERSON_EXTERNAL_ID_COLUMN_NAME}}} = "
        f"${{{_STATE_PERSON_EXTERNAL_ID_VIEW}.external_id}}\n"
        f"            AND ${{{base_view}.{PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME}}} = "
        f"${{{_STATE_PERSON_EXTERNAL_ID_VIEW}.id_type}}"
    )
    person_sql_on = (
        f"${{{_STATE_PERSON_EXTERNAL_ID_VIEW}.person_id}} = "
        f"${{{_STATE_PERSON_VIEW}.person_id}}"
    )
    return [
        LookMLExploreParameter.join(
            _STATE_PERSON_EXTERNAL_ID_VIEW,
            [
                LookMLJoinParameter.type(JoinType.LEFT_OUTER),
                LookMLJoinParameter.view_label("Person"),
                LookMLJoinParameter.fields(
                    [
                        f"{_STATE_PERSON_EXTERNAL_ID_VIEW}.external_id",
                        f"{_STATE_PERSON_EXTERNAL_ID_VIEW}.id_type",
                    ]
                ),
                LookMLJoinParameter.relationship(JoinCardinality.MANY_TO_ONE),
                LookMLJoinParameter.sql_on(external_id_sql_on),
            ],
        ),
        LookMLExploreParameter.join(
            _STATE_PERSON_VIEW,
            [
                LookMLJoinParameter.type(JoinType.LEFT_OUTER),
                LookMLJoinParameter.view_label("Person"),
                LookMLJoinParameter.relationship(JoinCardinality.MANY_TO_ONE),
                LookMLJoinParameter.sql_on(person_sql_on),
            ],
        ),
    ]


def _build_explore(
    collection_config: DocumentCollectionConfig,
    extractor_configs: list[LLMExtractorConfig],
) -> LookMLExplore:
    """Returns a LookMLExplore for one (state, document_collection) pair."""
    base_view = metadata_view_name(collection_config)
    has_person_external_id_join = (
        collection_config.root_entity_id_type
        is DocumentRootEntityIdType.PERSON_EXTERNAL_ID
    )

    parameters: list[LookMLExploreParameter] = [
        LookMLExploreParameter.label(_explore_label(collection_config)),
        _doc_join(
            joined_view=document_contents_view_name(collection_config),
            view_label="Document Text",
            base_view=base_view,
            relationship=JoinCardinality.MANY_TO_ONE,
        ),
    ]

    if has_person_external_id_join:
        parameters.extend(_person_join_parameters(base_view))

    for extractor_config in extractor_configs:
        results_view = extraction_results_view_name(extractor_config)
        extractor_label = snake_to_title(
            extractor_config.extractor_collection.name.lower()
        )
        parameters.append(
            _doc_join(
                joined_view=results_view,
                view_label=extractor_label,
                base_view=base_view,
                relationship=JoinCardinality.MANY_TO_ONE,
            )
        )

        array_fields = (
            extractor_config.extractor_collection.output_schema.array_of_struct_user_fields
        )
        for array_field in array_fields:
            fanout_view = array_field_view_name(extractor_config, array_field)
            fanout_label = f"{extractor_label}: {snake_to_title(array_field.name)}"
            parameters.append(
                _doc_join(
                    joined_view=fanout_view,
                    view_label=fanout_label,
                    base_view=base_view,
                    relationship=JoinCardinality.ONE_TO_MANY,
                )
            )

    return LookMLExplore(
        explore_name=_explore_template_name(collection_config),
        extension_required=True,
        view_name=base_view,
        view_label=f"{snake_to_title(collection_config.name)} Metadata",
        parameters=parameters,
    )


def write_explore(
    *,
    output_dir: str,
    collection_config: DocumentCollectionConfig,
    extractor_configs: list[LLMExtractorConfig],
    source_script_path: str,
) -> None:
    """Writes the explore LKml file for one (state, document_collection) pair."""
    explore_output_dir = os.path.join(output_dir, DOCUMENT_STORES_EXPLORES_SUBDIR)
    write_lookml_file(
        output_directory=explore_output_dir,
        file_name=f"{_explore_base_name(collection_config)}.explore.lkml",
        source_script_path=source_script_path,
        file_body=_build_explore(collection_config, extractor_configs).build(),
    )
