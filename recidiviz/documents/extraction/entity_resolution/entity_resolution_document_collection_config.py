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
"""The document collection config for an entity-resolution composite-document
collection: one composite document per root entity, aggregating that entity's
first-order mentions of a single entity group for the ER extractor to resolve.

ER composite documents ride the standard document store — they are generated,
content-addressed, and uploaded like any other collection — so this is a
`DocumentCollectionConfig` and everything in the store flow treats it as one.
Code that must distinguish an ER composite collection from a first-order one does
so by type, rather than by inspecting collection names.

The collection is fully determined by the (first-order extractor config, entity
group) pair it resolves, so those are its only two inputs and every
`DocumentCollectionConfig` field — down to the composite-document generation
query — is derived from them.
"""
import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_attr_validators import (
    is_valid_unquoted_bq_identifier,
)
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.dataset_config import (
    document_extraction_pre_resolution_results_dataset_for_region,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    EntityResolutionCompositeDocumentQueryTemplateBuilder,
    entry_source_map_schema_field,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    DocumentRootEntityIdType,
)

# Suffix appended to the first-order extractor collection name (plus the entity
# group name) to form the composite-document collection name.
ENTITY_RESOLUTION_COLLECTION_NAME_SUFFIX = "ENTITY_RESOLUTION"


def entity_resolution_collection_name(
    *, first_order_extractor_collection_name: str, entity_group_name: str
) -> str:
    """Returns the composite-document collection name for a (first-order extractor
    collection, entity group) pair, e.g.
    `CASE_NOTE_EMPLOYMENT_INFO_EMPLOYER_ENTITY_RESOLUTION`.

    The ER extractor collection shares this name with the composite-document
    collection, so a state's ER extractor binds one as its input document
    collection and the other as its extractor collection.
    """
    return (
        f"{first_order_extractor_collection_name}_{entity_group_name.upper()}"
        f"_{ENTITY_RESOLUTION_COLLECTION_NAME_SUFFIX}"
    )


@attr.define
class EntityResolutionDocumentCollectionConfig(DocumentCollectionConfig):
    """Configuration for an entity-resolution composite-document collection. See
    the module docstring.

    Everything below the two inputs is a derived override of a base class field:
    an ER collection is generated, never authored, so nothing about it is passed
    in beyond the pair it resolves.
    """

    # The fully-resolved first-order extractor config whose validated mentions the
    # composite documents are built from.
    first_order_config: LLMExtractorConfig = attr.ib(
        validator=attr.validators.instance_of(LLMExtractorConfig)
    )

    # The entity group on the first-order collection to resolve — one ER
    # collection per (first-order collection, entity group).
    entity_group: EntityGroupConfig = attr.ib(
        validator=attr.validators.instance_of(EntityGroupConfig)
    )

    state_code: StateCode = attr.ib(
        init=False, validator=attr.validators.instance_of(StateCode)
    )

    root_entity_id_type: DocumentRootEntityIdType = attr.ib(
        init=False, validator=attr.validators.in_(DocumentRootEntityIdType)
    )

    name: str = attr.ib(
        init=False,
        validator=[
            is_valid_unquoted_bq_identifier,
            attr_validators.is_upper_snake_case,
        ],
    )

    description: str = attr.ib(
        init=False, validator=recidiviz_attr_validators.is_meaningful_description
    )

    document_primary_key_columns: list[bigquery.SchemaField] = attr.ib(
        init=False, validator=attr_validators.is_list_of(bigquery.SchemaField)
    )

    other_metadata_columns: list[bigquery.SchemaField] = attr.ib(
        init=False, validator=attr_validators.is_list_of(bigquery.SchemaField)
    )

    other_document_generation_output_columns: list[bigquery.SchemaField] = attr.ib(
        init=False, validator=attr_validators.is_list_of(bigquery.SchemaField)
    )

    document_generation_query_template: str = attr.ib(
        init=False, validator=attr_validators.is_str
    )

    @state_code.default
    def _state_code(self) -> StateCode:
        return self.first_order_config.state_code

    @root_entity_id_type.default
    def _root_entity_id_type(self) -> DocumentRootEntityIdType:
        """The internal (person_id / staff_id) counterpart of the first-order
        collection's root entity, which is what the `__pre_resolution` views the
        composites are built from resolve every document to.
        """
        return (
            self.first_order_config.input_document_collection.root_entity_id_type.internal_id_type
        )

    @name.default
    def _name(self) -> str:
        return entity_resolution_collection_name(
            first_order_extractor_collection_name=self.first_order_extractor_collection_name,
            entity_group_name=self.entity_group.name,
        )

    @description.default
    def _description(self) -> str:
        return (
            f"Auto-generated entity-resolution composite documents for the "
            f"[{self.entity_group.name}] entity group of the "
            f"[{self.first_order_extractor_collection_name}] collection in "
            f"[{self.state_code.value}]. One composite document per root entity, "
            f"aggregating that entity's first-order mentions for the entity "
            f"resolution extractor to resolve."
        )

    @document_primary_key_columns.default
    def _document_primary_key_columns(self) -> list[bigquery.SchemaField]:
        """One composite document per root entity, so the root entity columns
        alone are the primary key — no document-specific columns.
        """
        return []

    @other_metadata_columns.default
    def _other_metadata_columns(self) -> list[bigquery.SchemaField]:
        return []

    @other_document_generation_output_columns.default
    def _other_document_generation_output_columns(self) -> list[bigquery.SchemaField]:
        """The composite-document generation query emits the entry→source map
        alongside the document text; the store carries it through to the
        temp-updates table for the ER upload extension to divert to the map table.
        """
        return [entry_source_map_schema_field()]

    @document_generation_query_template.default
    def _document_generation_query_template(self) -> str:
        """The composite-document generation query. A composite is always produced
        — length is not a generation concern (the per-document size guardrail skips
        oversized documents at extraction time).
        """
        return EntityResolutionCompositeDocumentQueryTemplateBuilder(
            root_entity_id_type=self.root_entity_id_type,
            entity_group=self.entity_group,
            pre_resolution_view_address=self.pre_resolution_view_address,
            source_document_contents_address=self.first_order_config.input_document_collection.document_contents_table_address,
        ).build_query_template()

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        if (
            self.entity_group
            not in self.first_order_config.extractor_collection.entity_groups
        ):
            raise ValueError(
                f"Entity group [{self.entity_group.name}] is not declared on "
                f"first-order collection "
                f"[{self.first_order_extractor_collection_name}]."
            )

    @property
    def first_order_extractor_collection_name(self) -> str:
        """Returns the name of the first-order extractor collection these
        composite documents resolve mentions for (e.g.
        `CASE_NOTE_EMPLOYMENT_INFO`). Distinct from the name of the document
        collection that extractor reads.
        """
        return self.first_order_config.extractor_collection.name

    # TODO(OBT-32175): Move this derivation to be defined where
    #  LLMExtractorPreResolutionResultsViewBuilder is defined, once that exists.
    @property
    def pre_resolution_view_address(self) -> BigQueryAddress:
        """Returns the address of the first-order `__pre_resolution` parsed view
        the composite documents are built from: the array-grain view for an array
        entity group, the doc-grain view for a top-level one.
        """
        table_id = self.first_order_extractor_collection_name.lower()
        if self.entity_group.source_array_field is not None:
            table_id = f"{table_id}_{self.entity_group.source_array_field.name}"
        return BigQueryAddress(
            dataset_id=document_extraction_pre_resolution_results_dataset_for_region(
                self.state_code
            ),
            table_id=table_id,
        )
