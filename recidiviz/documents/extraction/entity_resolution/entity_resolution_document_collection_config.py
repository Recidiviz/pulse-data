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
from recidiviz.common.descriptor import Descriptor
from recidiviz.documents.extraction.entity_resolution.entity_resolution_collection_names import (
    entity_resolution_collection_name,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    entry_source_map_schema_field,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.extraction.models.llm_extractor_collection_config import (
    EntityGroupConfig,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.documents.extraction.views.llm_extractor_array_level_results_view_builders import (
    LLMExtractorPreResolutionArrayFieldResultsViewBuilder,
)
from recidiviz.documents.extraction.views.llm_extractor_doc_level_results_view_builders import (
    LLMExtractorPreResolutionResultsViewBuilder,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    DocumentRootEntityIdType,
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

    # An ER collection's generation query is built from other document-store tables
    # (see build_document_generation_query_builder), so it has no single authored
    # template — this stays None.
    _authored_document_generation_query_template: str | None = attr.ib(
        init=False, default=None, validator=attr_validators.is_opt_str
    )

    document_descriptor: Descriptor = attr.ib(
        init=False, validator=attr.validators.instance_of(Descriptor)
    )

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # TODO(OBT-42680) This is kind of hacky and we should think if there is a
        # better approach we can take here.
        if self._authored_document_generation_query_template is not None:
            raise ValueError(
                "ER composite-document collections are generated, not authored, "
                "so they have no authored document-generation query template."
            )
        if (
            self.entity_group
            not in self.first_order_config.extractor_collection.entity_groups
        ):
            raise ValueError(
                f"Entity group [{self.entity_group.name}] is not declared on "
                f"first-order collection "
                f"[{self.first_order_extractor_collection_name}]."
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

    @document_descriptor.default
    def _document_descriptor(self) -> Descriptor:
        # The documents in an ER collection are composite documents. This descriptor
        # names the collection's own documents; the ER clustering prompt names the
        # *source* documents separately, from the first-order collection's
        # `document_descriptor`.
        return Descriptor(singular="composite document", plural="composite documents")

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
        alongside the document text. This value ultimately gets written to the
        *entry_source_map_table as part of the document discovery flow.
        """
        return [entry_source_map_schema_field()]

    @property
    def first_order_extractor_collection_name(self) -> str:
        """Returns the name of the first-order extractor collection these
        composite documents resolve mentions for (e.g.
        `CASE_NOTE_EMPLOYMENT_INFO`). Distinct from the name of the document
        collection that extractor reads.
        """
        return self.first_order_config.extractor_collection.name

    def pre_resolution_view_materialized_address(
        self, *, sandbox_dataset_prefix: str | None
    ) -> BigQueryAddress:
        """Returns the address the composite documents are built from: the
        materialized table behind the first-order "__pre_resolution" parsed view — the
        array-level view for an array entity group, the doc-level view for a top-level
        one. Reading the materialized table means composite-document generation reads an
        already-computed table instead of re-running the whole parse. Scoped to
        |sandbox_dataset_prefix| when the first-order views were deployed under
        a sandbox (None in production).
        """
        if (source_array_field := self.entity_group.source_array_field) is not None:
            return LLMExtractorPreResolutionArrayFieldResultsViewBuilder.materialized_address_for_array_field(
                config=self.first_order_config,
                array_field=source_array_field,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
            )
        return (
            LLMExtractorPreResolutionResultsViewBuilder.materialized_address_for_config(
                config=self.first_order_config,
                sandbox_dataset_prefix=sandbox_dataset_prefix,
            )
        )

    def entry_source_map_table_address(
        self, *, sandbox_dataset_prefix: str | None
    ) -> BigQueryAddress:
        """Returns the project-agnostic address of this collection's entry→source
        map table, which maps each composite-document entry back to the first-order
        mention occurrence it was rendered from, scoped to |sandbox_dataset_prefix|
        when running under a sandbox (None in production).
        """
        return EntityResolutionEntrySourceMapBQTable.address(
            state_code=self.state_code,
            first_order_extractor_collection_name=self.first_order_extractor_collection_name,
            entity_group_name=self.entity_group.name,
            sandbox_prefix=sandbox_dataset_prefix,
        )

    @property
    def entry_source_map_table_description(self) -> str:
        """Returns the description of this collection's entry→source map table."""
        return EntityResolutionEntrySourceMapBQTable.description(
            state_code=self.state_code,
            first_order_extractor_collection_name=self.first_order_extractor_collection_name,
            entity_group_name=self.entity_group.name,
        )
