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
"""Builds the query that generates a document collection's documents.

The generation query has two parts: the collection's own generation SQL (which
varies by collection type — an authored template for a base collection, a composite
query built from other document-store tables for entity resolution) and an invariant
wrapper that adds the framework-computed document_contents_id and filters out
null-text rows. The builder owns the wrapper; the collection's own generation SQL is
passed in as the inner query. The factory in this module picks that inner query by
collection type — knowing which tables each type reads and how they are scoped for a
sandbox — and hands it to the builder.
"""

import string

import attr

from recidiviz.big_query.big_query_query_builder import PROJECT_ID_KEY
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    EntityResolutionCompositeDocumentQueryTemplateBuilder,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)
from recidiviz.utils.string import StrictStringFormatter


@attr.define(frozen=True, kw_only=True)
class DocumentGenerationQueryBuilder:
    """Builds the full generation query for a document collection: the collection's
    own generation SQL, passed in as the inner query, wrapped with the
    framework-computed document_contents_id and a null-text filter."""

    project_id: str = attr.ib(validator=attr_validators.is_str)
    config: DocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(DocumentCollectionConfig)
    )
    inner_query: str = attr.ib(validator=attr_validators.is_str)
    """The collection's own generation SQL template (referencing {project_id}), whose
    output columns the wrapper passes through alongside the computed
    document_contents_id."""

    @inner_query.validator
    def _check_inner_query_references_project_id(
        self, _attribute: "attr.Attribute", value: str
    ) -> None:
        field_names = {
            field_name for _, field_name, _, _ in string.Formatter().parse(value)
        }
        if PROJECT_ID_KEY not in field_names:
            raise ValueError(
                f"inner_query must reference the [{{{PROJECT_ID_KEY}}}] "
                f"template key, which build_query interpolates the project into. "
                f"Received: [{value}]"
            )

    def build_query(self) -> str:
        """Returns the full generation query: every column the collection's
        generation SQL outputs, plus document_contents_id computed from
        document_text, with null-text rows filtered out, with the project_id
        interpolated.
        """
        return StrictStringFormatter().format(
            self._wrap_inner_query(self.inner_query),
            **{PROJECT_ID_KEY: self.project_id},
        )

    def _wrap_inner_query(self, inner_query: str) -> str:
        """Returns |inner_query| wrapped to add the framework-computed
        document_contents_id and drop null-text rows. Invariant across collection
        types."""
        passthrough_columns = [
            col.name
            for col in self.config.build_bq_document_generation_output_schema()
            if col.name != DOCUMENT_CONTENTS_ID_COLUMN_NAME
        ]
        return f"""
SELECT
    {self._document_contents_id_sql_clause(self.config.state_code)} AS {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
    {list_to_query_string(passthrough_columns)}
FROM ({inner_query})
WHERE {DOCUMENT_TEXT_COLUMN_NAME} IS NOT NULL"""

    @staticmethod
    def _document_contents_id_sql_clause(state_code: StateCode) -> str:
        """Returns the SQL that computes document_contents_id for |state_code|."""
        return f"TO_HEX(SHA256(CONCAT('{state_code.value}', '|', {DOCUMENT_TEXT_COLUMN_NAME})))"


def _build_entity_resolution_inner_query(
    *,
    config: EntityResolutionDocumentCollectionConfig,
    sandbox_context: DocumentStoreSandboxContext | None,
) -> str:
    """Returns the composite-document generation SQL for an entity-resolution
    collection. It reads two tables that can live in different places: the
    `__pre_resolution` results of the first-order extractor collection, and the document
    contents of the first-order document collection that extractor reads — each scoped to
    its own prefix in |sandbox_context| (None in production).
    """
    first_order_document_collection = (
        config.first_order_config.input_document_collection
    )
    return EntityResolutionCompositeDocumentQueryTemplateBuilder(
        root_entity_id_type=config.root_entity_id_type,
        entity_group=config.entity_group,
        pre_resolution_view_materialized_address=config.pre_resolution_view_materialized_address(
            sandbox_dataset_prefix=(
                sandbox_context.read_prefix_for_extractor_collection(
                    config.first_order_extractor_collection_name
                )
                if sandbox_context
                else None
            )
        ),
        source_document_contents_address=first_order_document_collection.document_contents_table_address(
            sandbox_dataset_prefix=(
                sandbox_context.source_read_prefix_for_document_collection(
                    first_order_document_collection.name
                )
                if sandbox_context
                else None
            )
        ),
    ).build_query_template()


def build_document_generation_query_builder(
    *,
    config: DocumentCollectionConfig,
    project_id: str,
    sandbox_context: DocumentStoreSandboxContext | None,
) -> DocumentGenerationQueryBuilder:
    """Returns the generation query builder for |config|, scoping any sandbox reads its
    generation SQL makes to |sandbox| (None in production). An entity-resolution
    collection's inner query reads the first-order `__pre_resolution` results and source
    contents at the sandbox's prefixes; any other collection reads its authored template,
    whose SQL reads prefix-invariant source data and so is unaffected by the sandbox.
    """
    if isinstance(config, EntityResolutionDocumentCollectionConfig):
        inner_query = _build_entity_resolution_inner_query(
            config=config, sandbox_context=sandbox_context
        )
    else:
        inner_query = config.authored_generation_query_template
    return DocumentGenerationQueryBuilder(
        project_id=project_id, config=config, inner_query=inner_query
    )
