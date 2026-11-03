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
"""Materializes a document collection's full generation query output to a
table, and for an entity-resolution collection rebuilds its entry→source map
table from that output.
"""

import logging

import attr

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common import attr_validators
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_hydrator import (
    EntityResolutionEntrySourceMapHydrator,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)
from recidiviz.documents.store.document_generation_query_builder import (
    DocumentGenerationQueryBuilder,
    build_document_generation_query_builder,
)
from recidiviz.documents.store.document_store_sandbox_context import (
    DocumentStoreSandboxContext,
)


@attr.define(frozen=True, kw_only=True)
class DocumentGenerationOutputMaterializer:
    """Materializes a document collection's full generation query output to a
    table by running its document_generation_query, and for an entity-resolution
    collection rebuilds its entry→source map table from that output.
    """

    config: DocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(DocumentCollectionConfig)
    )
    """The collection whose generation query runs."""

    project_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Project the generation query reads from and the output table lives in."""

    big_query_client: BigQueryClient = attr.ib()
    """Client the output write and the map write run through."""

    sandbox_context: DocumentStoreSandboxContext | None = attr.ib(
        validator=attr_validators.is_opt(DocumentStoreSandboxContext)
    )
    """Where the collections the generation query reads live, and where this
    collection's map table is written; None in production."""

    generation_query_builder: DocumentGenerationQueryBuilder = attr.ib(
        init=False,
        validator=attr.validators.instance_of(DocumentGenerationQueryBuilder),
    )
    """Builds the collection's generation query, with its reads scoped to
    sandbox_context."""

    @generation_query_builder.default
    def _build_generation_query_builder(self) -> DocumentGenerationQueryBuilder:
        return build_document_generation_query_builder(
            config=self.config,
            project_id=self.project_id,
            sandbox_context=self.sandbox_context,
        )

    @property
    def output_prefix(self) -> str | None:
        """Returns the sandbox dataset prefix the collection's tables are written
        under, or None in production.
        """
        if self.sandbox_context is None:
            return None
        return self.sandbox_context.output_prefix_for_writing(self.config.name)

    def materialize(
        self, *, output_address: BigQueryAddress
    ) -> ProjectSpecificBigQueryAddress:
        """Runs the collection's generation query into the table at
        |output_address|, replacing any existing table, and returns that table's
        project-specific address. For an entity-resolution collection, then
        rebuilds the map table from the output, so a composite's map changes even
        when its text does not.
        """
        project_specific_output_address = output_address.to_project_specific_address(
            self.project_id
        )
        self._write_generation_output(output_address=project_specific_output_address)
        if isinstance(self.config, EntityResolutionDocumentCollectionConfig):
            EntityResolutionEntrySourceMapHydrator(
                project_id=self.project_id,
                big_query_client=self.big_query_client,
                config=self.config,
                output_sandbox_prefix=self.output_prefix,
            ).run(project_specific_output_address)
        return project_specific_output_address

    def _write_generation_output(
        self, *, output_address: ProjectSpecificBigQueryAddress
    ) -> None:
        """Runs the collection's generation query into the table at
        |output_address|, replacing any existing table.
        """
        logging.info(
            "Materializing generation output for collection [%s] to [%s]",
            self.config.name,
            output_address.to_str(),
        )
        self.big_query_client.create_table_from_query(
            address=output_address.to_project_agnostic_address(),
            query=self.generation_query_builder.build_query(),
            use_query_cache=False,
            overwrite=True,
        )
