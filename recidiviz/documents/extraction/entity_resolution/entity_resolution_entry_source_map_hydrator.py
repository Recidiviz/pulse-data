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
"""Replaces one entity-resolution collection's entry→source map table from a
run's full composite-document generation output."""
import logging

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common import attr_validators
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_query_builder import (
    EntityResolutionEntrySourceMapQueryBuilder,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)


@attr.define(frozen=True, kw_only=True)
class EntityResolutionEntrySourceMapHydrator:
    """Replaces one entity-resolution collection's entry→source map table from a
    run's full composite-document generation output."""

    project_id: str = attr.ib(validator=attr_validators.is_str)
    """Project the map table and the generation output live in."""

    big_query_client: BigQueryClient = attr.ib()
    """Client the map write runs through."""

    config: EntityResolutionDocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(EntityResolutionDocumentCollectionConfig)
    )
    """The entity-resolution composite-document collection whose map this hydrates."""

    def run(
        self, document_generation_output_address: ProjectSpecificBigQueryAddress
    ) -> None:
        """Rebuilds the map table from |document_generation_output_address|.

        The write is a write-truncate over the full generation output, so a root
        entity that leaves the generation output loses its rows with no separate
        delete pass.
        """
        query_builder = EntityResolutionEntrySourceMapQueryBuilder(
            project_id=self.project_id,
            config=self.config,
        )

        map_table_address = self.config.entry_source_map_table_address
        logging.info(
            "Replacing entry→source map table [%s] for collection [%s] from [%s]",
            map_table_address.to_str(),
            self.config.name,
            document_generation_output_address.to_str(),
        )
        self.big_query_client.create_table_from_query(
            address=map_table_address,
            query=query_builder.build_entry_source_map_query(
                document_generation_output_address
            ),
            use_query_cache=False,
            overwrite=True,
            clustering_fields=[self.config.root_entity_id_column_name],
            output_schema=EntityResolutionEntrySourceMapBQTable.schema(
                root_entity_id_type=self.config.root_entity_id_type
            ),
        )
