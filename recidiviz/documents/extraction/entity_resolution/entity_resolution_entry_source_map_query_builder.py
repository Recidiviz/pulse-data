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
"""Builds the query that hydrates one entity-resolution collection's
entry→source map table.
"""
import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common import attr_validators
from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    ENTRY_NUM_FIELD_NAME,
    ENTRY_SOURCE_MAP_COLUMN_NAME,
    SOURCE_ARRAY_INDEX_FIELD_NAME,
    SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME,
    SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME,
)
from recidiviz.documents.extraction.entity_resolution.entity_resolution_document_collection_config import (
    EntityResolutionDocumentCollectionConfig,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
)
from recidiviz.utils.string_formatting import fix_indent


@attr.define(frozen=True, kw_only=True)
class EntityResolutionEntrySourceMapQueryBuilder:
    """Builds the query that hydrates one entity-resolution
    collection's entry→source map table."""

    project_id: str = attr.ib(validator=attr_validators.is_str)
    """Project the map table and the mentions' source tables live in."""

    config: EntityResolutionDocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(EntityResolutionDocumentCollectionConfig)
    )
    """The entity-resolution composite-document collection whose map this builds."""

    def build_entry_source_map_query(
        self, document_generation_output_address: ProjectSpecificBigQueryAddress
    ) -> str:
        """Returns the query whose result is the collection's complete entry→source map
        table contents, read from |document_generation_output_address|. For a generation
        output holding two root entities with two entries each, it returns four rows.
        """
        root_id = self.config.root_entity_id_column_name
        return fix_indent(
            f"""
            SELECT
                generation_output.{root_id} AS {root_id},
                generation_output.{DOCUMENT_CONTENTS_ID_COLUMN_NAME} AS {DOCUMENT_CONTENTS_ID_COLUMN_NAME},
                entry.{ENTRY_NUM_FIELD_NAME} AS {ENTRY_NUM_FIELD_NAME},
                entry.{SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME} AS {SOURCE_DOCUMENT_CONTENTS_ID_FIELD_NAME},
                entry.{SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME} AS {SOURCE_DOCUMENT_UPDATE_DATETIME_FIELD_NAME},
                entry.{SOURCE_ARRAY_INDEX_FIELD_NAME} AS {SOURCE_ARRAY_INDEX_FIELD_NAME}
            FROM {document_generation_output_address.format_address_for_query()} generation_output,
            UNNEST(generation_output.{ENTRY_SOURCE_MAP_COLUMN_NAME}) AS entry
            """,
            indent_level=0,
        )
