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
"""Data classes used across the document store module."""
import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common import attr_validators
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
)


@attr.define(frozen=True, kw_only=True)
class DocumentBatchRange:
    """A range of documents within a temp table to process. The table at
    |temp_new_document_contents_table_address| is expected to have both document_contents_id and
    sequence_num columns."""

    collection_name: str = attr.ib(validator=attr_validators.is_str)
    temp_new_document_contents_table_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    start_sequence_num_inclusive: int = attr.ib(validator=attr_validators.is_int)
    end_sequence_num_exclusive: int = attr.ib(validator=attr_validators.is_int)


@attr.define(frozen=True, kw_only=True)
class SingleCollectionDocumentDiscoveryResult:
    """Result of running document discovery for a single collection."""

    config: DocumentCollectionConfig = attr.ib(
        validator=attr.validators.instance_of(DocumentCollectionConfig)
    )
    temp_document_metadata_updates_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    temp_new_document_contents_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    num_new_document_contents_rows: int = attr.ib(validator=attr_validators.is_int)
    num_document_metadata_updates_rows: int = attr.ib(validator=attr_validators.is_int)

    @property
    def collection_name(self) -> str:
        return self.config.name
