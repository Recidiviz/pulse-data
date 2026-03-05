# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Base and concrete extractor classes for document extraction."""
import abc
from typing import Any

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode


@attr.define(frozen=True)
class DocumentExtractorMetadata(abc.ABC):
    """Base class for extraction configurations.

    All extractors are state-specific. The input document collection must either be
    state-specific (matching this state) or have a state_code column that can be
    used to filter.
    """

    state_code: StateCode

    # The human-readable ID of this extractor which may remain constant over time, even
    # if the extraction methodology changes (e.g. US_IX_EMPLOYMENT)
    extractor_id: str

    # The name of the extractor collection this belongs to. Each state can only have
    # one extractor instance per collection. The full collection object can be looked
    # up via get_extractor_collection(collection_name).
    collection_name: str

    description: str

    # The DocumentCollectionConfig.collection_name corresponding to the collection of
    # documents that should be processed by this prompt.
    input_document_collection_name: str

    @classmethod
    @abc.abstractmethod
    def metadata_table_address(
        cls, sandbox_dataset_prefix: str | None
    ) -> BigQueryAddress:
        """The address of the metadata table for this type of extractor."""

    @abc.abstractmethod
    def extractor_version_id(self) -> str:
        """ID unique to this extractor which will change if the anything about the
        extraction method changes.
        """

    @abc.abstractmethod
    def as_metadata_row(self) -> dict[str, Any]:
        """Row that will be written to a metadata table about extractors
        every time anything about the collection changes (e.g. description or schema
        changes).
        """
