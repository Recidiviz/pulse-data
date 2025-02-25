# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Class for holding BigQuery source tables"""
from functools import cached_property

import attr
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableConfig,
    SourceTableCouldNotGenerateError,
    SourceTableLabel,
)


@attr.s(auto_attribs=True)
class SourceTableRepository:
    """The SourceTableRepository aggregates definitions of all source tables in our view graph."""

    source_table_collections: list[SourceTableCollection] = attr.ib(factory=list)

    @cached_property
    def source_tables(self) -> dict[BigQueryAddress, SourceTableConfig]:
        return {
            source_table.address: source_table
            for collection in self.source_table_collections
            for source_table in collection.source_tables
        }

    def get_collections_with_labels(
        self, labels: list[SourceTableLabel]
    ) -> list[SourceTableCollection]:
        return [
            collection
            for collection in self.source_table_collections
            if all(collection.has_label(label) for label in labels)
        ]

    def get_collections_with_dataset_id(
        self, dataset_id: str
    ) -> list[SourceTableCollection]:
        return [
            collection
            for collection in self.source_table_collections
            if collection.dataset_id == dataset_id
        ]

    @cached_property
    def source_datasets(self) -> list[str]:
        return sorted({a.dataset_id for a in self.source_tables})

    def get_collection_with_labels(
        self, labels: list[SourceTableLabel]
    ) -> SourceTableCollection:
        return one(self.get_collections_with_labels(labels=labels))

    def get_collection_with_dataset_id(self, dataset_id: str) -> SourceTableCollection:
        return one(self.get_collections_with_dataset_id(dataset_id=dataset_id))

    def collections_labelled_with(
        self, label_type: type[SourceTableLabel]
    ) -> list[SourceTableCollection]:
        return [
            collection
            for collection in self.source_table_collections
            if any(isinstance(label, label_type) for label in collection.labels)
        ]

    def build_config(self, address: BigQueryAddress) -> SourceTableConfig:
        try:
            return self.source_tables[address]
        except KeyError as e:
            raise SourceTableCouldNotGenerateError from e
