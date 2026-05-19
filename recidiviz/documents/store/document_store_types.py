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
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    get_document_collection_config,
)
from recidiviz.utils.types import assert_type


@attr.define(frozen=True, kw_only=True)
class DocumentUploadBatch:
    """A batch of documents within a temp table to process. The table at
    |temp_new_document_contents_table_address| is expected to have a
    document_upload_batch_number column assigned by cumulative byte size."""

    collection_name: str = attr.ib(validator=attr_validators.is_str)
    temp_new_document_contents_table_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    batch_number: int = attr.ib(validator=attr_validators.is_int)

    def to_dict(self) -> dict[str, str | int]:
        return {
            "collection_name": self.collection_name,
            "temp_new_document_contents_table_address": self.temp_new_document_contents_table_address.to_str(),
            "batch_number": self.batch_number,
        }

    @staticmethod
    def from_dict(data: dict[str, str | int]) -> "DocumentUploadBatch":
        return DocumentUploadBatch(
            collection_name=assert_type(data["collection_name"], str),
            temp_new_document_contents_table_address=ProjectSpecificBigQueryAddress.from_str(
                assert_type(data["temp_new_document_contents_table_address"], str)
            ),
            batch_number=assert_type(data["batch_number"], int),
        )


@attr.define(frozen=True, kw_only=True)
class SingleCollectionDocumentDiscoveryResult:
    """Result of running document discovery for a single collection."""

    state_code: StateCode = attr.ib(validator=recidiviz_attr_validators.is_state_code)
    collection_name: str = attr.ib(validator=attr_validators.is_str)
    temp_document_metadata_updates_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    temp_new_document_contents_address: ProjectSpecificBigQueryAddress = attr.ib(
        validator=attr.validators.instance_of(ProjectSpecificBigQueryAddress)
    )
    num_new_document_contents_rows: int = attr.ib(validator=attr_validators.is_int)
    num_document_metadata_updates_rows: int = attr.ib(validator=attr_validators.is_int)

    @property
    def config(self) -> DocumentCollectionConfig:
        return get_document_collection_config(self.state_code, self.collection_name)

    def to_dict(self) -> dict[str, str | int]:
        return {
            "state_code": self.state_code.value,
            "collection_name": self.collection_name,
            "temp_document_metadata_updates_address": self.temp_document_metadata_updates_address.to_str(),
            "temp_new_document_contents_address": self.temp_new_document_contents_address.to_str(),
            "num_new_document_contents_rows": self.num_new_document_contents_rows,
            "num_document_metadata_updates_rows": self.num_document_metadata_updates_rows,
        }

    @staticmethod
    def from_dict(
        data: dict[str, str | int]
    ) -> "SingleCollectionDocumentDiscoveryResult":
        return SingleCollectionDocumentDiscoveryResult(
            state_code=StateCode(assert_type(data["state_code"], str)),
            collection_name=assert_type(data["collection_name"], str),
            temp_document_metadata_updates_address=ProjectSpecificBigQueryAddress.from_str(
                assert_type(data["temp_document_metadata_updates_address"], str)
            ),
            temp_new_document_contents_address=ProjectSpecificBigQueryAddress.from_str(
                assert_type(data["temp_new_document_contents_address"], str)
            ),
            num_new_document_contents_rows=assert_type(
                data["num_new_document_contents_rows"], int
            ),
            num_document_metadata_updates_rows=assert_type(
                data["num_document_metadata_updates_rows"], int
            ),
        )
