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

import json

import attr

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    get_document_collection_config,
)


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

    def serialize(self) -> str:
        return json.dumps(
            {
                "collection_name": self.collection_name,
                "temp_new_document_contents_table_address": self.temp_new_document_contents_table_address.to_str(),
                "batch_number": self.batch_number,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "DocumentUploadBatch":
        data = json.loads(json_str)
        return DocumentUploadBatch(
            collection_name=data["collection_name"],
            temp_new_document_contents_table_address=ProjectSpecificBigQueryAddress.from_str(
                data["temp_new_document_contents_table_address"]
            ),
            batch_number=data["batch_number"],
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

    def serialize(self) -> str:
        return json.dumps(
            {
                "state_code": self.state_code.value,
                "collection_name": self.collection_name,
                "temp_document_metadata_updates_address": self.temp_document_metadata_updates_address.to_str(),
                "temp_new_document_contents_address": self.temp_new_document_contents_address.to_str(),
                "num_new_document_contents_rows": self.num_new_document_contents_rows,
                "num_document_metadata_updates_rows": self.num_document_metadata_updates_rows,
            }
        )

    @staticmethod
    def deserialize(json_str: str) -> "SingleCollectionDocumentDiscoveryResult":
        data = json.loads(json_str)
        return SingleCollectionDocumentDiscoveryResult(
            state_code=StateCode(data["state_code"]),
            collection_name=data["collection_name"],
            temp_document_metadata_updates_address=ProjectSpecificBigQueryAddress.from_str(
                data["temp_document_metadata_updates_address"]
            ),
            temp_new_document_contents_address=ProjectSpecificBigQueryAddress.from_str(
                data["temp_new_document_contents_address"]
            ),
            num_new_document_contents_rows=data["num_new_document_contents_rows"],
            num_document_metadata_updates_rows=data[
                "num_document_metadata_updates_rows"
            ],
        )
