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
"""Configuration for document collections stored in GCS with metadata in BigQuery."""
import os
from enum import Enum
from functools import cache
from pathlib import Path

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.yaml_dict import YAMLDict

DOCUMENT_ID_COLUMN_NAME = "document_id"
DOCUMENT_UPDATE_DATETIME_COLUMN_NAME = "document_update_datetime"
UPLOAD_DATETIME_COLUMN_NAME = "upload_datetime"
DOCUMENT_STORE_METADATA_DATASET_ID = "document_store_metadata"

# Path to the config directory relative to this file
_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "config")
_DOCUMENT_COLLECTIONS_DIR = os.path.join(_CONFIG_DIR, "document_collections")


class DocumentRootEntityIdType(Enum):
    """Enum representing the type of root entity ID used to associate documents."""

    PERSON_ID = "PERSON_ID"
    PERSON_EXTERNAL_ID = "PERSON_EXTERNAL_ID"
    STAFF_ID = "STAFF_ID"
    STAFF_EXTERNAL_ID = "STAFF_EXTERNAL_ID"

    def column_schemas(self) -> list[bigquery.SchemaField]:
        """Returns the BigQuery schema fields for this root entity type."""
        match self:
            case DocumentRootEntityIdType.PERSON_ID:
                return [
                    bigquery.SchemaField(
                        name="person_id",
                        field_type=bigquery.enums.SqlTypeNames.INT64.value,
                        mode="NULLABLE",
                    )
                ]
            case DocumentRootEntityIdType.PERSON_EXTERNAL_ID:
                return [
                    bigquery.SchemaField(
                        name="person_external_id",
                        field_type=bigquery.enums.SqlTypeNames.STRING.value,
                        mode="NULLABLE",
                    ),
                    bigquery.SchemaField(
                        name="person_external_id_type",
                        field_type=bigquery.enums.SqlTypeNames.STRING.value,
                        mode="NULLABLE",
                    ),
                ]
            case DocumentRootEntityIdType.STAFF_ID:
                return [
                    bigquery.SchemaField(
                        name="staff_id",
                        field_type=bigquery.enums.SqlTypeNames.INT64.value,
                        mode="NULLABLE",
                    )
                ]
            case DocumentRootEntityIdType.STAFF_EXTERNAL_ID:
                return [
                    bigquery.SchemaField(
                        name="staff_external_id",
                        field_type=bigquery.enums.SqlTypeNames.STRING.value,
                        mode="NULLABLE",
                    ),
                    bigquery.SchemaField(
                        name="staff_external_id_type",
                        field_type=bigquery.enums.SqlTypeNames.STRING.value,
                        mode="NULLABLE",
                    ),
                ]
            case _:
                raise NotImplementedError(
                    f"column_schemas not implemented for {self.value}"
                )


VALID_BQ_TYPES = frozenset(
    {
        "STRING",
        "INT64",
        "FLOAT64",
        "BOOLEAN",
        "TIMESTAMP",
        "DATE",
        "DATETIME",
    }
)


# TODO(#61718): Add a way to have a meta collection that pulls from any table
#  that already has document_ids in it and associates metadata to those
#  document ids.
@attr.define
class DocumentCollectionConfig:
    """Configuration for a document collection - a group of GCS docs and associated
    metadata that are all the same "shape" / generated from the same data source.
    """

    # If set, this is a state-specific collection
    state_code: StateCode | None

    # Name that uniquely identifies a collection of documents who are generated via the
    # same process / logic and which have shared metadata structure.
    #  Examples:
    #    US_XX_CASE_NOTES
    #    US_YY_CONTACT_NOTES
    #    PERSON_LEVEL_CASE_NOTES
    #
    # If this document store collection is state-specific (state_code is nonnull), then
    # this must start with the state code (e.g. US_XX_*).
    collection_name: str

    # Description providing more detail about the contents of each document
    collection_description: str

    # Enum that tells us what type of id is used to associate each document with a root
    #  entity. Used to derive the column(s) that stores the root entity id info in the
    #  metadata table.
    root_entity_type: DocumentRootEntityIdType

    # The schemas for the columns in the document metadata table which can be used along
    # with uniquely identify the same document. If a document changes over time (e.g.
    # if a case note is edited in place), the primary key will remain the same.
    other_primary_key_column_schemas: list[bigquery.SchemaField]

    # Additional metadata columns that are stored alongside each document but are not
    # part of the primary key. These columns capture document properties like note_type,
    # contact_mode, etc. that are useful for filtering/analysis but don't identify the doc.
    additional_metadata_column_schemas: list[bigquery.SchemaField]

    # The query template that can be used to list all documents we want to store
    #  (does not do any filtering to understand what is already stored).
    # This template may contain {project_id} which will be substituted at query time.
    # Must output columns for: root entity, primary key columns, additional metadata columns,
    # document_text, and document_update_datetime.
    document_generation_query_template: str

    def __attrs_post_init__(self) -> None:
        if self.state_code is not None:
            expected_prefix = f"{self.state_code.value}_"
            if not self.collection_name.startswith(expected_prefix):
                raise ValueError(
                    f"State-specific collection name must start with state code prefix. "
                    f"Expected '{expected_prefix}', got '{self.collection_name}'"
                )

    def metadata_table_address(
        self, dataset_override: str | None = None
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=dataset_override or DOCUMENT_STORE_METADATA_DATASET_ID,
            table_id=f"{self.collection_name.lower()}_metadata",
        )

    def metadata_table_schema(self) -> list[bigquery.SchemaField]:
        return [
            *self.root_entity_type.column_schemas(),
            *self.other_primary_key_column_schemas,
            *self.additional_metadata_column_schemas,
            bigquery.SchemaField(
                name=DOCUMENT_ID_COLUMN_NAME,
                field_type=bigquery.enums.SqlTypeNames.STRING.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
                field_type=bigquery.enums.SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
            ),
            bigquery.SchemaField(
                name=UPLOAD_DATETIME_COLUMN_NAME,
                field_type=bigquery.enums.SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
            ),
        ]

    def build_document_generation_query(self, project_id: str) -> str:
        """Builds the document generation query with the given project_id."""
        query_builder = BigQueryQueryBuilder(
            parent_address_overrides=None,
            parent_address_formatter_provider=None,
        )
        return query_builder.build_query(
            project_id=project_id,
            query_template=self.document_generation_query_template,
            query_format_kwargs={},
        )

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "DocumentCollectionConfig":
        """Loads a DocumentCollectionConfig from a YAML file.

        The collection name and state code are derived from the file path:
        - Files in state directories (e.g., us_ix/case_notes.yaml) produce
          state-specific collections with names like US_IX_CASE_NOTES
        - Files in the root directory produce state-agnostic collections
          with names derived from the filename (e.g., person_level_case_notes.yaml
          becomes PERSON_LEVEL_CASE_NOTES)
        """
        path = Path(yaml_path)
        yaml_dict = YAMLDict.from_path(yaml_path)

        # Derive state_code and collection_name from file path
        parent_dir_name = path.parent.name
        file_stem = path.stem.upper()

        # Check if parent directory is a state code
        state_code: StateCode | None = None
        if parent_dir_name.upper().startswith("US_"):
            state_code = StateCode(parent_dir_name.upper())
            collection_name = f"{state_code.value}_{file_stem}"
        else:
            # State-agnostic collection
            collection_name = file_stem

        # Parse root entity type
        root_entity_type = DocumentRootEntityIdType(
            yaml_dict.pop("root_entity_type", str)
        )

        # Parse primary key columns
        primary_key_columns: list[bigquery.SchemaField] = []
        for col_yaml in yaml_dict.pop_dicts_optional("primary_key_columns") or []:
            description = col_yaml.pop_optional("description", str) or ""
            field_type = col_yaml.pop("type", str)
            if field_type not in VALID_BQ_TYPES:
                raise ValueError(
                    f"Invalid BigQuery type '{field_type}'. "
                    f"Must be one of: {', '.join(sorted(VALID_BQ_TYPES))}"
                )
            primary_key_columns.append(
                bigquery.SchemaField(
                    name=col_yaml.pop("name", str),
                    field_type=field_type,
                    mode="NULLABLE",
                    description=description,
                )
            )

        # Parse additional metadata columns
        additional_metadata_columns: list[bigquery.SchemaField] = []
        for col_yaml in (
            yaml_dict.pop_dicts_optional("additional_metadata_columns") or []
        ):
            description = col_yaml.pop_optional("description", str) or ""
            field_type = col_yaml.pop("type", str)
            if field_type not in VALID_BQ_TYPES:
                raise ValueError(
                    f"Invalid BigQuery type '{field_type}'. "
                    f"Must be one of: {', '.join(sorted(VALID_BQ_TYPES))}"
                )
            additional_metadata_columns.append(
                bigquery.SchemaField(
                    name=col_yaml.pop("name", str),
                    field_type=field_type,
                    mode="NULLABLE",
                    description=description,
                )
            )

        return cls(
            state_code=state_code,
            collection_name=collection_name,
            collection_description=yaml_dict.pop("collection_description", str),
            root_entity_type=root_entity_type,
            other_primary_key_column_schemas=primary_key_columns,
            additional_metadata_column_schemas=additional_metadata_columns,
            document_generation_query_template=yaml_dict.pop(
                "document_generation_query", str
            ),
        )


def _discover_yaml_files(base_dir: str) -> list[str]:
    """Discovers all YAML files in a directory and its subdirectories."""
    yaml_files: list[str] = []
    for root, _dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                yaml_files.append(os.path.join(root, file))
    return yaml_files


# TODO(#61720): For each of these, build a source table config so the metadata
#  table gets auto-created at deploy time.
@cache
def collect_document_collection_configs() -> dict[str, DocumentCollectionConfig]:
    """Returns a map of document collection name to its configuration.

    Collection names are unique identifiers like US_IX_CASE_NOTES or
    PERSON_LEVEL_CASE_NOTES.
    """
    configs: dict[str, DocumentCollectionConfig] = {}

    yaml_files = _discover_yaml_files(_DOCUMENT_COLLECTIONS_DIR)

    for yaml_path in yaml_files:
        config = DocumentCollectionConfig.from_yaml(yaml_path)

        if config.collection_name in configs:
            raise ValueError(
                f"Duplicate document collection name '{config.collection_name}' "
                f"found in {yaml_path}"
            )

        configs[config.collection_name] = config

    return configs


def get_document_collection_config(collection_name: str) -> DocumentCollectionConfig:
    """Returns the DocumentCollectionConfig for the given collection name.

    Raises KeyError if the collection is not found.
    """
    configs = collect_document_collection_configs()
    if collection_name not in configs:
        raise KeyError(
            f"Document collection '{collection_name}' not found. "
            f"Available collections: {list(configs.keys())}"
        )
    return configs[collection_name]
