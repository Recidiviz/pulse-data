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
"""Configuration for document collections stored in GCS with metadata in BigQuery."""
from collections import defaultdict
from enum import Enum
from pathlib import Path
from types import ModuleType

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_attr_validators import (
    is_valid_unquoted_bq_identifier,
)
from recidiviz.big_query.big_query_utils import (
    BigQueryFieldMode,
    to_validated_schema_field,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
    PERSON_EXTERNAL_ID_COLUMN_NAME,
    PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME,
    PERSON_ID_COLUMN_NAME,
    STAFF_EXTERNAL_ID_COLUMN_NAME,
    STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME,
    STAFF_ID_COLUMN_NAME,
    UPLOAD_DATETIME_COLUMN_NAME,
    get_document_store_column_schema,
)
from recidiviz.ingest.direct import regions as default_regions_module
from recidiviz.utils.yaml_dict import YAMLDict

DOCUMENT_COLLECTIONS_SUBDIR = "document_collections"


def _state_collections_dir(state_code: StateCode, region_module: ModuleType) -> Path:
    """Returns the path to the document collections directory for a state."""
    if region_module.__file__ is None:
        raise ValueError(f"No file associated with {region_module}.")
    return (
        Path(region_module.__file__).parent
        / state_code.value.lower()
        / DOCUMENT_COLLECTIONS_SUBDIR
    )


class DocumentRootEntityIdType(Enum):
    """Enum representing the type of root entity ID used to associate documents
    with a root entity (StatePerson or StateStaff)."""

    PERSON_ID = "person_id"
    PERSON_EXTERNAL_ID = "person_external_id"
    STAFF_ID = "staff_id"
    STAFF_EXTERNAL_ID = "staff_external_id"


def _root_entity_schema_fields(
    root_entity_id_type: DocumentRootEntityIdType,
) -> list[bigquery.SchemaField]:
    """Returns the BQ SchemaFields for the given root entity ID type."""

    match root_entity_id_type:
        case DocumentRootEntityIdType.PERSON_ID:
            return [
                get_document_store_column_schema(PERSON_ID_COLUMN_NAME),
            ]
        case DocumentRootEntityIdType.PERSON_EXTERNAL_ID:
            return [
                get_document_store_column_schema(PERSON_EXTERNAL_ID_COLUMN_NAME),
                get_document_store_column_schema(PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME),
            ]
        case DocumentRootEntityIdType.STAFF_ID:
            return [
                get_document_store_column_schema(STAFF_ID_COLUMN_NAME),
            ]
        case DocumentRootEntityIdType.STAFF_EXTERNAL_ID:
            return [
                get_document_store_column_schema(STAFF_EXTERNAL_ID_COLUMN_NAME),
                get_document_store_column_schema(STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME),
            ]
        case _:
            raise ValueError(f"Unexpected root_entity_id_type: {root_entity_id_type}")


@attr.define
class DocumentCollectionConfig:
    """Configuration for a document collection. A document collection is a set of documents that share the same schema
    and are generated using the same SQL query. A document can be any text blob associated with a root entity.
    """

    # The state this document collection belongs to.
    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))

    # Name that uniquely identifies a collection of documents within a state.
    name: str = attr.ib(validator=is_valid_unquoted_bq_identifier)

    # Description providing more detail about the collection, its intended use, and any other relevant information.
    # TODO(#68777) enforce that description is meaningful and not a placeholder.
    description: str = attr.ib(validator=attr_validators.is_str)

    # Columns that uniquely identify a document within this collection. Includes root
    # entity columns (derived from root_entity_id_type) followed by document-specific
    # primary key columns. The combination of all primary key columns should remain
    # stable over time (i.e. if a document is updated in place, its primary key should
    # not change).
    primary_key_columns: list[bigquery.SchemaField] = attr.ib(
        validator=[
            attr_validators.is_non_empty_list,
            attr_validators.is_list_of(bigquery.SchemaField),
        ]
    )

    # Additional metadata columns outputted by the document_generation_query that are
    # not part of the primary key but provide useful context about documents.
    other_metadata_columns: list[bigquery.SchemaField] = attr.ib(
        validator=attr_validators.is_list_of(bigquery.SchemaField)
    )

    # The SQL query template used to generate documents in this collection.
    # TODO(#68777): define the expected format of the query template and enforce it.
    document_generation_query_template: str = attr.ib(validator=attr_validators.is_str)

    def __attrs_post_init__(self) -> None:
        col_names = [
            col.name for col in self.primary_key_columns + self.other_metadata_columns
        ]
        duplicate_names = {n for n in col_names if col_names.count(n) > 1}
        if duplicate_names:
            raise ValueError(
                f"Document collection [{self.name}] for [{self.state_code.value}] "
                f"has duplicate column names: {duplicate_names}."
            )

    def build_bq_metadata_schema(self) -> list[bigquery.SchemaField]:
        """Returns the full BigQuery schema for this collection's metadata table."""
        return [
            *self.primary_key_columns,
            *self.other_metadata_columns,
            get_document_store_column_schema(DOCUMENT_CONTENTS_ID_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_UPDATE_DATETIME_COLUMN_NAME),
            get_document_store_column_schema(UPLOAD_DATETIME_COLUMN_NAME),
        ]

    def build_bq_temp_table_schema(self) -> list[bigquery.SchemaField]:
        """Returns the BigQuery schema for the temporary table used during
        document processing. Includes document_text (not persisted to the final
        metadata table) and excludes upload_datetime (set at final write time).
        """
        return [
            *self.primary_key_columns,
            *self.other_metadata_columns,
            get_document_store_column_schema(DOCUMENT_CONTENTS_ID_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_TEXT_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_UPDATE_DATETIME_COLUMN_NAME),
        ]

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "DocumentCollectionConfig":
        """Loads a DocumentCollectionConfig from a YAML file."""
        yaml_dict = YAMLDict.from_path(str(yaml_path))

        root_entity_id_type = DocumentRootEntityIdType(
            yaml_dict.pop("root_entity_id_type", str)
        )
        root_entity_columns = _root_entity_schema_fields(root_entity_id_type)

        document_pk_columns = []
        other_metadata_columns = []
        for col_dict in yaml_dict.pop_dicts_optional("document_metadata_columns") or []:
            is_pk = col_dict.pop_optional("is_document_primary_key", bool) or False
            field_type = bigquery.enums.SqlTypeNames(col_dict.pop("field_type", str))
            if is_pk:
                schema_field = to_validated_schema_field(
                    field_name=col_dict.pop("name", str),
                    field_type=field_type,
                    description=col_dict.pop("description", str),
                    mode=BigQueryFieldMode.REQUIRED,
                )
                document_pk_columns.append(schema_field)
            else:
                schema_field = to_validated_schema_field(
                    field_name=col_dict.pop("name", str),
                    field_type=field_type,
                    description=col_dict.pop("description", str),
                    mode=BigQueryFieldMode.NULLABLE,
                )
                other_metadata_columns.append(schema_field)

        return cls(
            state_code=cls.file_path_to_state_code(yaml_path),
            name=cls.file_path_to_config_name(yaml_path),
            description=yaml_dict.pop("description", str),
            primary_key_columns=root_entity_columns + document_pk_columns,
            other_metadata_columns=other_metadata_columns,
            document_generation_query_template=yaml_dict.pop(
                "document_generation_query", str
            ),
        )

    @staticmethod
    def config_name_to_file_path(
        state_code: StateCode,
        collection_name: str,
        region_module: ModuleType | None = None,
    ) -> Path:
        """Returns the file path to the YAML config for a given collection name."""
        return (
            _state_collections_dir(
                state_code, region_module=region_module or default_regions_module
            )
            / f"{collection_name}.yaml"
        )

    @staticmethod
    def file_path_to_config_name(file_path: Path) -> str:
        """Returns the collection name for a given YAML config file path."""
        return file_path.stem

    @staticmethod
    def file_path_to_state_code(file_path: Path) -> StateCode:
        """Returns the state code for a given YAML config file path."""
        # Parent is document_collections/, grandparent is the state dir
        return StateCode(file_path.parent.parent.name.upper())


_DOCUMENT_COLLECTION_CONFIGS: dict[
    StateCode, dict[str, DocumentCollectionConfig]
] = defaultdict(dict)


def _load_config_from_file(yaml_path: Path) -> DocumentCollectionConfig:
    """Loads a single config and caches it in the global dict."""
    if not yaml_path.is_file():
        raise ValueError(f"No config file found at [{yaml_path}]")

    config_name = DocumentCollectionConfig.file_path_to_config_name(yaml_path)
    state_code = DocumentCollectionConfig.file_path_to_state_code(yaml_path)

    if config_name in _DOCUMENT_COLLECTION_CONFIGS[state_code]:
        return _DOCUMENT_COLLECTION_CONFIGS[state_code][config_name]

    config = DocumentCollectionConfig.from_yaml(yaml_path)
    _DOCUMENT_COLLECTION_CONFIGS[state_code][config_name] = config
    return config


def collect_document_collection_configs(
    state_code: StateCode,
    region_module: ModuleType | None = None,
) -> dict[str, DocumentCollectionConfig]:
    """Returns a map of document collection name to its configuration for all document
    collections defined for the given state code.
    """
    state_dir = _state_collections_dir(
        state_code, region_module=region_module or default_regions_module
    )
    if not state_dir.is_dir():
        return {}

    yaml_file_paths = list(state_dir.glob("*.yaml"))
    for yaml_path in yaml_file_paths:
        _load_config_from_file(yaml_path)

    return _DOCUMENT_COLLECTION_CONFIGS[state_code]


def get_document_collection_config(
    state_code: StateCode,
    collection_name: str,
    region_module: ModuleType | None = None,
) -> DocumentCollectionConfig:
    """Returns the DocumentCollectionConfig for the given collection name."""
    if collection_name in _DOCUMENT_COLLECTION_CONFIGS[state_code]:
        return _DOCUMENT_COLLECTION_CONFIGS[state_code][collection_name]

    return _load_config_from_file(
        DocumentCollectionConfig.config_name_to_file_path(
            state_code,
            collection_name,
            region_module=region_module or default_regions_module,
        ),
    )
