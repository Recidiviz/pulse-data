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

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_attr_validators import (
    is_valid_unquoted_bq_identifier,
)
from recidiviz.big_query.big_query_utils import (
    BigQueryFieldMode,
    make_bq_compatible_identifier,
    to_validated_schema_field,
)
from recidiviz.common import attr_validators, recidiviz_attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.common.descriptor import Descriptor
from recidiviz.documents import config as default_config_module
from recidiviz.documents.dataset_config import (
    document_contents_dataset_for_region,
    document_store_metadata_dataset_for_region,
    document_store_temp_dataset_for_region,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
    PERSON_EXTERNAL_ID_COLUMN_NAME,
    PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME,
    PERSON_ID_COLUMN_NAME,
    ROW_CREATE_DATETIME_COLUMN_NAME,
    STAFF_EXTERNAL_ID_COLUMN_NAME,
    STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME,
    STAFF_ID_COLUMN_NAME,
    get_document_store_column_schema,
)
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStatePersonExternalId,
    NormalizedStateStaffExternalId,
)
from recidiviz.utils.yaml_dict import YAMLDict

DOCUMENT_COLLECTIONS_SUBDIR = "document_collections"

TEMP_DOCUMENT_GENERATION_OUTPUT_TABLE_ID_PREFIX = "temp_document_generation_output_"
TEMP_METADATA_UPDATES_TABLE_ID_PREFIX = "temp_document_metadata_updates_"
TEMP_NEW_DOCUMENT_CONTENTS_TABLE_ID_PREFIX = "temp_new_document_contents_"


def _state_collections_dir(state_code: StateCode, config_module: ModuleType) -> Path:
    """Returns the path to the document collections directory for a state."""
    if config_module.__file__ is None:
        raise ValueError(f"No file associated with {config_module}.")
    return (
        Path(config_module.__file__).parent
        / DOCUMENT_COLLECTIONS_SUBDIR
        / state_code.value.lower()
    )


class DocumentRootEntityIdType(Enum):
    """Enum representing the type of root entity ID used to associate documents
    with a root entity (StatePerson or StateStaff)."""

    PERSON_ID = "person_id"
    PERSON_EXTERNAL_ID = "person_external_id"
    STAFF_ID = "staff_id"
    STAFF_EXTERNAL_ID = "staff_external_id"

    @property
    def id_column_name(self) -> str:
        """Returns the metadata column that holds the root-entity ID itself. The
        external-id types carry an additional id_type column, but this is only
        the id-bearing column.
        """
        if self is DocumentRootEntityIdType.PERSON_ID:
            return PERSON_ID_COLUMN_NAME
        if self is DocumentRootEntityIdType.PERSON_EXTERNAL_ID:
            return PERSON_EXTERNAL_ID_COLUMN_NAME
        if self is DocumentRootEntityIdType.STAFF_ID:
            return STAFF_ID_COLUMN_NAME
        if self is DocumentRootEntityIdType.STAFF_EXTERNAL_ID:
            return STAFF_EXTERNAL_ID_COLUMN_NAME
        raise ValueError(f"Unexpected root_entity_id_type: [{self}]")

    @property
    def id_type_column_name(self) -> str | None:
        """Returns the column holding the id_type that qualifies id_column_name
        for the external-id types, or None for the internal-id types.
        """
        if self is DocumentRootEntityIdType.PERSON_EXTERNAL_ID:
            return PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME
        if self is DocumentRootEntityIdType.STAFF_EXTERNAL_ID:
            return STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME
        if self in (
            DocumentRootEntityIdType.PERSON_ID,
            DocumentRootEntityIdType.STAFF_ID,
        ):
            return None
        raise ValueError(f"Unexpected root_entity_id_type: [{self}]")

    @property
    def id_column_type(self) -> bigquery.enums.SqlTypeNames:
        """Returns the BigQuery type of id_column_name, read from
        the column's canonical schema.
        """
        return bigquery.enums.SqlTypeNames(
            get_document_store_column_schema(self.id_column_name).field_type
        )

    @property
    def external_id_entity_cls(self) -> type[NormalizedStateEntity]:
        """Returns the normalized external-id entity class that documents with
        this root entity ID type resolve their internal id through. Only defined
        for the external-id types; the internal-id types carry their id directly
        and have no external-id table to join against.
        """
        if self is DocumentRootEntityIdType.PERSON_EXTERNAL_ID:
            return NormalizedStatePersonExternalId
        if self is DocumentRootEntityIdType.STAFF_EXTERNAL_ID:
            return NormalizedStateStaffExternalId
        raise ValueError(
            f"root_entity_id_type [{self}] has no external-id entity class."
        )

    @property
    def internal_id_type(self) -> "DocumentRootEntityIdType":
        """Returns the internal (Recidiviz-assigned) counterpart of this root
        entity ID type — PERSON_ID for any person type, STAFF_ID for any staff
        type. This is the ID the extraction result views resolve every document
        to, and the grain an entity-resolution composite document is built at.
        """
        if self in (
            DocumentRootEntityIdType.PERSON_ID,
            DocumentRootEntityIdType.PERSON_EXTERNAL_ID,
        ):
            return DocumentRootEntityIdType.PERSON_ID
        if self in (
            DocumentRootEntityIdType.STAFF_ID,
            DocumentRootEntityIdType.STAFF_EXTERNAL_ID,
        ):
            return DocumentRootEntityIdType.STAFF_ID
        raise ValueError(f"Unexpected root_entity_id_type: [{self}]")

    @property
    def resolved_internal_id_column_name(self) -> str:
        """Returns the internal Recidiviz entity-id column (`person_id` or
        `staff_id`) that a document with this root entity ID type resolves to,
        collapsing the external-id types onto their internal id. Contrast with
        `id_column_name`, which is the raw metadata column (e.g. `person_external_id`).
        """
        return self.internal_id_type.id_column_name


def _root_entity_schema_fields(
    root_entity_id_type: DocumentRootEntityIdType,
) -> list[bigquery.SchemaField]:
    """Returns the BQ SchemaFields for the given root entity ID type: the id
    column, plus the id_type column for the external-id types.
    """
    column_names = [root_entity_id_type.id_column_name]
    if (id_type_column := root_entity_id_type.id_type_column_name) is not None:
        column_names.append(id_type_column)
    return [get_document_store_column_schema(name) for name in column_names]


@attr.define
class DocumentCollectionConfig:
    """Configuration for a document collection. A document collection is a set of documents that share the same schema
    and are generated using the same SQL query. A document can be any text blob associated with a root entity.
    """

    # The state this document collection belongs to.
    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))

    # UPPER_SNAKE_CASE name that uniquely identifies a collection of documents
    # within a state. Lowercased wherever it becomes a BQ identifier (metadata /
    # temp table ids) or GCS path; persisted as-is (uppercase) wherever it is
    # stored as a column value.
    name: str = attr.ib(
        validator=[is_valid_unquoted_bq_identifier, attr_validators.is_upper_snake_case]
    )

    # Description providing more detail about the collection, its intended use, and any other relevant information.
    description: str = attr.ib(
        validator=recidiviz_attr_validators.is_meaningful_description
    )

    # The type of root entity (StatePerson or StateStaff, internal or external ID)
    # that documents in this collection are associated with. Determines the leading
    # root entity primary key columns (see the primary_key_columns property).
    root_entity_id_type: DocumentRootEntityIdType = attr.ib(
        validator=attr.validators.in_(DocumentRootEntityIdType)
    )

    # The document-specific primary key columns — those that uniquely identify a
    # document within a single root entity, excluding the root entity columns
    # (which are derived from root_entity_id_type). May be empty when a root entity
    # has at most one document in the collection (e.g. an entity-resolution
    # composite document). Combined with the root entity columns via the
    # primary_key_columns property. The full primary key should remain stable over
    # time (i.e. if a document is updated in place, its primary key should not
    # change).
    document_primary_key_columns: list[bigquery.SchemaField] = attr.ib(
        validator=attr_validators.is_list_of(bigquery.SchemaField)
    )

    # Additional metadata columns outputted by the document_generation_query that are
    # not part of the primary key but provide useful context about documents.
    other_metadata_columns: list[bigquery.SchemaField] = attr.ib(
        validator=attr_validators.is_list_of(bigquery.SchemaField)
    )

    # The SQL query template used to generate documents in this collection.
    document_generation_query_template: str = attr.ib(validator=attr_validators.is_str)

    # Columns the generation query outputs that are never exported to the metadata
    # table — carried through the temp-updates table and consumed by
    # collection-specific post-processing, exactly like the document_text
    # column.
    other_document_generation_output_columns: list[bigquery.SchemaField] = attr.ib(
        validator=attr_validators.is_list_of(bigquery.SchemaField)
    )

    # Singular/plural noun for the documents in this collection (e.g. "supervision
    # case note"/"...notes"). Consumed by generated prompts that need to name the
    # documents in this collection specifically.
    document_descriptor: Descriptor = attr.ib(
        kw_only=True, validator=attr.validators.instance_of(Descriptor)
    )

    def __attrs_post_init__(self) -> None:
        col_names = [
            col.name
            for col in self.primary_key_columns
            + self.other_metadata_columns
            + self.other_document_generation_output_columns
        ]
        duplicate_names = {n for n in col_names if col_names.count(n) > 1}
        if duplicate_names:
            raise ValueError(
                f"Document collection [{self.name}] for [{self.state_code.value}] "
                f"has duplicate column names: {duplicate_names}."
            )

    @property
    def root_entity_id_column_name(self) -> str:
        """Returns the name of the column that holds this collection's root-entity
        ID (person or staff).
        """
        return self.root_entity_id_type.id_column_name

    @property
    def _metadata_table_id(self) -> str:
        """Returns the BigQuery table ID for this document collection's metadata table."""
        return self.name.lower()

    @property
    def metadata_table_address(self) -> BigQueryAddress:
        """Returns the project-agnostic BigQuery address for this collection's
        metadata table. Convert with `.to_project_specific_address(project_id)` where
        a concrete project is needed.
        """
        return BigQueryAddress(
            dataset_id=document_store_metadata_dataset_for_region(self.state_code),
            table_id=self._metadata_table_id,
        )

    @property
    def primary_key_columns(self) -> list[bigquery.SchemaField]:
        """Returns the columns that uniquely identify a document within this
        collection: the root entity columns (derived from root_entity_id_type)
        followed by the document-specific primary key columns.
        """
        return [
            *_root_entity_schema_fields(self.root_entity_id_type),
            *self.document_primary_key_columns,
        ]

    @property
    def primary_key_column_names(self) -> list[str]:
        """Returns the list of primary key column names for this document collection."""
        return [col.name for col in self.primary_key_columns]

    @property
    def other_metadata_column_names(self) -> list[str]:
        """Returns the list of other metadata column names for this document collection."""
        return [col.name for col in self.other_metadata_columns]

    @property
    def other_document_generation_output_column_names(self) -> list[str]:
        """Returns the list of generation-output-only column names for this
        document collection (empty for ordinary collections).
        """
        return [col.name for col in self.other_document_generation_output_columns]

    def build_bq_metadata_schema(self) -> list[bigquery.SchemaField]:
        """Returns the full BigQuery schema for this collection's metadata table."""
        return [
            *self.primary_key_columns,
            *self.other_metadata_columns,
            get_document_store_column_schema(DOCUMENT_CONTENTS_ID_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_UPDATE_DATETIME_COLUMN_NAME),
            get_document_store_column_schema(ROW_CREATE_DATETIME_COLUMN_NAME),
        ]

    def build_bq_document_generation_output_schema(
        self,
    ) -> list[bigquery.SchemaField]:
        """Returns the BigQuery schema of this collection's
        document_generation_query output. Includes document_text and any
        other_document_generation_output_columns (neither persisted to the final
        metadata table) and excludes row_create_datetime (set at final write time). The
        temp_document_metadata_updates_* tables share this schema — its rows are
        generation-output rows, with non-primary-key columns NULLed for deleted
        documents.
        """
        return [
            *self.primary_key_columns,
            *self.other_metadata_columns,
            get_document_store_column_schema(DOCUMENT_CONTENTS_ID_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_TEXT_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_UPDATE_DATETIME_COLUMN_NAME),
            *self.other_document_generation_output_columns,
        ]

    def temp_document_generation_output_table_address(
        self, run_id: str
    ) -> BigQueryAddress:
        """Returns the project-agnostic BigQuery address for the temp table holding
        this run's full document_generation_query output — every document for every
        root entity, before the diff narrows to changed rows.
        """
        return BigQueryAddress(
            dataset_id=document_store_temp_dataset_for_region(self.state_code),
            table_id=(
                f"{TEMP_DOCUMENT_GENERATION_OUTPUT_TABLE_ID_PREFIX}"
                f"{self.name.lower()}_{make_bq_compatible_identifier(run_id)}"
            ),
        )

    def temp_document_metadata_updates_table_address(
        self, run_id: str
    ) -> BigQueryAddress:
        """Returns the project-agnostic BigQuery address for the temp document
        metadata updates table that contains rows where there were any changes to
        document_contents_id or another metadata column for each primary key in
        this collection."""
        return BigQueryAddress(
            dataset_id=document_store_temp_dataset_for_region(self.state_code),
            table_id=(
                f"{TEMP_METADATA_UPDATES_TABLE_ID_PREFIX}"
                f"{self.name.lower()}_{make_bq_compatible_identifier(run_id)}"
            ),
        )

    def temp_new_document_contents_table_address(self, run_id: str) -> BigQueryAddress:
        """Returns the project-agnostic BigQuery address for the temp new document
        contents table that tracks which document_contents_ids in this collection
        have not yet been uploaded for the state. This is the table read from to
        perform the actual document upload."""
        return BigQueryAddress(
            dataset_id=document_store_temp_dataset_for_region(self.state_code),
            table_id=(
                f"{TEMP_NEW_DOCUMENT_CONTENTS_TABLE_ID_PREFIX}"
                f"{self.name.lower()}_{make_bq_compatible_identifier(run_id)}"
            ),
        )

    @property
    def _document_contents_table_id(self) -> str:
        """Returns the BigQuery table ID for this collection's document_contents table."""
        return f"{self.name.lower()}_document_contents"

    @property
    def document_contents_table_address(self) -> BigQueryAddress:
        """Returns the project-agnostic BigQuery address for this collection's
        document_contents table, which holds one row per distinct
        document_contents_id successfully uploaded to GCS for this collection.
        Convert with `.to_project_specific_address(project_id)` where a concrete
        project is needed.
        """
        return BigQueryAddress(
            dataset_id=document_contents_dataset_for_region(self.state_code),
            table_id=self._document_contents_table_id,
        )

    def build_bq_document_contents_schema(self) -> list[bigquery.SchemaField]:
        """Returns the BigQuery schema for the collection's document_contents table."""
        return [
            get_document_store_column_schema(DOCUMENT_CONTENTS_ID_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_TEXT_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_LENGTH_BYTES_COLUMN_NAME),
            get_document_store_column_schema(ROW_CREATE_DATETIME_COLUMN_NAME),
        ]

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "DocumentCollectionConfig":
        """Loads a DocumentCollectionConfig from a YAML file."""
        yaml_dict = YAMLDict.from_path(str(yaml_path))

        root_entity_id_type = DocumentRootEntityIdType(
            yaml_dict.pop("root_entity_id_type", str)
        )

        document_descriptor = Descriptor.from_yaml_dict(
            yaml_dict.pop_dict("document_descriptor")
        )

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
            root_entity_id_type=root_entity_id_type,
            document_primary_key_columns=document_pk_columns,
            other_metadata_columns=other_metadata_columns,
            document_generation_query_template=yaml_dict.pop(
                "document_generation_query", str
            ),
            # Not authorable from YAML — only generated (e.g. entity-resolution)
            # collections declare generation-output-only columns.
            other_document_generation_output_columns=[],
            document_descriptor=document_descriptor,
        )

    @staticmethod
    def config_name_to_file_path(
        state_code: StateCode,
        collection_name: str,
        config_module: ModuleType | None = None,
    ) -> Path:
        """Returns the file path to the YAML config for a given collection name."""
        return (
            _state_collections_dir(
                state_code, config_module=config_module or default_config_module
            )
            / f"{collection_name.lower()}.yaml"
        )

    @staticmethod
    def file_path_to_config_name(file_path: Path) -> str:
        """Returns the UPPER_SNAKE_CASE collection name for a given YAML config
        file path (whose stem is the lowercase form).
        """
        return file_path.stem.upper()

    @staticmethod
    def file_path_to_state_code(file_path: Path) -> StateCode:
        """Returns the state code for a given YAML config file path."""
        # Parent is the state dir
        return StateCode(file_path.parent.name.upper())


# Cached configs are partitioned by config module so that configs loaded from
# different modules (e.g. different test fake modules) never collide on a shared (state,
# name) key.
_DOCUMENT_COLLECTION_CONFIGS: dict[
    ModuleType, dict[StateCode, dict[str, DocumentCollectionConfig]]
] = defaultdict(lambda: defaultdict(dict))


def _load_document_collection_config(
    yaml_path: Path, config_module: ModuleType
) -> DocumentCollectionConfig:
    """Loads a single config and caches it under |config_module| in the global
    dict.
    """
    if not yaml_path.is_file():
        raise ValueError(f"No config file found at [{yaml_path}]")

    config_name = DocumentCollectionConfig.file_path_to_config_name(yaml_path)
    state_code = DocumentCollectionConfig.file_path_to_state_code(yaml_path)

    state_configs = _DOCUMENT_COLLECTION_CONFIGS[config_module][state_code]
    if config_name in state_configs:
        return state_configs[config_name]

    config = DocumentCollectionConfig.from_yaml(yaml_path)
    state_configs[config_name] = config
    return config


def collect_document_collection_config_yaml_paths(
    state_code: StateCode,
    config_module: ModuleType | None = None,
) -> list[Path]:
    """Returns a list of file paths to document collection YAML configs for a given state."""
    state_dir = _state_collections_dir(
        state_code, config_module=config_module or default_config_module
    )
    if not state_dir.is_dir():
        return []

    return list(state_dir.glob("*.yaml"))


def get_states_with_document_collections(
    config_module: ModuleType | None = None,
) -> list[StateCode]:
    """Returns the list of StateCode values that have document collection configs."""
    module = config_module or default_config_module
    return [
        sc
        for sc in StateCode
        if collect_document_collection_config_yaml_paths(sc, config_module=module)
    ]


def load_first_order_document_collection_configs(
    state_code: StateCode,
    config_module: ModuleType | None = None,
) -> dict[str, DocumentCollectionConfig]:
    """Returns a map of collection name to its configuration for every first-order
    (YAML-authored) document collection defined for the given state code.

    This does not include the entity-resolution composite-document collections,
    which are generated rather than authored; see
    `collect_all_document_collection_configs` in
    `document_collection_config_collectors.py` for the combined set.
    """
    module = config_module or default_config_module
    for yaml_path in collect_document_collection_config_yaml_paths(
        state_code, config_module=module
    ):
        _load_document_collection_config(yaml_path, module)

    return _DOCUMENT_COLLECTION_CONFIGS[module][state_code]
