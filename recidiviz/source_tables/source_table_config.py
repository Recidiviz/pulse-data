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
"""Classes for BigQuery source tables"""
import abc
import typing
from typing import Any

import attr
from google.cloud import bigquery
from google.cloud.bigquery import (
    ExternalConfig,
    ExternalSourceFormat,
    SchemaField,
    Table,
    TimePartitioning,
    TimePartitioningType,
)

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.constants import (
    EXTERNAL_DATA_FILE_NAME_PSEUDOCOLUMN,
    EXTERNAL_DATA_SOURCE_FORMATS_WITHOUT_FILE_NAME_PSUEDOCOLUMN,
    PARTITION_DATE_PSEUDOCOLUMN,
    PARTITION_TIME_PSEUDOCOLUMN,
    REQUIRE_PARTITION_FILTER_FIELD_NAME,
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type
from recidiviz.utils.yaml_dict import YAMLDict

# Split up word to avoid lint checks
PLACEHOLDER_TO_DO_STRING = "TO" + "DO"
DEFAULT_SOURCE_TABLE_DESCRIPTION = (
    f"{PLACEHOLDER_TO_DO_STRING}(XXXXX): Add a description as to what "
    f"this is used for and why it isn't managed in code"
)
DEFAULT_COLUMN_DESCRIPTION = (
    f"{PLACEHOLDER_TO_DO_STRING}(XXXXX): Add a column description"
)


@attr.define(kw_only=True)
class SourceTableConfig:
    """Object representing a BigQuery table"""

    address: BigQueryAddress
    description: str
    schema_fields: list[SchemaField]
    clustering_fields: list[str] | None = attr.ib(factory=list)
    time_partitioning: TimePartitioning | None = attr.ib(default=None)
    require_partition_filter: bool | None = attr.ib(default=None)
    yaml_definition_path: str | None = attr.ib(default=None)
    deployed_projects: list[str] = attr.ib(factory=list)
    is_sandbox_table: bool = attr.ib(default=False)

    # Mapping between BigQuery project and project to substitute in the {project_id} template var
    source_project_mapping: dict[str, str] | None = attr.ib(default=None)

    # Set via the external_data_configuration keyword argument, but private
    _external_data_configuration: ExternalConfig | None = attr.ib(default=None)

    # Lazily set when the external_data_configuration is accessed. This
    # external config has a matching schema and URIs formatted to match the
    # project if necessary.
    _formatted_external_data_configuration: ExternalConfig | None = attr.ib(
        default=None, init=False
    )

    def __attrs_post_init__(self) -> None:
        if not self.schema_fields:
            raise ValueError(
                f"Source table {self.address.to_str()} must have non-empty schema."
            )

        # we use .to_api_repr().get() here as doing time_partitioning.require_partition_filter
        # raises a deprecation warning; however, it can be set from `from_api_repr` w/o
        # the same warning so we still need to check it
        if (
            self.time_partitioning is not None
            and self.time_partitioning.to_api_repr().get(
                REQUIRE_PARTITION_FILTER_FIELD_NAME
            )
            is not None
        ):
            raise ValueError(
                "Setting the require_partition_filter property on TimePartitioning is "
                "deprecated. Please follow the instructions in go/source-table-time-partitioning "
                "to move this property to the table-level"
            )

        if self.time_partitioning is not None and self.require_partition_filter is None:
            raise ValueError(
                "require_partition_filter must be set if time_partitioning is set. If "
                "this did not happen automatically, please see go/source-table-time-partitioning "
                "for context on how to configure this."
            )

        for field in self.schema_fields:
            if not field.name:
                raise ValueError(
                    f"Found field in source table [{self.address.to_str()}] with an "
                    f"empty / undefined name."
                )

            if not field.field_type:
                raise ValueError(
                    f"Found field [{field.name}] in source table "
                    f"[{self.address.to_str()}] with no defined type."
                )
            if not field.mode:
                raise ValueError(
                    f"Found field [{field.name}] in source table "
                    f"[{self.address.to_str()}] with no defined mode."
                )

    @property
    def is_external_table(self) -> bool:
        """Returns true if this table reads from external data (i.e. has an
        external_data_configuration).
        """
        return self._external_data_configuration is not None

    @property
    def external_data_configuration(self) -> ExternalConfig | None:
        """Returns this table's external data configuration, formatted for the current
        project.
        """
        if not self._external_data_configuration:
            return None

        if not self._formatted_external_data_configuration:
            self._formatted_external_data_configuration = ExternalConfig.from_api_repr(
                self._external_data_configuration.to_api_repr()
            )
            # For external tables, we store the schema on the external_data_configuration
            self._formatted_external_data_configuration.schema = self.schema_fields
            self._format_source_uris_for_project(
                self.address,
                self._formatted_external_data_configuration,
                self.source_project_mapping,
            )
        return self._formatted_external_data_configuration

    @staticmethod
    def _format_source_uris_for_project(
        address: BigQueryAddress,
        external_data_configuration: ExternalConfig,
        source_project_mapping: dict[str, str] | None,
    ) -> None:
        """Formats any source_uris in the provided |external_data_configuration|
        so that they point to the appropriate bucket in the current project, if
        the URIs are GCS URIs.

        Throws if a URI cannot be formatted.
        """
        project_id = metadata.project_id()
        if not external_data_configuration.source_uris:
            raise ValueError(
                f"Cannot set external_data_configuration without any defined "
                f"sourceUris. Found no sourceUris for table [{address.to_str()}]."
            )

        source_data_project_id = None
        if source_project_mapping:
            if project_id in source_project_mapping:
                source_data_project_id = source_project_mapping[project_id]
            else:
                raise KeyError(
                    f"Project {project_id} not found in source_project_mapping {source_project_mapping}"
                )
        else:
            source_data_project_id = project_id

        source_format = external_data_configuration.source_format
        if source_format == ExternalSourceFormat.GOOGLE_SHEETS:
            # No need to reformat Google Sheets links
            return

        if source_format in (
            ExternalSourceFormat.CSV,
            ExternalSourceFormat.NEWLINE_DELIMITED_JSON,
        ):
            formatted_source_uris = []
            for source_uri in external_data_configuration.source_uris:
                try:
                    formatted_source_uris.append(
                        StrictStringFormatter().format(
                            source_uri, project_id=source_data_project_id
                        )
                    )
                except Exception as e:
                    raise ValueError(
                        f"Could not format source URI {source_uri}. All {source_format} "
                        f"source URIs must have a project_id format argument. For "
                        f"example 'gs://{{project_id}}-my-bucket/source_data.csv"
                    ) from e

            external_data_configuration.source_uris = formatted_source_uris
            return

        raise ValueError(f"Unsupported source_format {source_format}")

    def validate_source_table_external_data_configuration(
        self, update_config: "SourceTableCollectionUpdateConfig"
    ) -> None:
        """Enforces that that this source table's external_data_configuration is valid."""
        if not self.external_data_configuration:
            return

        if not self.external_data_configuration.schema:
            raise ValueError(
                f"Found empty external table schema for {self.address.to_str()}."
            )

        if update_config != update_config.regenerable():
            raise ValueError(
                f"Found managed source table [{self.address.to_str()}] which is not "
                f"designated as regenerable(). All external tables are fundamentally "
                f"regenerable - they can be deleted and recreated with the new config "
                f"without losing data."
            )

        if self.external_data_configuration.ignore_unknown_values is None:
            raise ValueError(
                f"Must explicitly set ignoreUnknownValues for external table "
                f"{self.address.to_str()}."
            )

        ignore_unknown_values = assert_type(
            self.external_data_configuration.ignore_unknown_values, bool
        )
        source_format = self.external_data_configuration.source_format
        if source_format == ExternalSourceFormat.GOOGLE_SHEETS:
            if not ignore_unknown_values:
                raise ValueError(
                    f"Must explicitly set ignoreUnknownValues: true for "
                    f"{source_format} external table {self.address.to_str()}. "
                    f"{source_format} tables do not respect ignoreUnknownValues: false."
                )
            return

        if source_format in (
            ExternalSourceFormat.NEWLINE_DELIMITED_JSON,
            ExternalSourceFormat.CSV,
        ):
            return

        raise ValueError(
            f"Unsupported external table source format [{source_format}]. Discuss with "
            f"#platform-team if you have a compelling use case for this format."
        )

    def has_column(self, column: str) -> bool:
        return any(c.name == column for c in self.schema_fields + self.pseudocolumns)

    @property
    def column_names(self) -> list[str]:
        """The list of column names for columns in this table"""
        return [f.name for f in self.schema_fields]

    @property
    def all_schema_fields(self) -> list[SchemaField]:
        """Returns all schema fields, both those explicitized in our source table configs,
        as well as implicit psuedocolumns added by BigQuery.
        """
        return [*self.schema_fields, *self.pseudocolumns]

    @property
    def pseudocolumns(self) -> list[SchemaField]:
        """Returns a list of pseudocolumns that are available for query on this table.
        These are columns that will not be returned by a SELECT * query on this table,
        but can be explicitly queried.
        """
        psuedocolumns = []

        # if time partitioning does not specify a field name, it means it uses ingest-time
        # partitioning; in these cases, there are additional psuedocolumns that are available.
        if self.time_partitioning and self.time_partitioning.field is None:
            if self.time_partitioning.type_ == TimePartitioningType.DAY:
                # if it uses day-based partitioning, there are two psuedocolumns
                psuedocolumns.append(
                    SchemaField(
                        name=PARTITION_DATE_PSEUDOCOLUMN,
                        field_type=bigquery.enums.SqlTypeNames.DATE.value,
                        mode="REQUIRED",
                    )
                )

            psuedocolumns.append(
                SchemaField(
                    name=PARTITION_TIME_PSEUDOCOLUMN,
                    field_type=bigquery.enums.SqlTypeNames.TIMESTAMP.value,
                    mode="REQUIRED",
                )
            )

        # MOST external tables have the _FILE_NAME column available for query, see:
        # https://cloud.google.com/bigquery/docs/query-cloud-storage-data#query_the_file_name_pseudo-column;
        # however, some external tables do not (such as google sheets)
        if (
            self._external_data_configuration
            and self._external_data_configuration.source_format
            not in EXTERNAL_DATA_SOURCE_FORMATS_WITHOUT_FILE_NAME_PSUEDOCOLUMN
        ):
            psuedocolumns.append(
                SchemaField(
                    name=EXTERNAL_DATA_FILE_NAME_PSEUDOCOLUMN,
                    field_type=bigquery.enums.SqlTypeNames.STRING.value,
                    mode="REQUIRED",
                )
            )

        return psuedocolumns

    def as_sandbox_table(self, sandbox_dataset_prefix: str) -> "SourceTableConfig":
        if self.is_sandbox_table:
            raise ValueError(
                f"Config for [{self.address.to_str()}] is already a sandbox config."
            )
        return attr.evolve(
            self,
            address=BigQueryAddress(
                dataset_id=BigQueryAddressOverrides.format_sandbox_dataset(
                    sandbox_dataset_prefix, self.address.dataset_id
                ),
                table_id=self.address.table_id,
            ),
            is_sandbox_table=True,
        )

    def to_dict(self) -> dict[str, Any]:
        """Provides a serializable representation of a SourceTableConfig object."""
        representation: dict[str, Any] = {
            "address": {
                "dataset_id": self.address.dataset_id,
                "table_id": self.address.table_id,
            },
            "description": self.description,
            "schema": [
                {
                    "name": field.name,
                    "description": field.description,
                    "type": field.field_type,
                    "mode": field.mode,
                }
                for field in self.schema_fields
            ],
            "clustering_fields": self.clustering_fields,
        }

        if self._external_data_configuration:
            representation[
                "external_data_configuration"
            ] = self._external_data_configuration.to_api_repr()

        if self.time_partitioning:
            representation["time_partitioning"] = self.time_partitioning.to_api_repr()

        if self.require_partition_filter is not None:
            representation["require_partition_filter"] = self.require_partition_filter

        return representation

    @classmethod
    def from_file(cls, yaml_path: str) -> "SourceTableConfig":
        yaml_definition = YAMLDict.from_path(yaml_path)

        external_data_configuration = None
        if external_config := yaml_definition.pop_optional(
            "external_data_configuration", dict
        ):
            external_data_configuration = ExternalConfig.from_api_repr(external_config)

        time_partitioning = None
        if time_partitioning_dict := yaml_definition.pop_optional(
            "time_partitioning", dict
        ):
            time_partitioning = TimePartitioning.from_api_repr(time_partitioning_dict)

        return cls(
            address=BigQueryAddress(**yaml_definition.pop("address", dict)),
            description=yaml_definition.pop_optional("description", str)
            or DEFAULT_SOURCE_TABLE_DESCRIPTION,
            schema_fields=[
                SchemaField.from_api_repr(column)
                for column in yaml_definition.pop("schema", list)
            ],
            external_data_configuration=external_data_configuration,
            yaml_definition_path=yaml_path,
            deployed_projects=yaml_definition.pop_list_optional(
                "deployed_projects", str
            )
            or [],
            time_partitioning=time_partitioning,
            require_partition_filter=yaml_definition.pop_optional(
                "require_partition_filter", bool
            ),
            source_project_mapping=yaml_definition.pop_optional(
                "source_project_mapping", dict
            ),
        )

    @classmethod
    def from_table(cls, table: Table) -> "SourceTableConfig":
        schema = [
            SchemaField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=field.description or DEFAULT_COLUMN_DESCRIPTION,
                fields=field.fields,
            )
            for field in table.schema
        ]

        return cls(
            address=BigQueryAddress(
                dataset_id=table.dataset_id,
                table_id=table.table_id,
            ),
            description=table.description or DEFAULT_SOURCE_TABLE_DESCRIPTION,
            schema_fields=schema,
            clustering_fields=table.clustering_fields,
            external_data_configuration=table.external_data_configuration,
            time_partitioning=table.time_partitioning,
            # In BQ, require_partition_filter defaults to False when not set.
            require_partition_filter=table.require_partition_filter
            if table.require_partition_filter is not None
            else (False if table.time_partitioning is not None else None),
        )


SourceTableLabelT = typing.TypeVar("SourceTableLabelT")


@attr.define
class SourceTableLabel(typing.Generic[SourceTableLabelT]):
    @property
    @abc.abstractmethod
    def value(self) -> SourceTableLabelT:
        """Returns the value for this label"""


@attr.define
class DataflowPipelineSourceTableLabel(SourceTableLabel[str]):
    pipeline_name: str = attr.ib(validator=attr_validators.is_str)

    @property
    def value(self) -> str:
        return self.pipeline_name


# TODO(#27373): Once we've refactored the Dataflow pipelines to read directly from
#  previous pipelines' state-specific outputs and converted the `state` and
#  `normalized_state` datasets to materialized view outputs, we should no longer have
#  any more of these source tables
@attr.define
class NormalizedStateAgnosticEntitySourceTableLabel(SourceTableLabel[bool]):
    """Label for source tables in the `normalized_state` dataset"""

    @property
    def value(self) -> bool:
        return True


@attr.define
class StateSpecificSourceTableLabel(SourceTableLabel[StateCode]):
    """Label for source tables in a state-specific dataset"""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))

    @property
    def value(self) -> StateCode:
        return self.state_code


@attr.define
class RawDataSourceTableLabel(SourceTableLabel[tuple[StateCode, DirectIngestInstance]]):
    """Tables in state-specific us_xx_raw_data datasets."""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))
    ingest_instance: DirectIngestInstance = attr.ib(
        validator=attr.validators.instance_of(DirectIngestInstance)
    )

    @property
    def value(self) -> tuple[StateCode, DirectIngestInstance]:
        return self.state_code, self.ingest_instance


@attr.define
class IngestViewResultsSourceTableLabel(SourceTableLabel[StateCode]):
    """Label for source tables in a state-specific ingest view results dataset"""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))

    @property
    def value(self) -> StateCode:
        return self.state_code


@attr.define
class SchemaTypeSourceTableLabel(SourceTableLabel[SchemaType]):
    """Tables whose schemas are defined by the schema with the given schema_type."""

    schema_type: SchemaType = attr.ib(validator=attr.validators.instance_of(SchemaType))

    @property
    def value(self) -> SchemaType:
        return self.schema_type


# TODO(#27373): Once we've refactored the Dataflow pipelines to read directly from
#  previous pipelines' state-specific outputs and converted the `state` and
#  `normalized_state` datasets to materialized view outputs, we should no longer have
#  any more of these state tables
@attr.define
class UnionedStateAgnosticSourceTableLabel(SourceTableLabel[str]):
    """Source tables that are built by unioning a collection of state-specific pipeline
    outputs into a single state-agnostic table.
    """

    dataset_id: str

    @property
    def value(self) -> str:
        return self.dataset_id


@attr.s(auto_attribs=True, frozen=True)
class SourceTableCollectionValidationConfig:
    """Configures the schema validation for the source table collection"""

    # Some unmanaged tables may have many different versions of their schema,
    # it may be useful to only check a subset of columns in these cases.
    # So these "required" columns are required by the view graph and should be
    # validated that the fields exist in BigQuery.
    # Required does not mean the column mode (REQUIRED vs NULLABLE)
    only_check_required_columns: bool


@attr.define(frozen=True, kw_only=True)
class SourceTableCollectionUpdateConfig:
    """Configuration object for how we attempt to manage the schema of source tables"""

    attempt_to_manage: bool
    allow_field_deletions: bool
    # For tables that are ephemeral, or whose contents are fully repopulated each time they're changed,
    # it may be beneficial to recreate the table in the case of an update error (e.g. incompatible schema type changes,
    # clustering field mismatch)
    recreate_on_update_error: bool

    def __attrs_post_init__(self) -> None:
        if self.recreate_on_update_error and not self.allow_field_deletions:
            raise ValueError(
                "Cannot set recreate_on_update_error=True with "
                "allow_field_deletions=False. If we can't delete columns, we can't "
                "delete and recreate the table."
            )

    @classmethod
    def externally_managed(cls) -> "SourceTableCollectionUpdateConfig":
        """Externally managed tables are used in our view graph, but whose creation /
        schema updates are not managed by our standard table update process. For
        example, pulse_dashboard_segment_metrics is created by a Segment reverse ETL
        integration. We want to have a copy of its schema in code, but we don't want to
        attempt to manage its schema. We will check during our table update that the
        actual schema in BQ matches what is defined in code.
        """
        return cls(
            attempt_to_manage=False,
            allow_field_deletions=False,
            recreate_on_update_error=False,
        )

    @classmethod
    def protected(cls) -> "SourceTableCollectionUpdateConfig":
        """Protected table collections contain tables whose schemas are managed by our
        standard table update process, but which may not easily be reconstructed from
        other sources. This configuration is the most precautionary, disallowing any
        field deletions as it may result in data loss.
        """
        return cls(
            attempt_to_manage=True,
            allow_field_deletions=False,
            recreate_on_update_error=False,
        )

    @classmethod
    def regenerable(cls) -> "SourceTableCollectionUpdateConfig":
        """Regenerable tables can be reconstructed from another source on the fly.
        This configuration allows field deletion. If invalid schema updates are
        requested (ie changing a column's type), it will be dropped and recreated with
        the new schema.
        """
        return cls(
            attempt_to_manage=True,
            allow_field_deletions=True,
            recreate_on_update_error=True,
        )


@attr.s(auto_attribs=True)
class SourceTableCollection:
    """Represents a set of source tables in a dataset. A dataset may be composed of
    multiple collections.
    """

    # The dataset where this collection of tables lives. There may or may not be other
    # tables in this datasest.
    dataset_id: str

    # The description for this collection of tables
    description: str

    # Configuration object for how we attempt to manage the source tables in this
    # collection.
    update_config: SourceTableCollectionUpdateConfig

    validation_config: SourceTableCollectionValidationConfig | None = attr.ib(
        default=None
    )

    labels: list[SourceTableLabel[Any]] = attr.ib(factory=list)
    default_table_expiration_ms: int | None = attr.ib(default=None)
    source_tables_by_address: dict[BigQueryAddress, SourceTableConfig] = attr.ib(
        factory=dict
    )
    is_sandbox_collection: bool = attr.ib(default=False)

    def as_sandbox_collection(
        self, sandbox_dataset_prefix: str
    ) -> "SourceTableCollection":
        if self.is_sandbox_collection:
            raise ValueError("Config for this collection is already a sandbox config.")

        dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
            sandbox_dataset_prefix, self.dataset_id
        )
        source_tables_by_address = {}
        for source_table in self.source_tables_by_address.values():
            sandbox_table = source_table.as_sandbox_table(sandbox_dataset_prefix)
            source_tables_by_address[sandbox_table.address] = sandbox_table

        return attr.evolve(
            self,
            dataset_id=dataset_id,
            source_tables_by_address=source_tables_by_address,
            is_sandbox_collection=True,
        )

    @property
    def table_expiration_ms(self) -> int | None:
        return (
            TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
            if self.is_sandbox_collection
            else self.default_table_expiration_ms
        )

    def _build_table_address(self, table_id: str) -> BigQueryAddress:
        return BigQueryAddress(dataset_id=self.dataset_id, table_id=table_id)

    def has_table(self, table_id: str) -> bool:
        return self._build_table_address(table_id) in self.source_tables_by_address

    def has_label(self, label: SourceTableLabel) -> bool:
        return any(
            src_label.value == label.value
            for src_label in self.labels
            if isinstance(src_label, type(label))
        )

    def has_any_label(self, labels: list[SourceTableLabel]) -> bool:
        """Returns True if the collection has any of the provided labels."""
        return any(
            src_label.value == label.value
            for src_label in self.labels
            for label in labels
            if isinstance(src_label, type(label))
        )

    def add_source_table(
        self,
        table_id: str,
        schema_fields: list[SchemaField],
        description: str | None = None,
        clustering_fields: list[str] | None = None,
    ) -> None:
        if not clustering_fields:
            clustering_fields = []

        address = self._build_table_address(table_id)
        self.source_tables_by_address[address] = SourceTableConfig(
            address=address,
            description=description or f"{address} as defined in code",
            schema_fields=schema_fields,
            clustering_fields=clustering_fields,
        )

    @property
    def source_tables(self) -> list[SourceTableConfig]:
        return list(self.source_tables_by_address.values())


class SourceTableConfigDoesNotExistError(ValueError):
    pass
