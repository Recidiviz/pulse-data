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
from google.cloud.bigquery import ExternalConfig, SchemaField, Table

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.yaml_dict import YAMLDict

DEFAULT_SOURCE_TABLE_DESCRIPTION = "TODO(#29155): Add a description as to what this is used for and why it isn't managed in code"


@attr.s(auto_attribs=True)
class SourceTableConfig:
    """Object representing a BigQuery table"""

    address: BigQueryAddress
    description: str
    schema_fields: list[SchemaField]
    external_data_configuration: ExternalConfig | None = attr.ib(default=None)
    clustering_fields: list[str] | None = attr.ib(factory=list)
    yaml_definition_path: str | None = attr.ib(default=None)
    is_sandbox_table: bool = attr.ib(default=False)

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
        representation = {
            "address": {
                "dataset_id": self.address.dataset_id,
                "table_id": self.address.table_id,
            },
            "description": self.description,
            "schema": [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                }
                for field in self.schema_fields
            ],
            "clustering_fields": self.clustering_fields,
        }

        if self.external_data_configuration:
            representation[
                "external_data_configuration"
            ] = self.external_data_configuration.to_api_repr()

        return representation

    @classmethod
    def from_file(cls, yaml_path: str) -> "SourceTableConfig":
        yaml_definition = YAMLDict.from_path(yaml_path)

        external_data_configuration = None
        if external_config := yaml_definition.pop_optional(
            "external_data_configuration", dict
        ):
            external_data_configuration = ExternalConfig.from_api_repr(external_config)

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
        )

    @classmethod
    def from_table(cls, table: Table) -> "SourceTableConfig":
        schema = [*table.schema]

        return cls(
            address=BigQueryAddress(
                dataset_id=table.dataset_id,
                table_id=table.table_id,
            ),
            description=table.description or DEFAULT_SOURCE_TABLE_DESCRIPTION,
            schema_fields=schema,
            clustering_fields=table.clustering_fields,
            external_data_configuration=table.external_data_configuration,
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

    # TODO(#29517): We should be able to remove this once the combined
    #  ingest/normalization pipeline outputs ALL entities to its us_xx_normalized_state
    #  dataset.
    source_is_normalization_pipeline: bool = attr.ib(validator=attr_validators.is_bool)

    @property
    def value(self) -> bool:
        return self.source_is_normalization_pipeline


@attr.define
class NormalizedStateSpecificEntitySourceTableLabel(SourceTableLabel[StateCode]):
    """Label for source tables in a state-specific us_xx_normalized_state dataset"""

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))

    @property
    def value(self) -> StateCode:
        return self.state_code


@attr.define
class IngestPipelineEntitySourceTableLabel(SourceTableLabel[StateCode]):
    """Label for source tables output by an ingest pipeline into a state-specific
    `us_xx_state` dataset.
    """

    state_code: StateCode = attr.ib(validator=attr.validators.instance_of(StateCode))

    @property
    def value(self) -> StateCode:
        return self.state_code


@attr.define
class IngestViewOutputSourceTableLabel(SourceTableLabel[StateCode]):
    """Label for source tables output by an ingest pipeline that contain the results of
    ingest view queries.
    """

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


@attr.s(auto_attribs=True)
class SourceTableCollection:
    """Represents a set of source tables in a dataset. A dataset may be composed of
    multiple collections.
    """

    dataset_id: str
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


class SourceTableCouldNotGenerateError(ValueError):
    pass
