# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""
Generates a JSON file that can be used to seed the emulator with source tables
"""
import json
import logging
import os
from typing import Literal

import attr
from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID

SOURCE_TABLES_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "./fixtures")
SOURCE_TABLES_FIXTURE_FILE_NAME = "emulator_source_tables.json"
SOURCE_TABLES_JSON_PATH = os.path.join(
    SOURCE_TABLES_FIXTURES_DIR, SOURCE_TABLES_FIXTURE_FILE_NAME
)


# The JSON classes below are attrs-based dataclasses intended for serialization to the file-format used  by
# the emulator to load initial data.
@attr.s(auto_attribs=True)
class ColumnJSON:
    """Represents a single column in a BQ table."""

    name: str
    type: str
    mode: Literal["NULLABLE", "REPEATED", "REQUIRED"]
    # Fields are nested columns for STRUCT values
    fields: list["ColumnJSON"] | None = attr.ib(default=None)

    @fields.validator
    def validate_is_struct_type(self, _attribute: str, value: list | None) -> None:
        if self.type != "STRUCT" and value:
            raise ValueError("Cannot set struct fields on non-struct type column")


@attr.s(auto_attribs=True)
class TableJSON:
    """Represents a single BQ table."""

    id: str
    columns: list[ColumnJSON]


@attr.s(auto_attribs=True)
class DatasetJSON:
    """Represents a single BQ dataset."""

    id: str
    tables: list[TableJSON]


@attr.s(auto_attribs=True)
class ProjectJSON:
    """Represents a single BQ project.
    Can be transformed to a dictionary suitable for conversion to JSON via attrs.as_dict()"""

    id: str
    datasets: list[DatasetJSON]

    def add_dataset(self, dataset_id: str) -> DatasetJSON:
        """Returns a dataset object for the dataset with the given id, creating a new one if it does not exit."""
        try:
            dataset = one(
                dataset for dataset in self.datasets if dataset.id == dataset_id
            )
        except ValueError:
            dataset = DatasetJSON(id=dataset_id, tables=[])
            self.datasets.append(dataset)

        return dataset

    def add_table(self, dataset_id: str, table_yaml: TableJSON) -> None:
        self.add_dataset(dataset_id).tables.append(table_yaml)


def build_column_from_schema_field(schema_field: SchemaField) -> ColumnJSON:
    return ColumnJSON(
        name=schema_field.name,
        type=schema_field.field_type,
        mode=schema_field.mode,
        fields=[build_column_from_schema_field(field) for field in schema_field.fields],
    )


def _update_with_all_source_table_schemas(
    project_json: ProjectJSON,
    source_table_collections: list[SourceTableCollection],
) -> None:
    """Given a set of source table collections, add their corresponding datasets and tables to a ProjectJSON instance"""
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Getting source table schemas configs...")

    for source_table_collection in source_table_collections:
        project_json.add_dataset(source_table_collection.dataset_id)

    source_tables_by_address = {
        address: source_table_config
        for source_table_collection in source_table_collections
        for address, source_table_config in source_table_collection.source_tables_by_address.items()
    }

    for address, table in source_tables_by_address.items():
        columns = [
            build_column_from_schema_field(column) for column in table.schema_fields
        ]

        # Tables with `external_data_configuration` specified get a special _FILE_NAME meta column
        if table.external_data_configuration is not None and any(
            uri.startswith("gs://")
            for uri in table.external_data_configuration.source_uris
        ):
            columns.append(
                ColumnJSON(
                    name="_FILE_NAME",
                    type="STRING",
                    mode="REQUIRED",
                    fields=[],
                ),
            )

        project_json.add_table(
            address.dataset_id, TableJSON(id=address.table_id, columns=columns)
        )


def write_emulator_source_tables_json(
    source_table_collections: list[SourceTableCollection], file_name: str
) -> str:
    """Given a set of source table collections, generate an emulator-compatible json input schema and write it to
    disk"""
    project_json = ProjectJSON(id=BQ_EMULATOR_PROJECT_ID, datasets=[])
    _update_with_all_source_table_schemas(
        project_json, source_table_collections=source_table_collections
    )

    source_table_file = os.path.join(SOURCE_TABLES_FIXTURES_DIR, file_name)
    if not os.path.exists(SOURCE_TABLES_FIXTURES_DIR):
        os.mkdir(SOURCE_TABLES_FIXTURES_DIR)

    with open(source_table_file, "w", encoding="utf-8") as f:
        f.write(json.dumps({"projects": [attr.asdict(project_json)]}))

    logging.info("Wrote source table output to %s!", source_table_file)

    return str(source_table_file)
