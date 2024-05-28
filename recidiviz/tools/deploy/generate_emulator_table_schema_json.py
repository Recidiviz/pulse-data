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
import argparse
import json
import logging
import os
from typing import Literal

import attr
from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SOURCE_TABLES_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
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


def _update_with_all_source_table_schemas(project_json: ProjectJSON) -> None:
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Getting source table schemas configs...")

    generator = build_source_table_repository_for_collected_schemata()
    for address, table in generator.source_tables.items():
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


def write_emulator_source_tables_json(project_id: str) -> str:
    project_json = ProjectJSON(id=BQ_EMULATOR_PROJECT_ID, datasets=[])
    with local_project_id_override(project_id):
        _update_with_all_source_table_schemas(project_json)

        if not os.path.exists(SOURCE_TABLES_FIXTURES_DIR):
            os.mkdir(SOURCE_TABLES_FIXTURES_DIR)

        with open(SOURCE_TABLES_JSON_PATH, "w", encoding="utf-8") as f:
            f.write(json.dumps({"projects": [attr.asdict(project_json)]}))

        logging.info("Wrote source table output to %s!", SOURCE_TABLES_JSON_PATH)

    return SOURCE_TABLES_JSON_PATH


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    known_args, _ = parser.parse_known_args()
    write_emulator_source_tables_json(known_args.project_id)
