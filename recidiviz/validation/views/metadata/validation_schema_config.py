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
"""Classes containing dataset schema information."""
import os
from typing import List

import attr

from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.validation.views.metadata import config


@attr.s(frozen=True)
class TableSchemaInfo:
    """Information about a single table in a dataset."""

    # Name of the BigQuery table
    table_name: str = attr.ib()

    # List of column names
    columns: List[str] = attr.ib(factory=list)


@attr.s(frozen=True)
class DatasetSchemaInfo:
    """Information about a dataset and its tables."""

    # Name of the BigQuery dataset where this schema resides
    dataset: str = attr.ib()

    # Mapping of table names to lists of column names
    tables: List[TableSchemaInfo] = attr.ib(default=[])

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "DatasetSchemaInfo":
        yaml_contents = YAMLDict.from_path(yaml_path)

        dataset = yaml_contents.pop("dataset", str)

        yaml_tables = yaml_contents.pop_dicts("tables")
        tables: List[TableSchemaInfo] = []

        for table_dict in yaml_tables:
            table_name = table_dict.pop("name", str)
            columns = table_dict.pop("columns", list)
            tables.append(
                TableSchemaInfo(
                    table_name=table_name,
                    columns=sorted([c.lower() for c in columns]),
                )
            )

        return DatasetSchemaInfo(
            dataset=dataset,
            tables=tables,
        )


def get_external_validation_schema() -> DatasetSchemaInfo:
    """Returns a DatasetSchemaInfo object for the external accuracy dataset."""
    return DatasetSchemaInfo.from_yaml(
        os.path.join(
            os.path.dirname(config.__file__),
            "validation_external_accuracy_schema.yaml",
        )
    )
