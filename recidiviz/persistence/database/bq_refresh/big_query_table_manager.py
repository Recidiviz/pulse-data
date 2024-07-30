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
"""Manages the structure of BigQuery tables that share a schema with one of the
SQLAlchemy schemas defined in a schema.py file.
Used during deploy time to update the schema of the BigQuery datasets so that they
match that of the schema being deployed before the next CloudSqlToBQ export or data
pipelines attempt to write these datasets. Does not perform any migrations, only adds
and deletes columns where necessary.
"""
from typing import List

from google.cloud.bigquery import SchemaField
from sqlalchemy import Table

from recidiviz.big_query.big_query_utils import schema_for_sqlalchemy_table
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    is_association_table,
    schema_has_region_code_query_support,
)


def bq_schema_for_sqlalchemy_table(
    schema_type: SchemaType, table: Table
) -> List[SchemaField]:
    """Derives a BigQuery table schema from a SQLAlchemy table. Adds region code columns
    to any association table.
    """
    add_state_code_field = schema_has_region_code_query_support(
        schema_type
    ) and is_association_table(table.name)

    return schema_for_sqlalchemy_table(table, add_state_code_field=add_state_code_field)
