# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Export configuration.

By default, lists all tables in the various schema.py files to be exported by
cloud_sql_to_bq_export_manager.py for both COUNTY and STATE modules.

To exclude tables from export, add them to *_TABLES_TO_EXCLUDE_FROM_EXPORT.

Add columns to *_COLUMNS_TO_EXCLUDE to exclude them from the export.

*_BASE_TABLES_BQ_DATASET is the BigQuery dataset to export the tables to.

gcs_export_uri defines the export URI location in Google Cloud Storage.
"""
# pylint: disable=line-too-long
import logging
from typing import Dict, List

import sqlalchemy

from recidiviz.persistence.database import schema_utils
from recidiviz.utils import metadata

######### COUNTY EXPORT VALUES #########

COUNTY_TABLES_TO_EXCLUDE_FROM_EXPORT = tuple( # type: ignore
    # List tables to be excluded from export here. For example:
    # schema.FakePersonHistoryTable
)

# By default, all tables in the county/aggregate schema.py modules are exported
# unless listed in COUNTY_TABLES_TO_EXCLUDE_FROM_EXPORT above.
COUNTY_TABLES_TO_EXPORT = tuple(
    table for table in [*schema_utils.get_aggregate_table_classes(),
                        *schema_utils.get_county_table_classes()]
    if table not in COUNTY_TABLES_TO_EXCLUDE_FROM_EXPORT
)

# Mapping from table name to a list of columns to be excluded for that table.
COUNTY_COLUMNS_TO_EXCLUDE = {
    'person': ['full_name', 'birthdate_inferred_from_age']
}

######### STATE EXPORT VALUES #########

# History tables that should be included in the export
STATE_HISTORY_TABLES_TO_INCLUDE_IN_EXPORT = [
    'state_person_history'
]

# Excluding history tables
STATE_TABLES_TO_EXCLUDE_FROM_EXPORT = tuple( # type: ignore
    # List tables to be excluded from export here. For example:
    # schema.FakePersonHistoryTable
    table for table in schema_utils.get_state_table_classes()
    if 'history' in table.name and
    table.name not in STATE_HISTORY_TABLES_TO_INCLUDE_IN_EXPORT
)

# By default, all tables in the state schema.py module are exported
# unless listed in STATE_TABLES_TO_EXCLUDE_FROM_EXPORT above.
STATE_TABLES_TO_EXPORT = tuple(
    table for table in schema_utils.get_state_table_classes()
    if table not in STATE_TABLES_TO_EXCLUDE_FROM_EXPORT
)

# As of right now, we aren't excluding any columns from the state schema export.
STATE_COLUMNS_TO_EXCLUDE: Dict[str, List[str]] = {}

STATE_BASE_TABLES_BQ_DATASET = 'state'

######### OPERATIONS EXPORT VALUES #########

OPERATIONS_TABLES_TO_EXPORT = tuple(
    table for table in schema_utils.get_operations_table_classes()
)

def gcs_export_uri(table_name: str) -> str:
    """Return export URI location in Google Cloud Storage given a table name."""
    project_id = str(metadata.project_id())
    assert 'recidiviz' in project_id, (
        'If you are running a manual export, '
        'you must set the GOOGLE_CLOUD_PROJECT '
        'environment variable to specify which project bucket to export into.'
    )
    bucket = '{}-dbexport'.format(project_id)
    GCS_EXPORT_URI_FORMAT = 'gs://{bucket}/{table_name}.csv'
    uri = GCS_EXPORT_URI_FORMAT.format(bucket=bucket, table_name=table_name)
    logging.info("GCS URI [%s] in project [%s]", uri, project_id)
    return uri

############################################
### ONLY MODIFY VALUES ABOVE THIS BLOCK ###
############################################

BQ_TYPES = {
    sqlalchemy.Boolean: 'BOOL',
    sqlalchemy.Date: 'DATE',
    sqlalchemy.DateTime: 'DATETIME',
    sqlalchemy.Enum: 'STRING',
    sqlalchemy.Integer: 'INT64',
    sqlalchemy.String: 'STRING',
    sqlalchemy.Text: 'STRING'
}

TABLE_EXPORT_QUERY = 'SELECT {columns} FROM {table}'

### COUNTY VALUES ###

COUNTY_ALL_TABLE_COLUMNS = {
    table.name: [column.name for column in table.columns]
    for table in COUNTY_TABLES_TO_EXPORT
}

COUNTY_TABLE_COLUMNS_TO_EXPORT = {
    table_name: list(
        column for column in COUNTY_ALL_TABLE_COLUMNS.get(table_name)  # type: ignore
        if column not in COUNTY_COLUMNS_TO_EXCLUDE.get(table_name, [])
    )
    for table_name in COUNTY_ALL_TABLE_COLUMNS
}

COUNTY_TABLE_EXPORT_QUERIES = {
    table: TABLE_EXPORT_QUERY.format(columns=', '.join(columns), table=table)
    for table, columns in COUNTY_TABLE_COLUMNS_TO_EXPORT.items()
}

COUNTY_TABLE_EXPORT_SCHEMA = {
    table.name: [
        {'name': column.name,
         'type': BQ_TYPES[type(column.type)],
         'mode': 'NULLABLE' if column.nullable else 'REQUIRED'}
        for column in table.columns
        if column.name in COUNTY_TABLE_COLUMNS_TO_EXPORT[table.name]
    ]
    for table in COUNTY_TABLES_TO_EXPORT
}

### STATE VALUES ###

STATE_ALL_TABLE_COLUMNS = {
    table.name: [column.name for column in table.columns]
    for table in STATE_TABLES_TO_EXPORT
}

STATE_TABLE_COLUMNS_TO_EXPORT = {
    table_name: list(
        column for column in STATE_ALL_TABLE_COLUMNS.get(table_name)  # type: ignore
        if column not in STATE_COLUMNS_TO_EXCLUDE.get(table_name, [])
    )
    for table_name in STATE_ALL_TABLE_COLUMNS
}

STATE_TABLE_EXPORT_QUERIES = {
    table: TABLE_EXPORT_QUERY.format(columns=', '.join(columns), table=table)
    for table, columns in STATE_TABLE_COLUMNS_TO_EXPORT.items()
}

STATE_TABLE_EXPORT_SCHEMA = {
    table.name: [
        {'name': column.name,
         'type': BQ_TYPES[type(column.type)],
         'mode': 'NULLABLE' if column.nullable else 'REQUIRED'}
        for column in table.columns
        if column.name in STATE_TABLE_COLUMNS_TO_EXPORT[table.name]
    ]
    for table in STATE_TABLES_TO_EXPORT
}

### OPERATIONS VALUES ###

OPERATIONS_ALL_TABLE_COLUMNS = {
    table.name: [column.name for column in table.columns]
    for table in OPERATIONS_TABLES_TO_EXPORT
}

OPERATIONS_TABLE_COLUMNS_TO_EXPORT = {
    table_name: list(
        column for column in OPERATIONS_ALL_TABLE_COLUMNS.get(table_name)  # type: ignore
    )
    for table_name in OPERATIONS_ALL_TABLE_COLUMNS
}

OPERATIONS_TABLE_EXPORT_QUERIES = {
    table: TABLE_EXPORT_QUERY.format(columns=', '.join(columns), table=table)
    for table, columns in OPERATIONS_TABLE_COLUMNS_TO_EXPORT.items()
}

OPERATIONS_TABLE_EXPORT_SCHEMA = {
    table.name: [
        {'name': column.name,
         'type': BQ_TYPES[type(column.type)],
         'mode': 'NULLABLE' if column.nullable else 'REQUIRED'}
        for column in table.columns
        if column.name in OPERATIONS_TABLE_COLUMNS_TO_EXPORT[table.name]
    ]
    for table in OPERATIONS_TABLES_TO_EXPORT
}
