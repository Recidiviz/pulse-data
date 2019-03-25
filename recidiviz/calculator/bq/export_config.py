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

By default, lists all tables in schema.py to be exported by export_manager.py.
To exclude tables from export, add them to TABLES_TO_EXCLUDE_FROM_EXPORT.

Add columns to COLUMNS_TO_EXCLUDE to exclude them from the export.

BASE_TABLES_BQ_DATASET is the BigQuery dataset to export the tables to.

gcs_export_uri defines the export URI location in Google Cloud Storage.
"""
import logging

import sqlalchemy

from recidiviz.persistence.database import schema
from recidiviz.utils import metadata


TABLES_TO_EXCLUDE_FROM_EXPORT = tuple( # type: ignore
    # List tables to be excluded from export here. For example:
    # schema.FakePersonHistoryTable
)

# By default, all tables in schema.py are exported
# unless listed in TABLES_TO_EXCLUDE_FROM_EXPORT above.
TABLES_TO_EXPORT = tuple(
    table.__table__ for table in
    schema.get_all_table_classes() if table not in TABLES_TO_EXCLUDE_FROM_EXPORT
)

# Mapping from table name to a list of columns to be excluded for that table.
COLUMNS_TO_EXCLUDE = {
    'person': ['full_name', 'birthdate', 'birthdate_inferred_from_age']
}

BASE_TABLES_BQ_DATASET = 'census'


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
    logging.info("GCS URI '%s' in project '%s'", uri, project_id)
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

ALL_TABLE_COLUMNS = {
    table.name: [column.name for column in table.columns]
    for table in TABLES_TO_EXPORT
}

TABLE_COLUMNS_TO_EXPORT = {
    table_name: list(
        column for column in ALL_TABLE_COLUMNS.get(table_name) # type: ignore
        if column not in COLUMNS_TO_EXCLUDE.get(table_name, [])
    )
    for table_name in ALL_TABLE_COLUMNS
}

TABLE_EXPORT_QUERY = 'SELECT {columns} FROM {table}'

TABLE_EXPORT_QUERIES = {
    table: TABLE_EXPORT_QUERY.format(columns=', '.join(columns), table=table)
    for table, columns in TABLE_COLUMNS_TO_EXPORT.items()
}

TABLE_EXPORT_SCHEMA = {
    table.name: [
        {'name': column.name,
         'type': BQ_TYPES[type(column.type)],
         'mode': 'NULLABLE' if column.nullable else 'REQUIRED'}
        for column in table.columns
        if column.name in TABLE_COLUMNS_TO_EXPORT[table.name]
    ]
    for table in TABLES_TO_EXPORT
}
