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
from typing import Dict, List, Optional

import sqlalchemy
from sqlalchemy import Table
from sqlalchemy.ext.declarative import DeclarativeMeta
from google.cloud import bigquery
from recidiviz.big_query.big_query_client import BigQueryClient

# TODO(#4302): Remove unused schema module imports
# These imports are needed to ensure that the SQL Alchemy table metadata is loaded for the
# metadata_base in this class. We would like to eventually have a better
# way to achieve this and have the tables loaded alongside the DeclarativeMeta base class.
# pylint:disable=unused-import
from recidiviz.persistence.database.schema.operations import schema as operations_schema
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema.aggregate import schema as aggregate_schema
from recidiviz.persistence.database.schema.justice_counts import schema as justice_counts_schema

from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType
from recidiviz.utils.environment import GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION
from recidiviz.persistence.database import base_schema
from recidiviz.utils import metadata
from recidiviz.calculator.query.county import dataset_config as county_dataset_config
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.calculator.query.operations import dataset_config as operations_dataset_config
from recidiviz.calculator.query.justice_counts import dataset_config as justice_counts_dataset_config


class CloudSqlToBQConfig:
    """Configuration class for exporting tables from Cloud SQL to BigQuery
        Args:
            metadata_base: The base schema for the config
            dataset_ref: String reference to the BigQuery dataset
            columns_to_exclude: An optional dictionary of table names and the columns
                to exclude from the export.
            history_tables_to_include: An optional List of history tables to include
                in the export.

        Usage:
            config = CloudSqlToBQConfig.for_schema_type(SchemaType.JAILS)
            tables = config.get_tables_to_export()
    """
    def __init__(self,
                 metadata_base: DeclarativeMeta,
                 schema_type: SchemaType,
                 dataset_ref: str,
                 columns_to_exclude: Optional[Dict[str, List[str]]] = None,
                 history_tables_to_include: Optional[List[str]] = None):
        if history_tables_to_include is None:
            history_tables_to_include = []
        if columns_to_exclude is None:
            columns_to_exclude = {}
        self.metadata_base = metadata_base
        self.schema_type = schema_type
        self.dataset_ref = dataset_ref
        self.sorted_tables: List[Table] = metadata_base.metadata.sorted_tables
        self.columns_to_exclude = columns_to_exclude
        self.history_tables_to_include = history_tables_to_include

    def _get_table_class_by_name(self, table_name) -> Table:
        """Return a Table class object by its table_name"""
        for table in self.sorted_tables:
            if table.name == table_name:
                return table
        raise ValueError(f'{table_name}: Table name not found in the schema.')

    def _get_tables_to_exclude(self) -> List[Optional[Table]]:
        """Return List of table classes to exclude from export"""
        if self.metadata_base == base_schema.StateBase:
            return list(
                table for table in self.sorted_tables
                if 'history' in table.name and table.name not in self.history_tables_to_include
            )

        return []

    def _get_all_table_columns(self, table_name) -> List[str]:
        """Return a List of all column names for a given table name"""
        table = self._get_table_class_by_name(table_name)
        return [column.name for column in table.columns]

    def _get_table_columns_to_export(self, table_name) -> List[str]:
        """Return a List of column objects to export for a given table name"""
        return list(
            column for column in self._get_all_table_columns(table_name)
            if column not in self.columns_to_exclude.get(table_name, [])
        )

    def get_bq_schema_for_table(self, table_name: str) -> List[bigquery.SchemaField]:
        """Return a List of SchemaField objects for a given table name"""
        table = self._get_table_class_by_name(table_name)
        return [
            bigquery.SchemaField(
                column.name,
                BQ_TYPES[type(column.type)],
                'NULLABLE' if column.nullable else 'REQUIRED')
            for column in table.columns
            if column.name in self._get_table_columns_to_export(table_name)
        ]

    def get_gcs_export_uri_for_table(self, table_name: str) -> str:
        """Return export URI location in Google Cloud Storage given a table name."""
        project_id = metadata.project_id()
        if project_id not in {GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION}:
            raise ValueError(
                'Unexpected project_id [{project_id}]. If you are running a manual export, '
                'you must set the GOOGLE_CLOUD_PROJECT environment variable to specify '
                'which project bucket to export into.')
        bucket = '{}-dbexport'.format(project_id)
        GCS_EXPORT_URI_FORMAT = 'gs://{bucket}/{table_name}.csv'
        uri = GCS_EXPORT_URI_FORMAT.format(bucket=bucket, table_name=table_name)
        return uri

    def get_table_export_query(self, table_name: str) -> str:
        """Return a formatted SQL query for a given table name"""
        columns = self._get_table_columns_to_export(table_name)
        return TABLE_EXPORT_QUERY.format(columns=', '.join(columns), table=table_name)

    def get_tables_to_export(self) -> List[Table]:
        """Return List of table classes to include in export"""
        return list(
            table for table in self.sorted_tables
            if table not in self._get_tables_to_exclude()
        )

    def get_dataset_ref(self, big_query_client: BigQueryClient) -> Optional[bigquery.DatasetReference]:
        """Uses the dataset config reference to request the BigQuery dataset to load the table into.
            Gets created if it does not already exist."""
        return big_query_client.dataset_ref_for_id(self.dataset_ref)

    @classmethod
    def for_schema_type(cls, schema_type: SchemaType) -> 'CloudSqlToBQConfig':
        if schema_type == SchemaType.JAILS:
            return CloudSqlToBQConfig(
                metadata_base=base_schema.JailsBase,
                schema_type=SchemaType.JAILS,
                dataset_ref=county_dataset_config.COUNTY_BASE_DATASET,
                columns_to_exclude=COUNTY_COLUMNS_TO_EXCLUDE)
        if schema_type == SchemaType.JUSTICE_COUNTS:
            return CloudSqlToBQConfig(
                metadata_base=base_schema.JusticeCountsBase,
                schema_type=SchemaType.JUSTICE_COUNTS,
                dataset_ref=justice_counts_dataset_config.JUSTICE_COUNTS_BASE_DATASET)
        if schema_type == SchemaType.OPERATIONS:
            return CloudSqlToBQConfig(
                metadata_base=base_schema.OperationsBase,
                schema_type=SchemaType.OPERATIONS,
                dataset_ref=operations_dataset_config.OPERATIONS_BASE_DATASET)
        if schema_type == SchemaType.STATE:
            return CloudSqlToBQConfig(
                metadata_base=base_schema.StateBase,
                schema_type=SchemaType.STATE,
                dataset_ref=state_dataset_config.STATE_BASE_DATASET,
                history_tables_to_include=STATE_HISTORY_TABLES_TO_INCLUDE_IN_EXPORT)

        raise ValueError(f'Unexpected schema type value [{schema_type}]')


# Mapping from table name to a list of columns to be excluded for that table.
COUNTY_COLUMNS_TO_EXCLUDE = {
    'person': ['full_name', 'birthdate_inferred_from_age']
}

# History tables that should be included in the export
STATE_HISTORY_TABLES_TO_INCLUDE_IN_EXPORT = [
    'state_person_history'
]

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
    sqlalchemy.Text: 'STRING',
    sqlalchemy.ARRAY: 'ARRAY',
}

TABLE_EXPORT_QUERY = 'SELECT {columns} FROM {table}'
