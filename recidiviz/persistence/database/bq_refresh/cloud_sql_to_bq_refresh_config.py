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
"""Export configuration. By default, exports all non-history tables from a given schema.

To update the configuration, take the following steps:
- Announce in #eng that you intend to change the config file for staging or prod [acquires pseudo-lock]
- Download file from gs://{project-name}-configs/cloud_sql_to_bq_config.yaml
- Update and re-upload file
- Announce in #eng that the change is complete [releases pseudo-lock]

The yaml file format for cloud_sql_to_bq_config.yaml is:

region_codes_to_exclude:
  - <list of state region codes>
state_history_tables_to_include:
  - <list of state history tables>
county_columns_to_exclude:
  <map with tables as keys>:
    - <list of columns for those tables to exclude>
"""
from typing import Dict, List, Optional

import yaml
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
from recidiviz.persistence.database.schema.justice_counts import (
    schema as justice_counts_schema,
)

from recidiviz.utils import environment
from recidiviz.persistence.database import base_schema
from recidiviz.utils import metadata
from recidiviz.calculator.query.county import dataset_config as county_dataset_config
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.calculator.query.operations import (
    dataset_config as operations_dataset_config,
)
from recidiviz.calculator.query.justice_counts import (
    dataset_config as justice_counts_dataset_config,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema_utils import (
    get_table_class_by_name,
    BQ_TYPES,
    get_region_code_col,
    schema_has_region_code_query_support,
    SchemaType,
)
from recidiviz.persistence.database.schema_table_region_filtered_query_builder import (
    CloudSqlSchemaTableRegionFilteredQueryBuilder,
    BigQuerySchemaTableRegionFilteredQueryBuilder,
)


class CloudSqlToBQConfig:
    """Configuration class for exporting tables from Cloud SQL to BigQuery
    Args:
        metadata_base: The base schema for the config
        dataset_id: String reference to the BigQuery dataset
        columns_to_exclude: An optional dictionary of table names and the columns
            to exclude from the export.
        history_tables_to_include: An optional List of history tables to include
            in the export.

    Usage:
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.JAILS)
        tables = config.get_tables_to_export()
    """

    def __init__(
        self,
        metadata_base: DeclarativeMeta,
        schema_type: SchemaType,
        dataset_id: str,
        columns_to_exclude: Optional[Dict[str, List[str]]] = None,
        history_tables_to_include: Optional[List[str]] = None,
        region_codes_to_exclude: Optional[List[str]] = None,
    ):
        if history_tables_to_include is None:
            history_tables_to_include = []
        if columns_to_exclude is None:
            columns_to_exclude = {}
        if region_codes_to_exclude is None:
            region_codes_to_exclude = []
        self.metadata_base = metadata_base
        self.schema_type = schema_type
        self.dataset_id = dataset_id
        self.sorted_tables: List[Table] = metadata_base.metadata.sorted_tables
        self.columns_to_exclude = columns_to_exclude
        self.history_tables_to_include = history_tables_to_include
        self.region_codes_to_exclude = region_codes_to_exclude

    def _get_tables_to_exclude(self) -> List[Optional[Table]]:
        """Return List of table classes to exclude from export"""
        if self.metadata_base == base_schema.StateBase:
            return list(
                table
                for table in self.sorted_tables
                if "history" in table.name
                and table.name not in self.history_tables_to_include
            )

        return []

    def _get_table_columns_to_export(self, table: Table) -> List[str]:
        """Return a List of column objects to export for a given table"""
        return list(
            column.name
            for column in table.columns
            if column not in self.columns_to_exclude.get(table.name, [])
        )

    def get_bq_schema_for_table(self, table_name: str) -> List[bigquery.SchemaField]:
        """Return a List of SchemaField objects for a given table name
        If it is an association table for a schema that includes the region code, a region code schema field
        is appended.
        """
        table = get_table_class_by_name(table_name, self.sorted_tables)
        columns = [
            bigquery.SchemaField(
                column.name,
                BQ_TYPES[type(column.type)],
                "NULLABLE" if column.nullable else "REQUIRED",
            )
            for column in table.columns
            if column.name in self._get_table_columns_to_export(table)
        ]

        if not schema_has_region_code_query_support(self.metadata_base):
            # We shouldn't add a region code column for this schema
            return columns

        column_names = {column.name for column in table.columns}
        region_code_col = get_region_code_col(self.metadata_base, table)
        if region_code_col in column_names:
            # We already have the region column we need in the table
            return columns

        columns.append(
            bigquery.SchemaField(
                region_code_col, BQ_TYPES[sqlalchemy.String], "NULLABLE"
            )
        )

        return columns

    def get_gcs_export_uri_for_table(self, table_name: str) -> str:
        """Return export URI location in Google Cloud Storage given a table name."""
        project_id = self._get_project_id()
        bucket = "{}-dbexport".format(project_id)
        gcs_export_uri_format = "gs://{bucket}/{table_name}.csv"
        uri = gcs_export_uri_format.format(bucket=bucket, table_name=table_name)
        return uri

    def get_table_export_query(self, table_name: str) -> str:
        """Return a formatted SQL query for a given table name

        For association tables, it adds a region code column to the select statement through a join.

        If there are region codes to exclude from the export, it either:
            1. Returns a query that excludes rows by region codes from the table
            2. Returns a query that joins an association table to its referred table to filter
                by region code. It also includes the region code column in the association table's
                select statement (i.e. state_code, region_code).

        """
        table = get_table_class_by_name(table_name, self.sorted_tables)
        columns = self._get_table_columns_to_export(table)
        query_builder = CloudSqlSchemaTableRegionFilteredQueryBuilder(
            self.metadata_base,
            table,
            columns,
            region_codes_to_exclude=self.region_codes_to_exclude,
        )
        return query_builder.full_query()

    def get_tables_to_export(self) -> List[Table]:
        """Return List of table classes to include in export"""
        return list(
            table
            for table in self.sorted_tables
            if table not in self._get_tables_to_exclude()
        )

    def get_dataset_ref(
        self, big_query_client: BigQueryClient
    ) -> Optional[bigquery.DatasetReference]:
        """Uses the dataset_id to request the BigQuery dataset reference to load the table into.
        Gets created if it does not already exist."""
        return big_query_client.dataset_ref_for_id(self.dataset_id)

    def get_stale_bq_rows_for_excluded_regions_query_builder(
        self, table_name: str
    ) -> BigQuerySchemaTableRegionFilteredQueryBuilder:
        table = get_table_class_by_name(table_name, self.sorted_tables)
        columns = self._get_table_columns_to_export(table)
        # Include region codes in query that were excluded from the export to GCS
        # If no region codes are excluded, an empty list will generate a query for zero rows.
        region_codes_to_include = (
            self.region_codes_to_exclude if self.region_codes_to_exclude else []
        )
        return BigQuerySchemaTableRegionFilteredQueryBuilder(
            project_id=self._get_project_id(),
            dataset_id=self.dataset_id,
            metadata_base=self.metadata_base,
            table=table,
            columns_to_include=columns,
            region_codes_to_include=region_codes_to_include,
        )

    @staticmethod
    def _get_project_id() -> str:
        project_id = metadata.project_id()
        if project_id not in {
            environment.GCP_PROJECT_STAGING,
            environment.GCP_PROJECT_PRODUCTION,
        }:
            raise ValueError(
                f"Unexpected project_id [{project_id}]. If you are running a manual export, "
                "you must set the GOOGLE_CLOUD_PROJECT environment variable to specify "
                "which project bucket to export into."
            )
        return project_id

    @classmethod
    def is_valid_schema_type(cls, schema_type: SchemaType) -> bool:
        if schema_type in (SchemaType.JAILS, SchemaType.STATE, SchemaType.OPERATIONS):
            return True
        if schema_type in (
            # Justice Counts views currently rely on federated queries directly to Postgres instead of this refresh.
            # TODO(#5081): Re-enable this once arrays are removed from the Justice Counts schema.
            SchemaType.JUSTICE_COUNTS,
            # Case Triage does not need to be exported to BigQuery
            SchemaType.CASE_TRIAGE,
        ):
            return False

        raise ValueError(f"Unexpected schema type value [{schema_type}]")

    @classmethod
    def for_schema_type(
        cls, schema_type: SchemaType, yaml_path: Optional[GcsfsFilePath] = None
    ) -> "CloudSqlToBQConfig":
        """Logic for instantiating a config object for a schema type."""
        if not cls.is_valid_schema_type(schema_type):
            raise ValueError(f"Unsupported schema_type: [{schema_type}]")

        gcs_fs = GcsfsFactory.build()
        if not yaml_path:
            yaml_path = GcsfsFilePath.from_absolute_path(
                f"gs://{cls._get_project_id()}-configs/cloud_sql_to_bq_config.yaml"
            )
        yaml_string = gcs_fs.download_as_string(yaml_path)
        try:
            yaml_config = yaml.safe_load(yaml_string)
        except yaml.YAMLError as e:
            raise ValueError(f"Could not parse YAML in [{yaml_path.abs_path()}]") from e

        if schema_type == SchemaType.JAILS:
            return CloudSqlToBQConfig(
                metadata_base=base_schema.JailsBase,
                schema_type=SchemaType.JAILS,
                dataset_id=county_dataset_config.COUNTY_BASE_DATASET,
                columns_to_exclude=yaml_config.get("county_columns_to_exclude", {}),
            )
        if schema_type == SchemaType.OPERATIONS:
            return CloudSqlToBQConfig(
                metadata_base=base_schema.OperationsBase,
                schema_type=SchemaType.OPERATIONS,
                region_codes_to_exclude=yaml_config.get("region_codes_to_exclude", []),
                dataset_id=operations_dataset_config.OPERATIONS_BASE_DATASET,
            )
        if schema_type == SchemaType.STATE:
            return CloudSqlToBQConfig(
                metadata_base=base_schema.StateBase,
                schema_type=SchemaType.STATE,
                dataset_id=state_dataset_config.STATE_BASE_DATASET,
                region_codes_to_exclude=yaml_config.get("region_codes_to_exclude", []),
                history_tables_to_include=yaml_config.get(
                    "state_history_tables_to_include", []
                ),
            )

        raise ValueError(f"Unexpected schema type value [{schema_type}]")
