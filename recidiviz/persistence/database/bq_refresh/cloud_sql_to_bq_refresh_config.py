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
"""Configuration class for exporting tables from Cloud SQL to BigQuery."""
from typing import List, Optional

from sqlalchemy import Table

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.operations.dataset_config import (
    OPERATIONS_BASE_DATASET,
    OPERATIONS_BASE_REGIONAL_DATASET,
)
from recidiviz.case_triage.views.dataset_config import (
    CASE_TRIAGE_FEDERATED_DATASET,
    CASE_TRIAGE_FEDERATED_REGIONAL_DATASET,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_query_builder import (
    FederatedCloudSQLTableQueryBuilder,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_table_class_by_name,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


class CloudSqlToBQConfig:
    """Configuration class for exporting tables from Cloud SQL to BigQuery
    Args:
        schema_type: The schema to export.

    Usage:
        config = CloudSqlToBQConfig.for_schema_type(SchemaType.OPERATIONS)
        tables = config.get_tables_to_export()
    """

    def __init__(self, schema_type: SchemaType):
        self.schema_type = schema_type
        self.sorted_tables = list(get_all_table_classes_in_schema(schema_type))

    def regional_dataset(self, dataset_override_prefix: Optional[str]) -> str:
        """Returns the name of the dataset where refreshed data should be written right
        before it is copied to its final destination in the multi-region dataset that
        can be referenced by views.

        This dataset will have a region that matches the region of the schema's CloudSQL
        instance, e.g us-east1.
        """
        if self.schema_type == SchemaType.OPERATIONS:
            dest_dataset = OPERATIONS_BASE_REGIONAL_DATASET
        elif self.schema_type == SchemaType.CASE_TRIAGE:
            dest_dataset = CASE_TRIAGE_FEDERATED_REGIONAL_DATASET
        else:
            raise ValueError(f"Unexpected schema_type [{self.schema_type}].")

        if dataset_override_prefix:
            dest_dataset = f"{dataset_override_prefix}_{dest_dataset}"
        return dest_dataset

    def multi_region_dataset(self, dataset_override_prefix: Optional[str]) -> str:
        """Returns the name of the dataset that is the final destination for refreshed
        data. This dataset will live in the 'US' mult-region and can be referenced by
        our views.
        """
        if self.schema_type == SchemaType.OPERATIONS:
            dest_dataset = OPERATIONS_BASE_DATASET
        elif self.schema_type == SchemaType.CASE_TRIAGE:
            dest_dataset = CASE_TRIAGE_FEDERATED_DATASET
        else:
            raise ValueError(f"Unexpected schema_type [{self.schema_type}].")

        if dataset_override_prefix:
            dest_dataset = f"{dataset_override_prefix}_{dest_dataset}"
        return dest_dataset

    def multi_region_dataset_description(self) -> str:
        if self.schema_type == SchemaType.OPERATIONS:
            return (
                "Internal Recidiviz operations data loaded from the operations "
                "CloudSQL instance."
            )
        if self.schema_type == SchemaType.CASE_TRIAGE:
            return "Case Triage data loaded from the case-triage CloudSQL instance."
        raise ValueError(f"Unexpected schema_type [{self.schema_type}].")

    @property
    def database_key(self) -> SQLAlchemyDatabaseKey:
        """Returns a key for the database associated with a particular schema."""
        return SQLAlchemyDatabaseKey.for_schema(self.schema_type)

    def materialized_address_for_table(self, table: Table) -> BigQueryAddress:
        """Returns the dataset that data for a given table in a schema is materialized
        into.
        """
        dataset = self.regional_dataset(dataset_override_prefix=None)
        return BigQueryAddress(dataset_id=dataset, table_id=table.name)

    @property
    def connection_region(self) -> str:
        """Returns the region of the BigQuery CloudSQL external connection."""
        # TODO(#7285): Migrate Justice Counts connection to be in same region as instance
        return (
            "US"
            if self.schema_type == SchemaType.JUSTICE_COUNTS
            else SQLAlchemyEngineManager.get_cloudsql_instance_region(self.schema_type)
        )

    def _get_table_columns_to_export(self, table: Table) -> List[str]:
        """Return a List of column objects to export for a given table"""
        return list(column.name for column in table.columns)

    def get_table_federated_export_query(self, table_name: str) -> str:
        """Return a formatted SQL query for a given CloudSQL schema table that can be
        used to export data for a given state to BigQuery via a federated query.
        """
        table = get_table_class_by_name(table_name, self.schema_type)
        columns = self._get_table_columns_to_export(table)

        query_builder = FederatedCloudSQLTableQueryBuilder(
            schema_type=self.schema_type, table=table, columns_to_include=columns
        )
        return query_builder.full_query()

    def get_tables_to_export(self) -> List[Table]:
        """Return List of table classes to include in export"""
        return list(self.sorted_tables)

    @classmethod
    def is_valid_schema_type(cls, schema_type: SchemaType) -> bool:
        if schema_type in (
            SchemaType.OPERATIONS,
            SchemaType.CASE_TRIAGE,
        ):
            return True
        if schema_type in (
            SchemaType.JUSTICE_COUNTS,
            SchemaType.PATHWAYS,
            SchemaType.STATE,
            SchemaType.WORKFLOWS,
            SchemaType.INSIGHTS,
            SchemaType.RESOURCE_SEARCH,
        ):
            return False

        raise ValueError(f"Unexpected schema type value [{schema_type}]")

    @classmethod
    def for_schema_type(cls, schema_type: SchemaType) -> "CloudSqlToBQConfig":
        """Logic for instantiating a config object for a schema type."""
        if not cls.is_valid_schema_type(schema_type):
            raise ValueError(f"Unsupported schema_type: [{schema_type}]")

        return CloudSqlToBQConfig(schema_type=schema_type)
