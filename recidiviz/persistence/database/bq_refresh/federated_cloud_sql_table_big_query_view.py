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
"""View and view builder classes for generated federated queries against CloudSQL
tables.
"""

from typing import Dict, Optional

from sqlalchemy import Table

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils.string import StrictStringFormatter

TABLE_QUERY_TEMPLATE = """/*{description}*/
SELECT
    *
FROM EXTERNAL_QUERY(
    "{{project_id}}.{connection_region}.{connection_name}",
    "{cloud_sql_query}"
)"""

DESCRIPTION_TEMPLATE = """View providing a connection to the [{cloudsql_table_name}]
table in the [{database_name}] database in the [{schema_name}] schema. This view is 
managed outside of regular view update operations and the results can be found in the 
schema-specific datasets (`state`, `jails`, `justice_counts`, etc)."""


class FederatedCloudSQLTableBigQueryView(BigQueryView):
    """View that queries a SQLAlchemy schema table in CloudSQL from BigQuery, using a
    `EXTERNAL_QUERY` federated query.

    All materialized table locations may be overridden to live outside the
    dataset so that we can restrict actual CloudSQL query permissions separately from
    the permissions on the queried data.
    """

    def __init__(
        self,
        dataset_id: str,
        connection_region: str,
        connection_name: str,
        table: Table,
        view_id: str,
        cloud_sql_query: str,
        database_key: SQLAlchemyDatabaseKey,
        materialized_address_override: BigQueryAddress,
        dataset_overrides: Optional[Dict[str, str]] = None,
    ):
        description = StrictStringFormatter().format(
            DESCRIPTION_TEMPLATE,
            cloudsql_table_name=table.name,
            database_name=database_key.db_name,
            schema_name=database_key.schema_type.value,
        )

        super().__init__(
            dataset_id=dataset_id,
            view_id=view_id,
            description=description,
            should_materialize=True,
            materialized_address_override=materialized_address_override,
            dataset_overrides=dataset_overrides,
            view_query_template=TABLE_QUERY_TEMPLATE,
            # View query template args
            connection_region=connection_region,
            connection_name=connection_name,
            cloud_sql_query=cloud_sql_query,
        )


class FederatedCloudSQLTableBigQueryViewBuilder(
    BigQueryViewBuilder[FederatedCloudSQLTableBigQueryView]
):
    """View builder for FederatedCloudSQLTableBigQueryView"""

    def __init__(
        self,
        connection_region: str,
        table: Table,
        view_id: str,
        database_key: SQLAlchemyDatabaseKey,
        cloud_sql_query: str,
        materialized_address_override: BigQueryAddress,
    ):
        self.connection_region = connection_region
        self.table = table
        self.view_id = view_id
        self.database_key = database_key
        self.materialized_address_override = materialized_address_override
        self.connection_name = self._connection_name_for_database(self.database_key)
        self.dataset_id = f"{self.connection_name}_connection"
        self.cloud_sql_query = cloud_sql_query

    @staticmethod
    def _connection_name_for_database(database_key: SQLAlchemyDatabaseKey) -> str:
        # TODO(#8282): Remove this custom v2 logic when we're in the Phase 5 cleanup.
        if database_key.schema_type == SchemaType.STATE:
            return f"{database_key.schema_type.value.lower()}_v2_{database_key.db_name}_cloudsql"
        if database_key.schema_type in (SchemaType.JAILS, SchemaType.OPERATIONS):
            return f"{database_key.schema_type.value.lower()}_v2_cloudsql"

        if database_key.is_default_db:
            return f"{database_key.schema_type.value.lower()}_cloudsql"

        return (
            f"{database_key.schema_type.value.lower()}_{database_key.db_name}_cloudsql"
        )

    def _build(
        self, *, dataset_overrides: Optional[Dict[str, str]] = None
    ) -> FederatedCloudSQLTableBigQueryView:
        if self.materialized_address_override is None:
            raise ValueError("The materialized_address_override field cannot be None.")
        return FederatedCloudSQLTableBigQueryView(
            dataset_id=self.dataset_id,
            connection_region=self.connection_region,
            connection_name=self.connection_name,
            table=self.table,
            view_id=self.view_id,
            cloud_sql_query=self.cloud_sql_query,
            database_key=self.database_key,
            materialized_address_override=self.materialized_address_override,
            dataset_overrides=dataset_overrides,
        )

    def should_build(self) -> bool:
        return True
