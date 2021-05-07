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
"""Classes to collect FederatedCloudSQLTableBigQueryViewBuilders for a given schema."""

from typing import List

from sqlalchemy import Table

from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
)
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.calculator.query.county.dataset_config import (
    COUNTY_BASE_REGIONAL_DATASET,
)
from recidiviz.calculator.query.justice_counts.dataset_config import (
    JUSTICE_COUNTS_BASE_REGIONAL_DATASET,
)
from recidiviz.case_triage.views.dataset_config import (
    CASE_TRIAGE_FEDERATED_REGIONAL_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view import (
    FederatedCloudSQLTableBigQueryViewBuilder,
)
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
)
from recidiviz.persistence.database.sqlalchemy_database_key import (
    SQLAlchemyDatabaseKey,
    SQLAlchemyStateDatabaseVersion,
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)


def is_state_segmented_refresh_schema(schema_type: SchemaType) -> bool:
    """Returns True if the data if the data for a particular state can and should be
    refreshed on a per-state basis.
    """
    if schema_type in (SchemaType.STATE, SchemaType.OPERATIONS):
        return True

    if schema_type in (
        SchemaType.JAILS,
        SchemaType.JUSTICE_COUNTS,
        SchemaType.CASE_TRIAGE,
    ):
        return False

    raise ValueError(f"Unexpected schema type [{schema_type}]")


class StateSegmentedSchemaFederatedBigQueryViewCollector(
    BigQueryViewCollector[FederatedCloudSQLTableBigQueryViewBuilder]
):
    """Collects all FederatedCloudSQLTableBigQueryViewBuilders for any schema whose
    CloudSQL -> BQ refresh should be segmented per-state.
    """

    def __init__(self, schema_type: SchemaType):
        if not is_state_segmented_refresh_schema(schema_type):
            raise ValueError(
                f"Only valid for state-segmented schema types. Cannot be instantiated "
                f"with schema_type [{schema_type}]"
            )
        self.schema_type = schema_type
        self.instance_region = SQLAlchemyEngineManager.get_cloudsql_instance_region(
            self.schema_type
        )

    def collect_view_builders(self) -> List[FederatedCloudSQLTableBigQueryViewBuilder]:
        config = CloudSqlToBQConfig.for_schema_type(self.schema_type)

        views = []
        for table in config.get_tables_to_export():
            for state_code in get_existing_direct_ingest_states():
                if state_code.value in config.region_codes_to_exclude:
                    continue
                database_key = self._database_key_for_segment(state_code)
                views.append(
                    FederatedCloudSQLTableBigQueryViewBuilder(
                        instance_region=self.instance_region,
                        table=table,
                        view_id=f"{state_code.value.lower()}_{table.name}",
                        database_key=database_key,
                        materialized_address_override=self.materialized_address_for_segment(
                            schema_type=self.schema_type,
                            table=table,
                            state_code=state_code,
                        ),
                        cloud_sql_query=config.get_single_state_table_federated_export_query(
                            table, state_code
                        ),
                    )
                )
        return views

    @staticmethod
    def materialized_address_for_segment(
        schema_type: SchemaType,
        table: Table,
        state_code: StateCode,
    ) -> BigQueryAddress:
        if not is_state_segmented_refresh_schema(schema_type):
            raise ValueError(f"Unexpected schema type [{schema_type}]")

        return BigQueryAddress(
            dataset_id=f"{state_code.value.lower()}_{schema_type.value.lower()}_regional",
            table_id=table.name,
        )

    def _database_key_for_segment(self, state_code: StateCode) -> SQLAlchemyDatabaseKey:
        if self.schema_type == SchemaType.STATE:
            # TODO(#6226): Cut over to query from primary state databases
            return SQLAlchemyDatabaseKey.for_state_code(
                state_code=state_code, db_version=SQLAlchemyStateDatabaseVersion.LEGACY
            )

        return SQLAlchemyDatabaseKey.for_schema(self.schema_type)


class UnsegmentedSchemaFederatedBigQueryViewCollector(
    BigQueryViewCollector[FederatedCloudSQLTableBigQueryViewBuilder]
):
    """Collects all FederatedCloudSQLTableBigQueryViewBuilders for any schema whose
    CloudSQL -> BQ refresh is not segmented by state or any other dimension.
    """

    def __init__(self, schema_type: SchemaType):
        if is_state_segmented_refresh_schema(schema_type):
            raise ValueError(
                f"Only valid for unsegmented schema types. Cannot be instantiated with "
                f"schema_type [{schema_type}]"
            )
        self.schema_type = schema_type
        # TODO(#7285): Migrate Justice Counts connection to be in same region as instance
        self.instance_region = (
            "US"
            if schema_type == SchemaType.JUSTICE_COUNTS
            else SQLAlchemyEngineManager.get_cloudsql_instance_region(self.schema_type)
        )

    def collect_view_builders(self) -> List[FederatedCloudSQLTableBigQueryViewBuilder]:
        config = CloudSqlToBQConfig.for_schema_type(self.schema_type)
        database_key = SQLAlchemyDatabaseKey.for_schema(self.schema_type)

        views = []
        for table in config.get_tables_to_export():
            views.append(
                FederatedCloudSQLTableBigQueryViewBuilder(
                    instance_region=self.instance_region,
                    table=table,
                    view_id=table.name,
                    cloud_sql_query=config.get_table_federated_export_query(table.name),
                    database_key=database_key,
                    materialized_address_override=self._materialized_address_for_table(
                        table
                    ),
                )
            )
        return views

    def _materialized_address_for_table(self, table: Table) -> BigQueryAddress:
        if self.schema_type == SchemaType.JAILS:
            return BigQueryAddress(
                dataset_id=COUNTY_BASE_REGIONAL_DATASET, table_id=table.name
            )
        if self.schema_type == SchemaType.JUSTICE_COUNTS:
            # TODO(#7285): JUSTICE_COUNTS has a custom materialized location for
            #  backwards compatibility. Once we delete the legacy views at
            #  `justice_counts.{table_name}` etc, we will be able to write materialized
            #  tables to that location.
            return BigQueryAddress(
                dataset_id=JUSTICE_COUNTS_BASE_REGIONAL_DATASET,
                table_id=f"{table.name}_materialized",
            )
        if self.schema_type == SchemaType.CASE_TRIAGE:
            return BigQueryAddress(
                dataset_id=CASE_TRIAGE_FEDERATED_REGIONAL_DATASET, table_id=table.name
            )

        raise ValueError(f"Unexpected schema type [{self.schema_type}]")
