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

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view import (
    FederatedCloudSQLTableBigQueryViewBuilder,
)


class StateSegmentedSchemaFederatedBigQueryViewCollector(
    BigQueryViewCollector[FederatedCloudSQLTableBigQueryViewBuilder]
):
    """Collects all FederatedCloudSQLTableBigQueryViewBuilders for any schema whose
    CloudSQL -> BQ refresh should be segmented per-state.
    """

    def __init__(self, config: CloudSqlToBQConfig):
        if not config.is_state_segmented_refresh_schema():
            raise ValueError(
                f"Only valid for state-segmented schema types. Cannot be instantiated "
                f"with schema_type [{config.schema_type}]"
            )
        self.config = config
        self.state_codes_to_collect = [
            state_code
            for state_code in get_existing_direct_ingest_states()
            if state_code.value not in self.config.region_codes_to_exclude
        ]

    def collect_view_builders(self) -> List[FederatedCloudSQLTableBigQueryViewBuilder]:
        views = []
        for table in self.config.get_tables_to_export():
            for state_code in self.state_codes_to_collect:
                database_key = self.config.database_key_for_segment(state_code)
                cloud_sql_query = (
                    self.config.get_single_state_table_federated_export_query(
                        table, state_code
                    )
                )
                materialized_address = (
                    self.config.materialized_address_for_segment_table(
                        table=table,
                        state_code=state_code,
                    )
                )
                views.append(
                    FederatedCloudSQLTableBigQueryViewBuilder(
                        connection_region=self.config.connection_region,
                        table=table,
                        view_id=f"{state_code.value.lower()}_{table.name}",
                        database_key=database_key,
                        materialized_address_override=materialized_address,
                        cloud_sql_query=cloud_sql_query,
                    )
                )
        return views


class UnsegmentedSchemaFederatedBigQueryViewCollector(
    BigQueryViewCollector[FederatedCloudSQLTableBigQueryViewBuilder]
):
    """Collects all FederatedCloudSQLTableBigQueryViewBuilders for any schema whose
    CloudSQL -> BQ refresh is not segmented by state or any other dimension.
    """

    def __init__(self, config: CloudSqlToBQConfig):
        if config.is_state_segmented_refresh_schema():
            raise ValueError(
                f"Only valid for unsegmented schema types. Cannot be instantiated with "
                f"schema_type [{config.schema_type}]"
            )
        self.config = config

    def collect_view_builders(self) -> List[FederatedCloudSQLTableBigQueryViewBuilder]:
        views = []
        for table in self.config.get_tables_to_export():
            materialized_address = (
                self.config.materialized_address_for_unsegmented_table(table)
            )
            views.append(
                FederatedCloudSQLTableBigQueryViewBuilder(
                    connection_region=self.config.connection_region,
                    table=table,
                    view_id=table.name,
                    cloud_sql_query=self.config.get_table_federated_export_query(
                        table.name
                    ),
                    database_key=self.config.unsegmented_database_key,
                    materialized_address_override=materialized_address,
                )
            )
        return views
