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
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view import (
    FederatedCloudSQLTableBigQueryViewBuilder,
)


class FederatedCloudSQLTableBigQueryViewCollector(
    BigQueryViewCollector[FederatedCloudSQLTableBigQueryViewBuilder]
):
    """Collects all FederatedCloudSQLTableBigQueryViewBuilders for the schema associated
    with the provided config.
    """

    def __init__(self, config: CloudSqlToBQConfig):
        self.config = config

    def collect_view_builders(self) -> List[FederatedCloudSQLTableBigQueryViewBuilder]:
        views = []
        for table in self.config.get_tables_to_export():
            materialized_address = self.config.materialized_address_for_table(table)
            views.append(
                FederatedCloudSQLTableBigQueryViewBuilder(
                    connection_region=self.config.connection_region,
                    table=table,
                    view_id=table.name,
                    cloud_sql_query=self.config.get_table_federated_export_query(
                        table.name
                    ),
                    database_key=self.config.database_key,
                    materialized_address_override=materialized_address,
                )
            )
        return views
