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

"""Export data from Cloud SQL and load it into BigQuery."""

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.persistence.database.bq_refresh import (
    bq_refresh,
    cloud_sql_to_gcs_export,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)

from recidiviz.persistence.database.schema_utils import SchemaType

# TODO(#7397): Delete this whole file once federated export ships to production.
def export_table_then_load_table(
    big_query_client: BigQueryClient,
    table: str,
    schema_type: SchemaType,
) -> None:
    """Exports a Cloud SQL table to CSV, then loads it into BigQuery.

    If a table excludes some region codes, it first loads all the GCS and the excluded region's data to a temp table.
    See for details: load_table_with_excluded_regions

    Waits until the BigQuery load is completed.

    Args:
        big_query_client: A BigQueryClient.
        table: Table to export then import. Table must be defined
            in the metadata_base class for its corresponding SchemaType.
        schema_type: The SchemaType this table belongs to.
    Returns:
        True if load succeeds, else False.
    """
    cloud_sql_to_bq_config = CloudSqlToBQConfig.for_schema_type(schema_type)
    export_success = cloud_sql_to_gcs_export.export_table(table, cloud_sql_to_bq_config)

    if not export_success:
        raise ValueError(
            f"Failure to export CloudSQL table to GCS, skipping BigQuery load of table [{table}]."
        )

    bq_refresh.refresh_bq_table_from_gcs_export_synchronous(
        big_query_client, table, cloud_sql_to_bq_config
    )
