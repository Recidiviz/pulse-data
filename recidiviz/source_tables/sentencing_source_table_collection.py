# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Contains source table definitions for sentencing views."""
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.entrypoints.sentencing.write_case_insights_data_to_bq import (
    return_schema,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
)
from recidiviz.view_registry.datasets import SENTENCING_DATASET


def collect_sentencing_source_tables() -> list[SourceTableCollection]:
    """Collects sentencing tables that will be exported"""

    dataset_id = SENTENCING_DATASET
    table_address = BigQueryAddress(
        dataset_id=dataset_id, table_id="case_insights_rates"
    )
    case_insights_config = SourceTableConfig(
        address=table_address,
        schema_fields=[
            bigquery.SchemaField(field["name"], field["type"], field["mode"])
            for field in return_schema()
        ],
        description="Case insights rates table for sentencing use",
    )

    case_insights_collection = SourceTableCollection(
        dataset_id=dataset_id,
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        source_tables_by_address={table_address: case_insights_config},
    )

    return [case_insights_collection]
