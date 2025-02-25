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

from recidiviz.entrypoints.sentencing.datasets import (
    CASE_INSIGHTS_RATES_ADDRESS,
    CASE_INSIGHTS_RATES_SCHEMA,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
)


def collect_sentencing_source_tables() -> list[SourceTableCollection]:
    """Collects sentencing tables that will be exported"""

    case_insights_config = SourceTableConfig(
        address=CASE_INSIGHTS_RATES_ADDRESS,
        schema_fields=[
            bigquery.SchemaField(field["name"], field["type"], field["mode"])
            for field in CASE_INSIGHTS_RATES_SCHEMA
        ],
        description="Case insights rates table for sentencing use",
    )

    case_insights_collection = SourceTableCollection(
        dataset_id=CASE_INSIGHTS_RATES_ADDRESS.dataset_id,
        update_config=SourceTableCollectionUpdateConfig.regenerable(),
        source_tables_by_address={case_insights_config.address: case_insights_config},
        description="Stores data calculated for sentencing views",
    )

    return [case_insights_collection]
