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
from recidiviz.calculator.query.state.dataset_config import SENTENCING_DATASET
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableConfig,
)

CASE_INSIGHTS_RATES_ADDRESS = BigQueryAddress(
    dataset_id=SENTENCING_DATASET, table_id="case_insights_rates"
)
# TODO(#40788): Deprecate probation/rider/term-specific columns
CASE_INSIGHTS_RATES_SCHEMA = [
    {"name": "state_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "gender", "type": "STRING", "mode": "REQUIRED"},
    {"name": "assessment_score_bucket_start", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "assessment_score_bucket_end", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "most_severe_description", "type": "STRING", "mode": "REQUIRED"},
    {"name": "recidivism_rollup", "type": "STRING", "mode": "REQUIRED"},
    {"name": "recidivism_num_records", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "recidivism_probation_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "recidivism_rider_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "recidivism_term_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "recidivism_series", "type": "STRING", "mode": "NULLABLE"},
    {"name": "disposition_num_records", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "disposition_probation_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "disposition_rider_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "disposition_term_pc", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "dispositions", "type": "STRING", "mode": "REQUIRED"},
]


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
