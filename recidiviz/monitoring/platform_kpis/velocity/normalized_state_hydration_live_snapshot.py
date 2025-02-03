# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Logic for generating a live snapshot of hydration percentage for the normalized state
dataset.
"""
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID = (
    "normalized_state_hydration_live_snapshot"
)
NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_DESCRIPTION = (
    "A live, point-in-time snapshot of hydration percentage of the normalized_state "
    "dataset by state_code. This view will only reflect the CURRENT state of the world "
    "as of the last view update. To see an archival version of this view, see "
    "`hydration_archive.normalized_state_hydration_archive`."
)

SINGLE_COLUMN_COUNT_TEMPLATE = """      STRUCT('{column_name}' AS column_name, COUNTIF({column_name} IS NOT NULL) AS hydrated_count)"""

# n.b. if you are changing this schema, be sure to also update the yaml-managed config
# for `normalized_state_hydration_archive` as a scheduled BQ job will read from this
# view and append to that table!
SINGLE_TABLE_TEMPLATE = """
SELECT 
    state_code, 
    '{table_name}' as table_name, 
    count(*) as entity_count,
    [
{column_counts}
    ] as column_hydration
FROM `{{project_id}}.{{normalized_state_dataset_id}}.{table_name}`
GROUP BY state_code, table_name
"""


def _build_query_for_table(*, table_name: str, table_fields: list[SchemaField]) -> str:
    column_counts = ",\n".join(
        StrictStringFormatter().format(
            SINGLE_COLUMN_COUNT_TEMPLATE,
            column_name=field.name,
        )
        for field in table_fields
    )

    return StrictStringFormatter().format(
        SINGLE_TABLE_TEMPLATE, table_name=table_name, column_counts=column_counts
    )


def get_normalized_state_hydration_live_snapshot_view_builder() -> BigQueryViewBuilder:
    all_normalized_state_tables = get_bq_schema_for_entities_module(normalized_entities)

    view_query = "\nUNION ALL\n".join(
        _build_query_for_table(table_name=table_name, table_fields=table_fields)
        for table_name, table_fields in all_normalized_state_tables.items()
    )

    return SimpleBigQueryViewBuilder(
        view_query_template=view_query,
        dataset_id=PLATFORM_KPIS_DATASET,
        view_id=NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
        description=NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_DESCRIPTION,
        normalized_state_dataset_id=NORMALIZED_STATE_DATASET,
        # this table is just a pass through for the bq schedule query, so we shouldn't
        # materialize this view more than is needed
        should_materialize=False,
    )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_normalized_state_hydration_live_snapshot_view_builder().build_and_print()
