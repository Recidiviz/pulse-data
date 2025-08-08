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
"""Logic for generating a historical view of table-level hydration counts for the
normalized state dataset.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.source_tables.yaml_managed.datasets import HYDRATION_ARCHIVE
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

NORMALIZED_STATE_TABLE_HYDRATION_VIEW_ID = "normalized_state_table_hydration"
NORMALIZED_STATE_TABLE_HYDRATION_DESCRIPTION = "A historical view of table-level hydration counts for the normalized state dataset."

NORMALIZED_STATE_HYDRATION_ARCHIVE_VIEW = BigQueryAddress(
    dataset_id=HYDRATION_ARCHIVE, table_id="normalized_state_hydration_archive"
)

VIEW_QUERY = """
WITH 
-- exclude cases where we might have 2 exports on a single day
archive_deduped AS (
    SELECT 
        hydration_date,
        state_code,
        table_name
    FROM `{project_id}.{hydration_archive_dataset_id}.{normalized_state_hydration_archive_table_id}`
    WHERE state_code != "US_OZ"
    QUALIFY (
        ROW_NUMBER() OVER (
            PARTITION BY hydration_date, state_code, table_name
            ORDER BY hydration_date DESC
        ) = 1
    )
)
SELECT 
    hydration_date,
    state_code,
    count(table_name) as num_tables_hydrated
FROM archive_deduped
GROUP BY hydration_date, state_code
"""


NORMALIZED_STATE_TABLE_HYDRATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=NORMALIZED_STATE_TABLE_HYDRATION_VIEW_ID,
    description=NORMALIZED_STATE_TABLE_HYDRATION_DESCRIPTION,
    hydration_archive_dataset_id=NORMALIZED_STATE_HYDRATION_ARCHIVE_VIEW.dataset_id,
    normalized_state_hydration_archive_table_id=NORMALIZED_STATE_HYDRATION_ARCHIVE_VIEW.table_id,
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        NORMALIZED_STATE_TABLE_HYDRATION_VIEW_BUILDER.build_and_print()
