# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""View tracking officers' primary offices over time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_NAME = "supervision_officer_primary_office"

SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_DESCRIPTION = """
Determines each officer's primary office using the modal office among assigned clients.
This table takes snapshots at the start of each month to determine modal office.
"""

SUPERVISION_OFFICER_PRIMARY_OFFICE_QUERY_TEMPLATE = """
/*{description}*/

-- TODO(#15008): replace this with state-provided primary office in states where available

-- unnested date array starting at SUPERVISION_METRICS_START_DATE 
-- through the last day of the most recent complete month
WITH date_array AS (
    SELECT DISTINCT
        DATE_TRUNC(date, MONTH) AS date,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            (
                SELECT 
                    MIN(start_date)
                FROM 
                    `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized`
            ), DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
            INTERVAL 1 DAY
        )) AS date
)

-- determine each officer's primary office via modal office at start of each period
SELECT
    a.state_code,
    a.supervising_officer_external_id AS officer_id,
    b.date,
    c.supervision_district AS district,
    c.supervision_office AS office,
    IFNULL(COUNT(DISTINCT(a.person_id)), 0) AS caseload_primary,
FROM
    `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` a
INNER JOIN
    date_array b
ON
    b.date BETWEEN a.start_date AND IFNULL(a.end_date, "9999-01-01")
INNER JOIN
    `{project_id}.{sessions_dataset}.location_sessions_materialized` c
ON
    b.date BETWEEN c.start_date AND IFNULL(c.end_date, "9999-01-01")
    AND a.person_id = c.person_id
WHERE
    supervising_officer_external_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
QUALIFY
    -- keep only modal office
    -- district and office included in ORDER BY to break ties via alphabetical order
    ROW_NUMBER() OVER (PARTITION BY state_code, officer_id, date
        ORDER BY caseload_primary DESC, district, office              
    ) = 1
"""

SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_PRIMARY_OFFICE_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "officer_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_BUILDER.build_and_print()
