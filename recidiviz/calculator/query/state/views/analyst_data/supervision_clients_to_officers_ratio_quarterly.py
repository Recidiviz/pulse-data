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
"""Ratio between supervision clients and officers - by quarter"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_NAME = (
    "supervision_clients_to_officers_ratio_quarterly"
)

SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_DESCRIPTION = """
Ratio between supervision clients and officers - by quarter
"""

SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_QUERY_TEMPLATE = """
-- Historical ratio of supervision clients/supervision officers
SELECT
state_code,
start_quarter,
DATE_ADD(start_quarter, INTERVAL 3 MONTH) AS end_quarter,
SAFE_DIVIDE(supervision_clients, supervision_officers) AS officer_to_client_ratio
FROM (
    SELECT
        state_code,
        start_quarter,
        COUNT(DISTINCT(os.supervising_officer_external_id)) AS supervision_officers,
        COUNT(DISTINCT os.person_id) AS supervision_clients
    FROM `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` os,
    -- An array that starts the first year someone was sent to SCCP
    UNNEST(GENERATE_DATE_ARRAY("1950-01-01", CURRENT_DATE('US/Eastern'), INTERVAL 1 QUARTER)) AS start_quarter
    WHERE start_quarter BETWEEN os.start_date AND COALESCE(os.end_date, CURRENT_DATE('US/Eastern'))
        -- TODO(#16408) we need to confirm who to subset here
        AND (os.state_code != "US_ME" OR os.supervising_officer_external_id != "0")
    GROUP BY 1,2
    )
ORDER BY 1,2
"""

SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_NAME,
    view_query_template=SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_QUERY_TEMPLATE,
    description=SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_DESCRIPTION,
    should_materialize=True,
    sessions_dataset=SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_BUILDER.build_and_print()
