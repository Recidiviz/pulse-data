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
"""US_ND - Early Termination Eligibility"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_DAY_0_EARLY_TERMINATION_VIEW_NAME = "us_nd_day_0_early_termination"

US_ND_DAY_0_EARLY_TERMINATION_VIEW_DESCRIPTION = (
    """Early Termination Eligibility Criteria for North Dakota"""
)

US_ND_DAY_0_EARLY_TERMINATION_QUERY_TEMPLATE = """
WITH early_term_data_raw AS (
    SELECT *
    FROM (
        SELECT
            SID AS person_external_id,
            UPPER(FIRST) AS first_name,
            UPPER(LAST_NAME) AS last_name,
            CAST(PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', EARLY_TERMINATION_DATE) AS DATE) AS early_termination_date,
            EARLY_TERMINATION_ACKNOWLEDGED,
        FROM `{project_id}.{us_nd_raw_data_up_to_date_dataset}.docstars_offenders_latest`
    )
    WHERE early_termination_date < DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 7 DAY)
),
sessions AS (
    SELECT
        person_id,
        external_id AS person_external_id,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    INNER JOIN `{project_id}.{state_dataset}.state_person_external_id`
        USING (state_code, person_id)
    WHERE state_code = 'US_ND'
        AND id_type = "US_ND_SID" 
        AND end_date IS NULL
        AND compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
        AND compartment_level_2 = 'PROBATION'
),
violations AS (
    SELECT DISTINCT person_id,
    FROM `{project_id}.{sessions_dataset}.violation_responses_materialized`
    WHERE state_code = 'US_ND'
        AND response_date >= DATE_SUB(CURRENT_DATE('US/Central'), INTERVAL 6 MONTH)
)

SELECT *
FROM early_term_data_raw
-- limits to people currently on probation
INNER JOIN sessions
    USING (person_external_id)
LEFT JOIN violations
    USING (person_id)
-- excludes people with violation in past 6-months
WHERE violations.person_id IS NULL
"""

US_ND_DAY_0_EARLY_TERMINATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=US_ND_DAY_0_EARLY_TERMINATION_VIEW_NAME,
    dataset_id=ANALYST_VIEWS_DATASET,
    description=US_ND_DAY_0_EARLY_TERMINATION_VIEW_DESCRIPTION,
    view_query_template=US_ND_DAY_0_EARLY_TERMINATION_QUERY_TEMPLATE,
    state_dataset=STATE_BASE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_nd"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_DAY_0_EARLY_TERMINATION_VIEW_BUILDER.build_and_print()
