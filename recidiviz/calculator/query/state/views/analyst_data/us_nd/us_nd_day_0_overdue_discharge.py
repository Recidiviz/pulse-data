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
"""US_ND - Overdue for Discharge Eligibility Criteria
    - Past projected end date
    - Not incarcerated
    - No pending violations
    - No other in-progress sentences"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_NAME = "us_nd_day_0_overdue_discharge"

US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_DESCRIPTION = (
    """Overdue for Discharge Eligibility Criteria for North Dakota"""
)

US_ND_DAY_0_OVERDUE_DISCHARGE_QUERY_TEMPLATE = """
    -- Past projected end date
WITH preliminary_eligibility AS (
    SELECT
        person_id,
        person_external_id,
        district_name,
        supervising_officer_external_id,
        date_of_supervision,
        projected_end_date,
        CONCAT(first_name, ' ', last_name) AS full_name,
    FROM `{project_id}.{analyst_dataset}.projected_discharges_materialized`
    WHERE state_code = 'US_ND'
        AND date_of_supervision >= projected_end_date
),
-- Limit to supervision
sessions AS (
    SELECT
        person_id,
        compartment_level_2,
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    WHERE state_code = 'US_ND'
        AND CURRENT_DATE('US/Central') BETWEEN start_date AND COALESCE(end_date, '9999-01-01')
        AND compartment_level_1 NOT LIKE '%INCARCERATION%'
),
-- most recent violation
most_recent_violation AS (
    SELECT
        person_id,
        response_date,
    FROM `{project_id}.{sessions_dataset}.violation_responses_materialized`
    WHERE state_code = 'US_ND'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY response_date DESC
    ) = 1
),
-- number of active/open sentences
sentence_count AS (
    SELECT
        person_id,
        COUNT(*) AS sentence_count,
    FROM `{project_id}.{state_dataset}.state_supervision_sentence`
    WHERE state_code = 'US_ND'
        AND CURRENT_DATE('US/Central') >= start_date
        AND completion_date IS NULL
    GROUP BY 1
)

SELECT
    pe.*,
    sessions.* EXCEPT (person_id),
    most_recent_violation.response_date AS violation_date,
    DATE_DIFF(pe.date_of_supervision, most_recent_violation.response_date, DAY) AS days_since_last_violation,
    sentence_count.sentence_count,
FROM preliminary_eligibility pe
-- Inner join to limit to SUPERVISION compartment_level_1
INNER JOIN sessions
    USING (person_id)
LEFT JOIN most_recent_violation
    USING (person_id)
LEFT JOIN sentence_count
    USING (person_id)
ORDER BY projected_end_date

"""

US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_NAME,
    description=US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_DESCRIPTION,
    view_query_template=US_ND_DAY_0_OVERDUE_DISCHARGE_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    state_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER.build_and_print()
