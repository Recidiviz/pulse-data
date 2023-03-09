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

"""Defines a criteria view that shows spans of time for
which clients are 30/24 months away from expected release date. When
average state-wide case load is lower than 90, residents are allowed
to be released within their last 30 months ; otherwise, residents are only
allowed to be released 24 months before their sentence ends.

Since folks can start their paperwork 3 months before they are 30/24 months
away from their release date, we differentiate
between the time people can start their paperwork ("critical_date":
3 months before they are 30/24 months away) and the actual time when
they can be released from prison (real_eligible_date: when they are 30/24
months away from their release date)
"""

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.raw_table_import import cis_319_after_csswa
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_X_MONTHS_REMAINING_ON_SENTENCE"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which clients are 30/24 months away from expected release date. When
average state-wide case load is lower than 90, residents are allowed
to be released within their last 30 months ; otherwise, residents are only
allowed to be released 24 months before their sentence ends.

Since folks can start their paperwork 3 months before they are 30/24 months
away from their release date, we differentiate
between the time people can start their paperwork ("critical_date":
3 months before they are 30/24 months away) and the actual time when
they can be released from prison (real_eligible_date: when they are 30/24
months away from their release date)
"""

_QUERY_TEMPLATE = f"""
WITH term_with_caseload AS (
-- Combine case load with term data
    SELECT 
        cl.state_code,
        person_id,
        GREATEST({nonnull_start_date_clause('tc.start_date')}, 
                 cl.start_quarter) AS start_date,
        LEAST({nonnull_end_date_clause('tc.end_date')}, 
                 cl.end_quarter) AS end_date,
        cl.officer_to_client_ratio AS case_load,
        end_date AS release_date,
        status,
        term_id,
    FROM `{{project_id}}.{{analyst_dataset}}.us_me_sentence_term_materialized` tc
    INNER JOIN `{{project_id}}.{{analyst_dataset}}.supervision_clients_to_officers_ratio_quarterly_materialized` cl
        ON cl.state_code = tc.state_code
        AND {nonnull_start_date_clause('tc.start_date')} < COALESCE(cl.end_quarter, 
                                                                    CURRENT_DATE('US/Eastern'))
        AND cl.start_quarter < {nonnull_end_date_clause('tc.end_date')}
),
term_crit_date AS (
-- Calculate the critical date as a function of the statewide case load
    SELECT 
        * EXCEPT(case_load, release_date),
        IF(start_date < '2021-10-18',
        -- Pre-reform: 18 months if caseload is more than 90, 24 months otherwise        
            DATE_SUB(release_date, INTERVAL IF(case_load > 90, 18, 24) MONTH),
        -- Post-reform: 24 months if caseload is more than 90, 30 months otherwise
            DATE_SUB(release_date, INTERVAL IF(case_load > 90, 24, 30) MONTH))
        AS critical_date,
    FROM term_with_caseload
),

term_crit_date_plus_real AS(
    -- Folks can start their paperwork 3 months before their eligibility date,
    -- so we save the actual eligibility date, but let the critical_date be 
    -- the eligibility date - 3 months
    SELECT
        * EXCEPT (critical_date),
        critical_date AS real_eligible_date,
        DATE_SUB(critical_date, INTERVAL 3 MONTH) AS critical_date,
    FROM term_crit_date
    WHERE start_date != {nonnull_end_date_clause('end_date')}
),
-- Create sub-sessions w/attributes
{create_sub_sessions_with_attributes('term_crit_date_plus_real')},

critical_date_spans AS (
    -- Drop additional repeated subsessions: if concurrent keep the longest one, drop
    --   completed sessions over active ones
    {cis_319_after_csswa()}
),

save_real_eligible_date AS (
    -- Save real_eligible_date so we could send it in JSON later
    SELECT
        DISTINCT person_id, state_code, critical_date, real_eligible_date
    FROM critical_date_spans
),

-- Critical date has passed
{critical_date_has_passed_spans_cte()}
SELECT
    cd.state_code,
    cd.person_id,
    CASE
        WHEN (start_date IS NULL) AND (critical_date_has_passed) THEN cd.critical_date
                                -- When there was no intake date in us_me_sentence_term,
                                -- start_date of our subsession is NULL for the
                                -- period for which the criteria is met. But
                                -- we know the end date and we can calculate the
                                -- start_date (eligible_date)
        ELSE start_date
    END start_date,
        -- if the most recent subsession is True, then end_date should be NULL
    IF((ROW_NUMBER() OVER (PARTITION BY cd.person_id, cd.state_code
                           ORDER BY start_date DESC) =  1)
            AND (critical_date_has_passed),
        NULL,
        end_date) AS end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(xps.real_eligible_date AS eligible_date)) AS reason,
FROM critical_date_has_passed_spans cd
LEFT JOIN save_real_eligible_date xps
    USING (person_id, state_code, critical_date)
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        analyst_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
