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

"""
Defines a criteria view that shows spans of time for
which clients have served 1/2 of their sentence if their term of imprisonment is
less or equal to 5 years or 2/3 if their term of imprisonment is more than 5 years.
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
from recidiviz.task_eligibility.utils.us_me_query_fragments import cis_319_after_csswa
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_SERVED_X_PORTION_OF_SENTENCE"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which clients have served 1/2 of their sentence if their term of imprisonment is
less or equal to 5 years or 2/3 if their term of imprisonment is more than 5 years.
"""

_QUERY_TEMPLATE = f"""
WITH term_wdur_cte AS (
-- Calculate duration of term
    SELECT
        *,
        -- Difference between the very first intake date (from county jail) and
        --  the maximum expected release date.
        DATE_DIFF(end_date_max,
                  start_date_from_county_jail, 
                  DAY) AS term_duration_days_original_sentence,
        DATE_DIFF(end_date,
                  start_date, 
                  DAY) AS term_duration_days,
    FROM (
            SELECT
            * EXCEPT (start_date_from_county_jail),
            -- if we don't have intake_date, we assume the end_date of previous term
            COALESCE(
                start_date_from_county_jail,
                LAG(end_date) OVER (PARTITION BY person_id ORDER BY end_date)
                ) AS start_date_from_county_jail,
            FROM `{{project_id}}.{{analyst_dataset}}.us_me_sentence_term_materialized`)
    ),
term_crit_date AS (
-- Calculate critical date
    SELECT 
        * EXCEPT (critical_date),
        critical_date AS real_eligible_date,
        -- folks can start their paperwork 3 months before their eligibility date
        DATE_SUB(critical_date, INTERVAL 3 MONTH) AS critical_date,
    FROM (
        SELECT
            * EXCEPT(term_duration_days, term_duration_days_original_sentence),
            CASE
                WHEN term_duration_days_original_sentence/365 > 5
                    THEN DATE_ADD(
                        start_date,
                        INTERVAL SAFE_CAST(TRUNC(term_duration_days*2/3, 0) AS INT64) DAY)
                ELSE DATE_ADD(
                    start_date,
                    INTERVAL SAFE_CAST(TRUNC(term_duration_days*1/2, 0) AS INT64) DAY)
            END critical_date,
            IF(term_duration_days_original_sentence/365 > 5, '2/3', '1/2') AS x_portion_served,
        FROM term_wdur_cte
    )
),

{create_sub_sessions_with_attributes('term_crit_date AS')},
critical_date_spans AS (
    {cis_319_after_csswa()}
),

{critical_date_has_passed_spans_cte(attributes = ['x_portion_served', 'real_eligible_date'])}

SELECT
    cd.state_code,
    cd.person_id,
    cd.start_date,
    -- if the most recent subsession is True, then end_date should be NULL instead of
    -- term end_date
    IF((ROW_NUMBER() OVER (PARTITION BY cd.person_id, cd.state_code
                           ORDER BY cd.start_date DESC) =  1)
            AND (cd.critical_date_has_passed),
        NULL,
        cd.end_date) AS end_date,                       
    cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(cd.real_eligible_date AS eligible_date,
                   cd.x_portion_served AS x_portion_served)) AS reason,
FROM critical_date_has_passed_spans cd
WHERE {nonnull_start_date_clause('cd.start_date')} != {nonnull_end_date_clause('cd.end_date')}
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
