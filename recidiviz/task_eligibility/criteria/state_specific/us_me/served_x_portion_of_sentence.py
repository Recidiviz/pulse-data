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
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.raw_table_import import (
    cis_319_after_csswa,
    cis_319_term_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_SERVED_X_PORTION_OF_SENTENCE"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which clients have served 1/2 of their sentence if their term of imprisonment is
less or equal to 5 years or 2/3 if their term of imprisonment is more than 5 years.
"""

_QUERY_TEMPLATE = f"""
WITH {cis_319_term_cte()},

term_wdur_cte AS (
-- Calculate duration of term
    SELECT 
        *, 
        DATE_DIFF(end_date, start_date, DAY) AS term_duration_days,
    FROM (SELECT         
            * EXCEPT (start_date),
            -- if we don't have intake_date, we assume the end_date of previous term
            COALESCE(
                start_date,
                LAG(end_date) OVER (PARTITION BY person_id ORDER BY end_date)
                ) AS start_date,
            FROM term_cte)
    ),
term_crit_date AS (
-- Calculate critical date
    SELECT
        * EXCEPT(term_duration_days),
        CASE 
            WHEN term_duration_days/365 > 5 
                THEN DATE_ADD(
                    start_date, 
                    INTERVAL SAFE_CAST(ROUND(term_duration_days*2/3) AS INT64) DAY)
            ELSE DATE_ADD(
                start_date, 
                INTERVAL SAFE_CAST(ROUND(term_duration_days*1/2) AS INT64) DAY)        
        END critical_date,
        IF(term_duration_days/365 > 5, '2/3', '1/2') AS x_portion_served,
    FROM term_wdur_cte
),
{create_sub_sessions_with_attributes('term_crit_date')},
critical_date_spans AS (
    {cis_319_after_csswa()}
),
save_x_portion_served AS (
    SELECT
        DISTINCT person_id, state_code, critical_date, x_portion_served
    FROM critical_date_spans
),
{critical_date_has_passed_spans_cte()}

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
    TO_JSON(STRUCT(cd.critical_date AS eligible_date,
                   xps.x_portion_served AS x_portion_served)) AS reason,
FROM critical_date_has_passed_spans cd
LEFT JOIN save_x_portion_served xps
    ON  xps.person_id = cd.person_id
        AND xps.state_code = cd.state_code
        ANd xps.critical_date = cd.critical_date
WHERE {nonnull_start_date_clause('cd.start_date')} != {nonnull_end_date_clause('cd.end_date')}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
