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
"""

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
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
from recidiviz.task_eligibility.utils.raw_table_import import cis_319_term_cte
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_X_MONTHS_REMAINING_ON_SENTENCE"

_DESCRIPTION = """Defines a criteria view that shows spans of time for
which clients are 30/24 months away from expected release date. When 
average state-wide case load is lower than 90, residents are allowed
to be released within their last 30 months ; otherwise, residents are only 
allowed to be released 24 months before their sentence ends.
"""

_QUERY_TEMPLATE = f"""
WITH case_load AS (
  -- Current ratio of supervision clients/supervision officers
  SELECT 
    supervision_clients/supervision_officers AS curr_supervision_client_ratio
  FROM (
        SELECT
            COUNT(DISTINCT(supervising_officer_external_id)) AS supervision_officers,
            COUNT(DISTINCT person_id) AS supervision_clients
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_officer_sessions_materialized`
        WHERE state_code = 'US_ME'
            AND CURRENT_DATE('US/Pacific')  <= {nonnull_end_date_clause('end_date')} 
            -- TODO(#16408) we need to confirm who to subset here
            AND supervising_officer_external_id != '0' -- Seems to be old/migration data
            -- TODO(#16823) should drop these 0s in normalization maybe?
        )
),
{cis_319_term_cte()},
term_cte_crit_date AS (
    -- Calculate the critical date as a function of the statewide case load
    SELECT 
        * EXCEPT(curr_supervision_client_ratio),
        CASE
            WHEN curr_supervision_client_ratio > 90 THEN DATE_SUB(end_date, INTERVAL 24 MONTH)
            ELSE DATE_SUB(end_date, INTERVAL 30 MONTH) 
        END critical_date,
    FROM term_cte, case_load
),
{create_sub_sessions_with_attributes('term_cte_crit_date')},
critical_date_spans AS (
    -- only keep the latest critical date in case of a repeated subsession
  SELECT 
        state_code,
        person_id, 
        start_date AS start_datetime,
        end_date AS end_datetime,
        critical_date
  FROM sub_sessions_with_attributes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, state_code, start_date, end_date
                    ORDER BY critical_date DESC) = 1
),
{critical_date_has_passed_spans_cte()}
SELECT
    state_code,
    person_id,
    CASE                                            
        WHEN (start_date IS NULL) AND (critical_date_has_passed) THEN critical_date  
                                -- When there was no intake date in CIS_319_TERM,
                                -- start_date of our subsession is NULL for the 
                                -- period for which the criteria is met. But
                                -- we know the end date and we can calculate the 
                                -- start_date (eligible_date) 
        ELSE start_date                             
    END start_date,   
        -- if the most recent subsession is True, then end_date should be NULL                              
    IF((ROW_NUMBER() OVER (PARTITION BY person_id, state_code
                           ORDER BY start_date DESC) =  1) 
            AND (critical_date_has_passed), 
        NULL, 
        end_date) AS end_date,                    
    critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(critical_date AS eligible_date)) AS reason,
FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
