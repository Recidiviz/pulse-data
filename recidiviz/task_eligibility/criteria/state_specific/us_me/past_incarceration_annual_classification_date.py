# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
# ============================================================================
"""Defines a criteria span view that shows spans of time during which
someone is past their reclassification date"""

from recidiviz.common.constants.states import StateCode
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.dataset_config import (
    completion_event_state_specific_dataset,
)
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_PAST_INCARCERATION_ANNUAL_CLASSIFICATION_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their reclassification date"""

_QUERY_TEMPLATE = f"""
  /* The following CTE unions together different date boundaries: year-span starts, year-span ends, reclassification meeting dates.
  Meeting dates contribute -1 and year-span starts contribute 1 in order to count how many reclassification meetings
  someone has due at a given time. The original session is spanified into yearly sections in order to keep track
   of reclassification meeting debt. */
    WITH
      meetings AS (
      SELECT
       person_id, 
       state_code,
       completion_event_date as reclass_meeting_date,
          FROM
            `{{project_id}}.{{completion_dataset}}.incarceration_assessment_completed_materialized`
      ), 
      population_change_dates AS (
          -- Everyone is assumed to start with 0 reclasses owed
          SELECT 
            state_code,
            person_id,
            start_date AS change_date,
            0 AS reclasses_owed,
          FROM
            `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized`
          WHERE
            state_code = 'US_ME'
              
          UNION ALL 

          -- Residents start owing 1 reclass after they've been one year in
          SELECT
            state_code,
            person_id,
            LEAST(DATE_ADD(start_date, INTERVAL OFFSET YEAR),
                {nonnull_end_date_exclusive_clause('end_date_exclusive')}) AS change_date,
            1 AS reclasses_owed,
          FROM
            `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized`,
            UNNEST(GENERATE_ARRAY(1, 100, 1)) AS OFFSET
          WHERE
            OFFSET <= DATE_DIFF({nonnull_end_date_exclusive_clause('end_date_exclusive')}, start_date, DAY) / 365
            AND state_code = 'US_ME' 
            
          UNION ALL
          
          -- This is used to capture the end_date of the super_session so we know when to end a span in the final CTE
          SELECT
            state_code,
            person_id,
            IFNULL(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), "9999-12-31") AS change_date,
            0 AS reclasses_owed,
          FROM
            `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized`
          WHERE
            state_code = 'US_ME'
            
          UNION ALL
          
          SELECT
            state_code,
            person_id,
            reclass_meeting_date AS change_date,
            -1 AS reclasses_owed,
          FROM
            meetings
            ),
      population_change_dates_agg AS (
          SELECT
            state_code,
            person_id,
            change_date,
            SUM(reclasses_owed) AS reclasses_owed,
          FROM
            population_change_dates
          GROUP BY
            1,
            2,
            3 ),
      time_agg_join AS (
          SELECT
            p.state_code,
            p.person_id,
            p.change_date AS start_date,
            LEAD(p.change_date) OVER (PARTITION BY p.state_code, p.person_id ORDER BY change_date) AS end_date,
            SUM(reclasses_owed) OVER (PARTITION BY p.state_code, p.person_id, cs.incarceration_super_session_id ORDER BY change_date ) AS reclassifications_needed,
          FROM
            population_change_dates_agg p
          LEFT JOIN
            `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` cs
          ON
            p.person_id = cs.person_id
            AND p.change_date BETWEEN cs.start_date
                AND {nonnull_end_date_exclusive_clause('cs.end_date_exclusive')}
            -- This statement ensures we remove the final span of each `incarceration_super_session_id`, which
            -- has a start_date set to the end_date of the super_session.
            QUALIFY( LEAD(incarceration_super_session_id) OVER (PARTITION BY p.state_code, p.person_id ORDER BY change_date) = incarceration_super_session_id) 
        )
    SELECT
      time_agg_join.state_code,
      time_agg_join.person_id,
      time_agg_join.start_date,
      {revert_nonnull_end_date_clause('time_agg_join.end_date')} as end_date,
      time_agg_join.reclassifications_needed > 0 as meets_criteria,
      TO_JSON(STRUCT(ANY_VALUE(time_agg_join.reclassifications_needed) AS reclassifications_needed,
        MAX(meetings.reclass_meeting_date) AS latest_classification_date)) as reason,
    FROM
      time_agg_join
      LEFT JOIN meetings
      ON meetings.reclass_meeting_date BETWEEN start_date AND end_date
      AND meetings.person_id = time_agg_join.person_id
    GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_ME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        completion_dataset=completion_event_state_specific_dataset(StateCode.US_ME),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
