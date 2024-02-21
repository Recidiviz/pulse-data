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

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    completion_event_state_specific_dataset,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_PAST_INCARCERATION_ANNUAL_CLASSIFICATION_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their reclassification date"""

_QUERY_TEMPLATE = f"""
  /* The following CTE unions together different date boundaries: year-span starts, year-span ends, reclassification meeting dates.
  Meeting dates contribute -1 and year-span starts contribute 1 in order to count how many reclassification meetings
  someone has due at a given time. The original session is spanified into yearly sections in order to keep track
   of reclassification meeting debt. */
    WITH super_sessions_with_6_years_remaining AS (
        -- This CTE is used to merge the super_sessions with the date at which they will 
        -- have 6 years remaining or less. That way it only includes spans of time where 
        -- they require an annual reclassification
          SELECT 
            iss.state_code,
            iss.person_id,
            iss.incarceration_super_session_id, 
            iss.start_date,
            LEAST( MAX({nonnull_end_date_clause('iss.end_date')}), 
                   MAX({nonnull_end_date_clause('syr.eligible_date')})
             ) AS end_date, -- If the eligible_date is before the end_date, we use the eligible_date
          FROM `{{project_id}}.{{sessions_dataset}}.incarceration_super_sessions_materialized` iss
          LEFT JOIN (
              -- Grab date at which folks will have 6 years remaining or less
              SELECT 
                state_code,
                person_id,
                SAFE_CAST(JSON_EXTRACT_SCALAR(reason,'$.eligible_date') AS DATE) AS eligible_date,
                start_date,
                end_date,
              FROM `{{project_id}}.{{task_eligibility_criteria_us_me}}.six_years_remaining_on_sentence_materialized`
              WHERE meets_criteria
          ) syr
          ON iss.person_id = syr.person_id
            AND iss.state_code = syr.state_code
            -- Merge any time we have overlapping spans
            AND {nonnull_start_date_clause('syr.start_date')} < {nonnull_end_date_clause('iss.end_date')}
            AND {nonnull_start_date_clause('iss.start_date')} < {nonnull_end_date_clause('syr.end_date')}
          WHERE iss.state_code = 'US_ME'
            -- If start_date is not before eligible date, they need a 6mo reclass from the very begining
            AND {nonnull_end_date_clause('syr.eligible_date')} > iss.start_date
          GROUP BY 1,2,3,4
      ),
      meetings AS (
          SELECT
          person_id, 
          state_code,
          completion_event_date as reclass_meeting_date,
          FROM
            `{{project_id}}.{{completion_event_us_me_dataset}}.incarceration_assessment_completed_materialized`
      ), 
      reclass_is_due AS (
          -- Residents start owing 1 reclass after they've been one year in
          SELECT
            state_code,
            person_id,
            LEAST(DATE_ADD(start_date, INTERVAL OFFSET YEAR), end_date) AS change_date,
            1 AS reclass_type,
          FROM
            super_sessions_with_6_years_remaining,
            UNNEST(GENERATE_ARRAY(1, 100, 1)) AS OFFSET
          WHERE
            OFFSET <= DATE_DIFF(end_date, start_date, DAY) / 365
      ),
      population_change_dates AS (
          -- Everyone is assumed to start with 0 reclasses owed
          SELECT 
            state_code,
            person_id,
            start_date AS change_date,
            0 AS reclass_type,
          FROM
            super_sessions_with_6_years_remaining
              
          UNION ALL 

          SELECT *
          FROM reclass_is_due
            
          UNION ALL
          
          -- This is used to capture the end_date of the super_session so we know when to end a span in the final CTE
          SELECT
            state_code,
            person_id,
            end_date AS change_date,
            0 AS reclass_type,
          FROM
            super_sessions_with_6_years_remaining
            
          UNION ALL
          
          SELECT
            state_code,
            person_id,
            reclass_meeting_date AS change_date,
            -1 AS reclass_type,
          FROM
            meetings
      ),
      population_change_dates_agg AS (
          SELECT
            state_code,
            person_id,
            change_date,
            SUM(reclass_type) AS reclass_type,
          FROM
            population_change_dates
          GROUP BY
            1,
            2,
            3 
      ),
      first_sum AS (
      -- We do a first sum to get the reclassifications needed at each point in time
          SELECT
            p.state_code,
            p.person_id,
            cs.incarceration_super_session_id,
            p.change_date AS start_date,
            SUM(p.reclass_type) OVER (PARTITION BY p.state_code, p.person_id, cs.incarceration_super_session_id ORDER BY p.change_date ) AS reclasses_needed,
            reclass_type,
          FROM
            population_change_dates_agg p
          INNER JOIN
            super_sessions_with_6_years_remaining cs
          ON
            p.person_id = cs.person_id
            AND p.change_date BETWEEN cs.start_date
                AND cs.end_date
      ),
      second_sum AS (
      -- Removes reclassifications done 60 days before one was due and performs
      -- the sum again
          SELECT 
            * EXCEPT(reclasses_needed, next_due_date), 
            SUM(reclass_type) OVER (PARTITION BY state_code, person_id, incarceration_super_session_id ORDER BY start_date ) AS reclasses_needed,
          FROM (
              SELECT 
                fs.*,
                MIN(r.change_date) AS next_due_date,
              FROM first_sum fs
              LEFT JOIN reclass_is_due r
                -- Merge to the first meeting that happens just after the start_date/change_date
                ON fs.person_id = r.person_id
                  AND fs.start_date < r.change_date
              GROUP BY 1,2,3,4,5,6
          )
          WHERE 
            -- If a reclass brings reclasses_needed below 0 and 
            --  it was done more than 60 days before such reclass was due, 
            --  we remove it.
            NOT (reclass_type <= -1 
              AND reclasses_needed < 0 
              AND DATE_DIFF(next_due_date, start_date, DAY) > 60)
      ),
      third_sum AS (
      -- Removes reclassifications that bring the reclasses_needed below -1 and
      -- performs the sum again
          SELECT 
            state_code,
            person_id,
            incarceration_super_session_id,
            start_date,
            -- We use the LEAD function to get the end_date of the next row
            LEAD(start_date) OVER (PARTITION BY state_code, person_id ORDER BY start_date) AS end_date,
            SUM(reclass_type) OVER (PARTITION BY state_code, person_id, incarceration_super_session_id ORDER BY start_date ) AS reclasses_needed,
          FROM second_sum
          -- We remove any reclassifications done if reclasses_needed is below -1
          WHERE NOT (reclasses_needed < -1 AND reclass_type <= -1)
          -- This statement ensures we remove the final span of each `incarceration_super_session_id`, which
          -- has a start_date set to the end_date of the super_session.
          QUALIFY(LEAD(incarceration_super_session_id) 
                  OVER(PARTITION BY state_code, person_id 
                        ORDER BY start_date) = incarceration_super_session_id) 
      )

    SELECT
      ts.state_code,
      ts.person_id,
      ts.start_date,
      {revert_nonnull_end_date_clause('ts.end_date')} as end_date,
      ts.reclasses_needed > 0 as meets_criteria,
      TO_JSON(STRUCT(ANY_VALUE(ts.reclasses_needed) AS reclasses_needed,
        MAX(meetings.reclass_meeting_date) AS latest_classification_date)) as reason,
    FROM
      third_sum ts
    LEFT JOIN meetings
      ON meetings.reclass_meeting_date BETWEEN ts.start_date AND ts.end_date
      AND meetings.person_id = ts.person_id
    GROUP BY 1,2,3,4,5
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=True,
    completion_event_us_me_dataset=completion_event_state_specific_dataset(
        StateCode.US_ME
    ),
    task_eligibility_criteria_us_me=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ME
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
