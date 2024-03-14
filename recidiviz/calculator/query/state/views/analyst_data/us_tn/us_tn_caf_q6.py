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
# =============================================================================
"""Computes Q6 of TN's CAF form (history of disciplinary reports) at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CAF_Q6_VIEW_NAME = "us_tn_caf_q6"

US_TN_CAF_Q6_VIEW_DESCRIPTION = """Computes Q6 of TN's CAF form (history of disciplinary reports) at any point in time
    See details of CAF Form here https://drive.google.com/file/d/18ez172yx3Tpx-rFaNseJv3dgMh7cGezg/view?usp=sharing
    """

US_TN_CAF_Q6_QUERY_TEMPLATE = f"""
    /* For this question, there are two types of critical dates:
    1) All disciplinaries that recieved a Guilty (not Guilty-Verbal) disposition and are 
    not missing disciplinary class
    2) Incarceration session starts to a TDOC facility (since a "new admission/parole violator" scores a 0)
    
    The first CTE identifies the relevant starts, the next CTE unions together these 2 types of critical dates
    
    We've confirmed with a TN Counselor that if someone returns to prison, even if they had a disciplinary
    in the past 6 months, they receive a score of 0. There is an exception to this if the disciplinary is
    serious or an escape, in which case the logic should consider the last 18 months of someone's incarceration. We're
    not currently implementing this exception but will in TODO(#28196)
    */
    WITH incarceration_state_prison_sub_sessions AS (
      SELECT cs.*
        FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` cs
      INNER JOIN `{{project_id}}.reference_views.location_metadata_materialized`
        ON facility = location_external_id
      WHERE cs.state_code = 'US_TN'
        AND compartment_level_1 = 'INCARCERATION'  
        AND compartment_level_2 = 'GENERAL'
        AND location_type = 'STATE_PRISON'
    ),
    incarceration_state_prison_sub_sessions_adjacent_spans AS (
        SELECT *
        -- After identifying starts to TDOC facilities, we aggregate adjacent spans so critical dates
        -- accurately represent TDOC facility starts, not other changes that can lead to a new span in sub-sessions
        FROM ({aggregate_adjacent_spans(table_name='incarceration_state_prison_sub_sessions',
                                    end_date_field_name="end_date_exclusive")})
    ),
    critical_dates AS (
      SELECT
        DISTINCT
        state_code,
        person_id,
        incident_date AS critical_date,
        incident_date,
        CAST(NULL AS DATE) AS session_start_date,
        1 AS disciplinary,
      FROM 
            `{{project_id}}.{{analyst_dataset}}.incarceration_incidents_preprocessed_materialized` dis
        WHERE
            state_code = "US_TN" 
            AND disposition = 'GU' 
            AND incident_details NOT LIKE "%VERBAL WARNING%"
            AND incident_class IS NOT NULL
      
      UNION DISTINCT 
         
        SELECT
            state_code,
            person_id,
            start_date AS critical_date,
            NULL AS incident_date,
            start_date AS session_start_date,
            0 AS disciplinary,
        FROM incarceration_state_prison_sub_sessions_adjacent_spans
    ),
    /* Each critical date can be relevant 6, 12, and 18 months after the date, since those are the boundaries when
    someone's score can change. This CTE joins an array of those months as well as computing the next critical date.
    The logic for choosing span_end is as follows:
    - We want to keep the minimum of next_critical_date and the 6, 12, 18 month interval after the critical date
    - However we want to also make sure each critical date is at least relevant for 6 months. This is particularly
    important if there are 2 disciplinaries within 6 months. Making span_end at least 6 months after a disciplinary
    means we can create overlapping sessions when both disciplinaries need to be 'counted' since Two or More Disciplinaries
    in 6 Months score a 4 
    */
    determine_span_ends AS (
      SELECT
        DISTINCT
        state_code,
        person_id,
        critical_date,
        incident_date,
        GREATEST(
            LEAST({nonnull_end_date_clause('next_critical_date')}, DATE_ADD(critical_date, INTERVAL month_lag MONTH)),
            DATE_ADD(critical_date, INTERVAL 6 MONTH)
        ) AS span_end,
        disciplinary,
        session_start_date,
      FROM (
        SELECT *,
          LEAD(critical_date) OVER (PARTITION BY person_id ORDER BY critical_date ASC) AS next_critical_date,
        FROM critical_dates
      ),
      UNNEST([6,12,18]) AS month_lag
    ),
    spans_cte AS (
        SELECT
          state_code,
          person_id,
          critical_date AS start_date,
          incident_date,
          span_end AS end_date,
          session_start_date,
          -- Disciplinaries should only count against someone for 6 months. After that, we still need the 12 and 18
          -- month boundaries but we don't want those spans to have disciplinary = 1 
          CASE WHEN DATE_DIFF(
                        span_end,
                        critical_date,
                        MONTH
                    ) > 6 
                    AND disciplinary = 1
                    THEN 0 
                    ELSE disciplinary
                    END AS disciplinary,
        FROM
            determine_span_ends
        
        UNION DISTINCT

        -- Join on all sessions to bring in session end dates for incarceration sessions
        SELECT
            state_code,
            person_id,
            start_date,
            NULL AS incident_date,
            end_date_exclusive AS end_date,
            start_date AS session_start_date,
            0 AS disciplinary,
          FROM incarceration_state_prison_sub_sessions_adjacent_spans            
    ),
    {create_sub_sessions_with_attributes('spans_cte')},
    dedup_cte AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date AS end_date_exclusive,
            -- Count if there is any disciplinary in that sub-session
            LOGICAL_OR(disciplinary = 1) AS disciplinary,
            -- Keep the latest incident_date
            MAX(incident_date) AS latest_incident_date,
            -- Sum disciplinaries where there might be overlapping sub-sessions
            SUM(disciplinary) AS sum_disciplinaries,
            -- Keep the last session start date
            MAX(session_start_date) AS session_start_date,
        FROM
            sub_sessions_with_attributes
        GROUP BY
            1,2,3,4
    )
    , scoring AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            /* The logic here is as follows:
            - When there are 2 or more disciplinaries, score is 4
            - When there is 1 disciplinary, score is 1 (because we've already accounted for this only "counting" for 6 months
            - When there are no discplinaries in a subsession, a few things can be true:
                - If the session start date falls during the span, the person gets a 0 as a 'new admission / parole violator'
                - If the span is 6 months long or less, we could have
                    - A person who is between 6-12 months since the last relevant disciplinary, in which case they score -1
                    - A person who is 12-18 months since their last relevant disciplinary, in which case they score -2
                    - Otherwise they score -4. A similar logic applies for people whose span is 12 months long or less
            */ 
            CASE WHEN sum_disciplinaries >= 2 THEN 4
                 WHEN sum_disciplinaries = 1 THEN 1
                 WHEN sum_disciplinaries = 0 THEN (
                    CASE 
                        WHEN session_start_date BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')} THEN 0
                        WHEN DATE_DIFF(COALESCE(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY),CURRENT_DATE('US/Eastern')), start_date, MONTH) <= 6 THEN (
                            CASE WHEN months_since_latest_disc <= 6 THEN -1
                                 WHEN months_since_latest_disc <= 12 THEN -2
                                 ELSE -4
                                 END
                        )
                        WHEN DATE_DIFF(COALESCE(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY),CURRENT_DATE('US/Eastern')), start_date, MONTH) <= 12 THEN (
                            CASE WHEN months_since_latest_disc <= 12 THEN -2
                                 ELSE -4
                                 END
                        )
                        ELSE -4
                        END
                 )
                 END AS q6_score
        FROM (
            SELECT *,
                -- For each sub-session, calculates time since most recent disciplinary. When there are no disciplinaries,
                -- we take latest session start date
                DATE_DIFF(start_date, COALESCE(latest_incident_date,session_start_date), MONTH) AS months_since_latest_disc
            FROM dedup_cte
        )
    )
    SELECT *
    FROM ({aggregate_adjacent_spans(table_name='scoring',
                                    attribute=['q6_score'],
                                    end_date_field_name="end_date_exclusive")})
    
"""

US_TN_CAF_Q6_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    view_id=US_TN_CAF_Q6_VIEW_NAME,
    description=US_TN_CAF_Q6_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    view_query_template=US_TN_CAF_Q6_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CAF_Q6_VIEW_BUILDER.build_and_print()
