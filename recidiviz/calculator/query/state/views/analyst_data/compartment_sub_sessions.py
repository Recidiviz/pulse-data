# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Sub-sessionized view of each individual. Session defined as continuous stay within a compartment and location"""
# pylint: disable=trailing-whitespace
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import STATIC_REFERENCE_TABLES_DATASET, \
    DATAFLOW_METRICS_MATERIALIZED_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SUB_SESSIONS_VIEW_NAME = 'compartment_sub_sessions'

COMPARTMENT_SUB_SESSIONS_VIEW_DESCRIPTION = \
    """Sub-sessionized view of each individual. Session defined as continuous stay within a compartment and location"""

COMPARTMENT_SUB_SESSIONS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH population_cte AS
    /*
    Union together incarceration and supervision population metrics (both in state and out of state). There are cases in 
    each of these individual dataflow metrics where we have the same person on the same day with different values for 
    supervision types or specialized purpose for incarceration. This deduplication is handled further down in the query. 
    
    Create a field that identifies the compartment_level_1 (incarceration vs supervision) and compartment_level_2, which 
    for incarceration can  be 'GENERAL','PAROLE_BOARD_HOLD' or 'TREATMENT_IN_PRISON', and for supervision can be 
    'PAROLE', 'PROBATION', or 'DUAL'.Records that are not in one of these compartments are left null and populated later
    in the query.
    
    The field "metric_source" is pulled from dataflow metric as to distinguish the population metric data sources. This 
    is done because SUPERVISION can come from either SUPERVISION_POPULATION and SUPERVISION_OUT_OF_STATE_POPULATION.
    
    Compartment location is defined as facility for incarceration and judicial district for supervision periods.
    */
    (
    SELECT 
        DISTINCT
        person_id,
        date_of_stay AS date,
        metric_type AS metric_source,
        created_on,
        state_code,
        age_bucket,
        gender,
        prioritized_race_or_ethnicity,
        'INCARCERATION' as compartment_level_1,
        CASE WHEN state_code = 'US_ID' AND specialized_purpose_for_incarceration IN ('GENERAL','PAROLE_BOARD_HOLD','TREATMENT_IN_PRISON')
          THEN specialized_purpose_for_incarceration 
          ELSE 'GENERAL' END AS compartment_level_2,
        facility AS compartment_location,
        CAST(NULL AS STRING) AS assessment_score_bucket
    FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics`
    WHERE metric_period_months = 0
        AND methodology = 'EVENT'
        AND state_code in ('US_ND','US_ID')
    UNION ALL
    SELECT 
        DISTINCT
        person_id,
        date_of_supervision AS date,
        metric_type AS metric_source,
        created_on,       
        state_code,
        age_bucket,
        gender,
        prioritized_race_or_ethnicity,
        'SUPERVISION' as compartment_level_1,
        CASE WHEN supervision_type in ('PAROLE', 'PROBATION','DUAL') THEN supervision_type END AS compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')),
        assessment_score_bucket
    FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics`
    WHERE metric_period_months = 0
        AND methodology = 'EVENT'
        AND state_code in ('US_ND','US_ID')
    UNION ALL
    SELECT 
        DISTINCT
        person_id,
        date_of_supervision AS date,
        metric_type AS metric_source,
        created_on,       
        state_code,
        age_bucket,
        gender,
        prioritized_race_or_ethnicity,
        'SUPERVISION' as compartment_level_1,
        CASE WHEN supervision_type in ('PAROLE', 'PROBATION','DUAL') THEN supervision_type END AS compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')),
        assessment_score_bucket
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_out_of_state_population_metrics`
    WHERE metric_period_months = 0
        AND methodology = 'EVENT'
        AND state_code in ('US_ND','US_ID')  
    )
    ,
    last_day_of_data_cte AS
    (
    SELECT 
        state_code,
        metric_source,
        MAX(created_on) AS last_day_of_data
    FROM population_cte
    GROUP BY 1,2
    ORDER BY 1,2       
    )   
    ,
    dedup_step_1_cte AS 
    (
    /*
    If a record has a null value in compartment_level_2 and has a row that is otherwise identical with a non-null compartment_2_value,
    then take the non-null version. 
    This is done in a step prior to other deduplication because there are other cases where we want to combine
    a person / day on both parole and probation supervision into dual supervision.
    */
    SELECT DISTINCT
        a.person_id,
        a.date,
        a.state_code,
        a.age_bucket,
        a.gender,
        a.prioritized_race_or_ethnicity,
        a.metric_source,
        a.compartment_level_1,
        COALESCE(a.compartment_level_2, b.compartment_level_2) compartment_level_2,
        a.compartment_location,
        a.assessment_score_bucket
    FROM population_cte a 
        LEFT JOIN  population_cte b 
    ON a.person_id = b.person_id
        AND a.date = b.date
        AND a.metric_source = b.metric_source
        AND b.compartment_level_2 IS NOT NULL
    )
    ,
    dedup_step_2_cte AS 
    /* 
    Creates dual supervision category and also dedups to a single person on a single day within metric_source. 
    This is done by classifying any cases where a person has more than one supervision level_2 on the same day as 
    being "DUAL" and then deduplicating so that there is only one record on that person/day.
    */
    (
    SELECT 
        person_id,
        date,
        state_code,
        age_bucket,
        gender,
        prioritized_race_or_ethnicity,
        metric_source,
        compartment_level_1,
        CASE WHEN cnt > 1 AND compartment_level_1 = 'SUPERVISION' THEN 'DUAL' ELSE compartment_level_2 END AS compartment_level_2,
        compartment_location,
        assessment_score_bucket
    FROM
        (
        SELECT 
            *, 
            COUNT(DISTINCT(compartment_level_2)) OVER(PARTITION BY person_id, date, compartment_level_1, metric_source) AS cnt,
            ROW_NUMBER() OVER(PARTITION BY person_id, date, compartment_level_1, metric_source) AS rn
        FROM dedup_step_1_cte
        )
    WHERE rn = 1
    )
    ,
    dedup_step_3_cte AS 
    /*
    Dedup across metric_source (INCARCERATION_POPULATION, SUPERVISION_POPULATION, SUPERVISION_OUT_OF_STATE_POPULATION),
    prioritizing in that order.
    */
    (
    SELECT
        person_id,
        date,
        state_code,
        age_bucket,
        gender,
        prioritized_race_or_ethnicity,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        compartment_location,
        assessment_score_bucket
    FROM 
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY person_id, date ORDER BY 
                CASE WHEN metric_source = 'INCARCERATION_POPULATION' THEN 1 
                    WHEN metric_source = 'SUPERVISION_POPULATION'  THEN 2 
                    WHEN metric_source = 'SUPERVISION_OUT_OF_STATE_POPULATION' THEN 3 END ASC) AS rn
        FROM  dedup_step_2_cte
        )
    WHERE rn = 1
    )
    ,
    filled_missing_pop_types_cte AS
    /*
    There are cases where a person will have a continuous set of days in supervision where part of the time is classified
    as parole or probation and part of the time is classified as unknown. Here the general assumption is made that if a
    a person has a continuous set of days within the same data source (incarceration or supervision), any unknown
    compartment values are forward-filled and back-filled. Unknown compartments that are 'islands' (not continuous with 
    any other compartment from the same data source, are set to "OTHER"). This is done to avoid artificially creating 
    new sessions when they are really part of the preceding or following session.
    */
    (
    SELECT  
        person_id,
        date,
        state_code,
        metric_source,
        compartment_level_1,
        COALESCE(
            LAST_VALUE(compartment_level_2 IGNORE NULLS) OVER(PARTITION BY person_id, compartment_level_1, group_continuous_dates ORDER BY date ASC), 
            FIRST_VALUE(compartment_level_2 IGNORE NULLS) OVER(PARTITION BY person_id, compartment_level_1, group_continuous_dates ORDER BY date ASC),
            'OTHER') AS compartment_level_2,
        compartment_location
    FROM 
        (
        SELECT
          *,
        /*
        Identify groups of continuous dates that are used for the forward and backward fill above. Within each person_id,
        the difference between the date and a row number ordered by date will be the same for continuous date ranges.
        */
            DATE_SUB(DATE, INTERVAL ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY date ASC) DAY) AS group_continuous_dates
        FROM dedup_step_3_cte
        )
    )
    ,
    sessionized_cte AS 
    /*
    Aggregate across distinct sub-sessions (continuous dates within metric_source, compartment, location, and person_id)
    and get the range of dates that define the session.
    */
    (
    SELECT
        group_continuous_dates_in_compartment,
        person_id,
        state_code,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        compartment_location,
        MIN(date) AS start_date,
        MAX(date) AS end_date
    FROM 
        (
        /*
        Create groups used to identify unique sessions. This is the same technique used above to identify continuous 
        dates, but now restricted to continuous dates within a compartment
        */
        SELECT *,
            DATE_SUB(DATE, INTERVAL ROW_NUMBER() OVER(PARTITION BY person_id, metric_source, compartment_level_1, compartment_level_2, compartment_location
                ORDER BY date ASC) DAY) AS group_continuous_dates_in_compartment
        FROM filled_missing_pop_types_cte
        ORDER BY date ASC
        )
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY MIN(DATE) ASC
    )
    ,
    sessionized_null_end_date_cte AS
    /*
    Same as sessionized cte with null end dates for active sessions.
    */
    (
    SELECT 
        s.person_id,
        s.state_code,
        s.metric_source,
        s.compartment_level_1,
        s.compartment_level_2,
        s.compartment_location,
        s.start_date,
        CASE WHEN s.end_date < l.last_day_of_data THEN s.end_date END AS end_date,
        l.last_day_of_data
    FROM sessionized_cte s
    LEFT JOIN last_day_of_data_cte l
        ON s.state_code = l.state_code
        AND s.metric_source = l.metric_source
    )
    ,
    release_compartment_cte AS
    /*
    Develop assumption around when someone is in a "RELEASE" compartment. Assume that if there is a gap between (1) the 
    end date of one session and the start date of the following or (2) the end date of the last session and the last day
    of data, that the person is released during that time period. This gives full session coverage for each person from 
    the start of their first session to the last day for which we have data
    */
    (
    SELECT 
        person_id,
        state_code,
        'INFERRED' AS metric_source,
        'RELEASE' AS compartment_level_1,
        'RELEASE' AS compartment_level_2,
        CAST(NULL AS STRING) AS compartment_location,
        start_date,
        end_date,
        MIN(last_day_of_data) OVER(PARTITION BY state_code) AS last_day_of_data
    FROM
        (
        SELECT 
            person_id,
            state_code,
            --new session starts the day after the current row's end date
            DATE_ADD(end_date, INTERVAL 1 DAY) as start_date,
            --new session ends the day before the following row's start date
            DATE_SUB(LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date ASC), INTERVAL 1 DAY) AS end_date,
            last_day_of_data
        FROM sessionized_null_end_date_cte
        )
    /*
    This where clause ensures that these new release records are only created when there is a gap in sessions.
    The release record start date will be greater than the release record end date when constructed from continuous 
    sessions, and will therefore be excluded. In cases where is a currently active session, no release record will 
    be created because the release record start date will be null.
    */
    WHERE COALESCE(end_date, '9999-01-01') >= start_date
    )
    ,
    full_sessionized_cte AS
    /*
    Union together the incarceration and supervision sessions with the newly created release sessions.
    */
    (
    SELECT * FROM sessionized_null_end_date_cte
    UNION ALL
    SELECT * FROM release_compartment_cte
    )
    ,
    start_metric_cte AS
     /*
    Combines three sources of admission / start metrics - (1) incarceration admissions, (2) revocation admisisons, and 
    (3) supervision starts. Each of these 3 metrics is deduped individually, limiting 1 admission type on each day. 
    No deduplication is done across the metrics because the join condition requires that the admission type joins to 
    to the relevant compartment (supervision vs incarceration). In theory, there could be dups if a person has a 
    new admission and a revocation admission on the same day, but that hasn't happen in the data, at this point.    
    */
    (
    SELECT 
        person_id,
        admission_date AS start_date,
        state_code,
        admission_reason AS start_reason,
        admission_reason AS start_sub_reason,
        'INCARCERATION' as compartment_level_1,
        --There are no cases of multiple admission reasons occurring on the same date as we are sub-setting for 
        --"NEW_ADMISSION", therefore there is no 'order by' in the window function below
        ROW_NUMBER() OVER(PARTITION BY person_id, admission_date) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_admission_metrics`
    WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND admission_reason = 'NEW_ADMISSION' 
    UNION ALL 
    SELECT 
        person_id,
        revocation_admission_date as start_date,
        state_code,
        'REVOCATION' AS start_reason,
         COALESCE(source_violation_type, 'UNKNOWN_REVOCATION') AS start_sub_reason,
        'INCARCERATION' as compartment_level_1,
        --This is very rare (2 cases) where a person has more that one revocation (with different reasons) on the same day. In both cases one of the 
        --records has a null reason, so here I dedup prioritizing the non-null one.
        ROW_NUMBER() OVER(PARTITION BY person_id, revocation_admission_date ORDER BY IF(source_violation_type IS NULL, 1, 0)) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_revocation_metrics`
    WHERE methodology = 'EVENT'
        AND metric_period_months = 1
    UNION ALL
    SELECT 
        person_id,
        start_date,
        state_code,
        admission_reason AS start_reason,
        admission_reason AS start_sub_reason,
        'SUPERVISION' as compartment_level_1,
         ROW_NUMBER() OVER(PARTITION BY person_id, start_date ORDER BY priority ASC) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_start_metrics` m
    --The main logic here is to de-prioritize transfers when they are concurrent with another reason
    LEFT JOIN `static_reference_tables.admission_start_reason_dedup_priority` d
      ON d.data_source = 'SUPERVISION'
      AND d.start_reason = m.admission_reason
    WHERE methodology = 'EVENT'
        AND metric_period_months = 0
    )
    ,
    release_metric_cte AS
     /*
    Pull in release reasons to join to the sessions view.
    TODO(#142): Add validation to ensure all release reasons are in the static ranking table.
    */
    (
    SELECT 
        person_id,
        state_code,
        release_date AS end_date,
        release_reason AS end_reason,
        'INCARCERATION' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, release_date ORDER BY COALESCE(priority, 999)) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics` AS m
    LEFT JOIN `{project_id}.{static_reference_views_dataset}.release_termination_reason_dedup_priority` AS p
        ON m.release_reason = p.end_reason
        AND p.data_source = 'INCARCERATION'
    WHERE metric_period_months = 1
        AND methodology = 'EVENT'
        AND end_reason IS NOT NULL
    UNION ALL  
    SELECT 
        person_id,
        state_code,
        termination_date AS end_date,
        termination_reason AS end_reason,
        'SUPERVISION' AS data_source,
        ROW_NUMBER() OVER(PARTITION BY person_id, termination_date ORDER BY COALESCE(priority, 999)) AS rn
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_termination_metrics` m
    LEFT JOIN `{project_id}.{static_reference_views_dataset}.release_termination_reason_dedup_priority` AS p
        ON m.termination_reason = p.end_reason
        AND p.data_source = 'SUPERVISION'
    WHERE metric_period_months = 1
        AND methodology = 'EVENT'
        AND end_reason IS NOT NULL
    )
    ,
    final_sessions_prep_cte AS
    (
    /*
    Final view that includes a sub_session_id, which is created based on the order of an individual's sessions. This view
    is also joined back to the start metric table to pull in the start reason and start sub reason associated with 
    the start of the session. 
    */
    SELECT 
        sessions.person_id,
        ROW_NUMBER() OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS sub_session_id,
        sessions.state_code,
        sessions.metric_source,
        sessions.compartment_level_1,
        sessions.compartment_level_2,
        sessions.compartment_location,
        starts.start_reason,
        starts.start_sub_reason,
        releases.end_reason,
        sessions.start_date,
        sessions.end_date,
        releases.end_date AS release_date,
        start_of_session.gender,
        start_of_session.age_bucket,
        start_of_session.prioritized_race_or_ethnicity,
        start_of_session.assessment_score_bucket,
        LAG(sessions.compartment_level_1) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS inflow_from_level_1,
        LAG(sessions.compartment_level_2) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS inflow_from_level_2,
        LEAD(sessions.compartment_level_1) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS outflow_to_level_1,
        LEAD(sessions.compartment_level_2) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS outflow_to_level_2,
        DATE_DIFF(COALESCE(sessions.end_date, sessions.last_day_of_data), sessions.start_date, DAY) AS session_length_days,
        sessions.last_day_of_data
    FROM full_sessionized_cte AS sessions
    LEFT JOIN dedup_step_3_cte AS start_of_session
        ON sessions.person_id = start_of_session.person_id
        AND sessions.start_date = start_of_session.date
    LEFT JOIN start_metric_cte starts
        ON starts.person_id = sessions.person_id
        AND starts.start_date = sessions.start_date
        AND starts.compartment_level_1 = sessions.compartment_level_1
        AND starts.rn = 1
    LEFT JOIN release_metric_cte releases
        -- The release date will be a day after the session end date as the population metrics count a person towards 
        -- population based on full days within that compartment
        ON releases.end_date = DATE_ADD(sessions.end_date, INTERVAL 1 DAY)
        AND releases.person_id = sessions.person_id
        AND releases.data_source = sessions.compartment_level_1
        AND releases.rn = 1
    )
    /*
    This takes the previous CTE and adds a field for session_id which is calculated based on moving to a new compartment
    (based the concatenation of compartment_level_1 and compartment_level2)
    */
    SELECT
        person_id,
        sub_session_id,
        SUM(CASE WHEN CONCAT(compartment_level_1, compartment_level_2)!=COALESCE(CONCAT(inflow_from_level_1, inflow_from_level_2),'') THEN 1 ELSE 0 END) 
            OVER(PARTITION BY person_id ORDER BY sub_session_id) AS session_id,
        state_code,
        start_date,
        end_date,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        compartment_location,
        start_reason,
        start_sub_reason,
        end_reason,
        release_date,
        gender,
        age_bucket,
        prioritized_race_or_ethnicity,
        assessment_score_bucket,
        inflow_from_level_1,
        inflow_from_level_2,
        outflow_to_level_1,
        outflow_to_level_2,
        session_length_days,
        last_day_of_data
    FROM final_sessions_prep_cte
    ORDER BY person_id ASC, sub_session_id ASC
    """

COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SUB_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SUB_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SUB_SESSIONS_VIEW_DESCRIPTION,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_views_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.build_and_print()
