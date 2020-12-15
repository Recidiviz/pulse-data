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
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET, STATIC_REFERENCE_TABLES_DATASET
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
        facility AS compartment_location
    FROM
        `{project_id}.{metrics_dataset}.incarceration_population_metrics`
    INNER JOIN
        `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
    USING (state_code, year, month, metric_period_months, metric_type, job_id)
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
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN'))
    FROM
        `{project_id}.{metrics_dataset}.supervision_population_metrics`
    INNER JOIN
        `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
    USING (state_code, year, month, metric_period_months, metric_type, job_id)
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
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN'))
    FROM `{project_id}.{metrics_dataset}.supervision_out_of_state_population_metrics` 
    INNER JOIN
        `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
    USING (state_code, year, month, metric_period_months, metric_type, job_id)
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
        a.compartment_location
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
        compartment_location
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
        compartment_location
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
    admissions_metric_cte AS
    /*
    Pull in admissions and revocation admissions reasons to join to the sessions view. The unioning together of
    admissions and revocations is the same logic used in dashboard views. 
    */
    (
    SELECT 
        DISTINCT 
        state_code,
        person_id,
        admission_date,
        'NEW_ADMISSION' AS admission_type,
         CAST(NULL AS STRING) AS revocation_violation_type,
    FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`
    INNER JOIN
        `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
        USING (state_code, year, month, metric_period_months, metric_type, job_id)
    WHERE methodology = 'EVENT'
        AND metric_period_months = 1
        AND admission_reason = 'NEW_ADMISSION'
    UNION ALL 
    SELECT 
        DISTINCT
        state_code,
        person_id,
        revocation_admission_date as admission_date,
        'REVOCATION' AS admission_type,
        CASE WHEN source_violation_type IN ('ABSCONDED', 'ESCAPED', 'FELONY', 'MISDEMEANOR', 'LAW') then 'NON_TECHNICAL'
            WHEN source_violation_type is NULL THEN 'UNKNOWN_REVOCATION'
            ELSE source_violation_type END as revocation_violation_type
    FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics` 
    INNER JOIN
        `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
    USING (state_code, year, month, metric_period_months, metric_type, job_id)
    WHERE methodology = 'EVENT'
        AND metric_period_months = 1
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
    FROM `{project_id}.{metrics_dataset}.incarceration_release_metrics` AS m
    INNER JOIN
        `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized` 
    USING (state_code, year, month, metric_period_months, metric_type, job_id)
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
    FROM `{project_id}.{metrics_dataset}.supervision_termination_metrics` m
    INNER JOIN
        `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
    USING (state_code, year, month, metric_period_months, metric_type, job_id)
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
    is also joined back to the admissions_metric_cte table to pull in the admission reason and supervision type associated with 
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
        admissions.admission_type,
        admissions.revocation_violation_type,
        releases.end_reason,
        sessions.start_date,
        sessions.end_date,
        releases.end_date AS release_date,
        start_of_session.gender,
        start_of_session.age_bucket,
        start_of_session.prioritized_race_or_ethnicity,
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
    LEFT JOIN admissions_metric_cte admissions
        ON admissions.person_id = sessions.person_id
        AND admissions.admission_date = sessions.start_date
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
        admission_type,
        revocation_violation_type,
        end_reason,
        release_date,
        gender,
        age_bucket,
        prioritized_race_or_ethnicity,
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
    metrics_dataset=DATAFLOW_METRICS_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_views_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.build_and_print()
