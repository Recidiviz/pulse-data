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
"""Sessionized view of each continuous period of dates with the same population attributes inherited from dataflow metrics"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DATAFLOW_SESSIONS_VIEW_NAME = "dataflow_sessions"

DATAFLOW_SESSIONS_SUPPORTED_STATES = ("US_ND", "US_ID", "US_MO", "US_PA")

DATAFLOW_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual. Session defined as continuous stay with same dataflow population attributes"""

DATAFLOW_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH population_cte AS
    /*
    Union together incarceration and supervision population metrics (both in state and out of state). There are cases in 
    each of these individual dataflow metrics where we have the same person on the same day with different values for 
    supervision types or specialized purpose for incarceration. This deduplication is handled further down in the query. 
    
    Create a field that identifies the compartment_level_1 (incarceration vs supervision) and compartment_level_2.
    
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
        'INCARCERATION' as compartment_level_1,
        /* TODO(#6126): Investigate ID missing reason for incarceration */
        CASE 
            WHEN state_code = 'US_ND' AND facility = 'CPP' 
                THEN 'COMMUNITY_PLACEMENT_PROGRAM'
            ELSE COALESCE(specialized_purpose_for_incarceration, 'GENERAL') END as compartment_level_2,
        facility AS compartment_location,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type
    FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized`
    WHERE state_code in ('{supported_states}')
        AND state_code <> 'US_ID'
    UNION ALL
    -- Use Idaho preprocessed dataset to deal with state-specific logic
    SELECT *
    FROM `{project_id}.{analyst_dataset}.us_id_incarceration_population_metrics_preprocessed_materialized`
    UNION ALL
    SELECT 
        DISTINCT
        person_id,
        date_of_supervision AS date,
        metric_type AS metric_source,
        created_on,       
        state_code,
        'SUPERVISION' as compartment_level_1,
        supervision_type as compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        supervision_level AS correctional_level,
        supervising_officer_external_id,
        case_type
    FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`
    WHERE state_code in ('{supported_states}')
    UNION ALL
    SELECT 
        DISTINCT
        person_id,
        date_of_supervision AS date,
        metric_type AS metric_source,
        created_on,       
        state_code,
        'SUPERVISION_OUT_OF_STATE' as compartment_level_1,
        supervision_type as compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        supervision_level AS correctional_level,
        supervising_officer_external_id,
        case_type
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_out_of_state_population_metrics_materialized`
    WHERE state_code in ('{supported_states}')
    )
    ,
    last_day_of_data_by_state_and_source AS
    /*
    Get the max date for each state and population source, and then the min of these dates for each state. This is to 
    be used as the 'current' date for which we assume anyone listed on this date is still in that compartment. 
    */
    (
    SELECT 
        state_code,
        metric_source,
        MAX(date) AS last_day_of_data
    FROM population_cte
    GROUP BY 1,2
    )
    ,
    last_day_of_data_by_state AS
    (
    SELECT 
        state_code,
        MIN(last_day_of_data) last_day_of_data
    FROM last_day_of_data_by_state_and_source
    GROUP BY 1 ORDER BY 1
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
        a.metric_source,
        a.compartment_level_1,
        COALESCE(a.compartment_level_2, b.compartment_level_2) compartment_level_2,
        a.compartment_location,
        a.correctional_level,
        a.supervising_officer_external_id,
        a.case_type 
    FROM population_cte a 
    LEFT JOIN  population_cte b 
    ON a.person_id = b.person_id
        AND a.date = b.date
        AND a.metric_source = b.metric_source
        AND b.compartment_level_2 IS NOT NULL
    )
    ,
    dedup_correctional_level AS
    /*
    Select a single correctional level for a given date, prioritizing higher supervision levels
    */
    (
    SELECT DISTINCT
        person_id,
        date,
        state_code,
        metric_source,
        compartment_level_1,
        correctional_level
    FROM (
        SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY person_id, date, compartment_level_1, metric_source 
                ORDER BY COALESCE(correctional_level_priority, 999)) AS rn
        FROM dedup_step_1_cte
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_level_dedup_priority` p
            USING(metric_source, correctional_level)
    )
    WHERE rn = 1    
    )
    ,
    dedup_step_2_cte AS
    /*
    Creates dual supervision category by classifying any cases where a person is listed on "PROBATION" and "PAROLE" on  
    the same day as being "DUAL".
    */
    (
    SELECT DISTINCT
        person_id,
        date,
        state_code,
        metric_source,
        compartment_level_1,
        CASE WHEN cnt > 1 AND compartment_level_1 = 'SUPERVISION' THEN 'DUAL' ELSE compartment_level_2 END AS compartment_level_2,
        compartment_location,
        supervising_officer_external_id,
        case_type,
        FROM (
            SELECT *,
            COUNT(DISTINCT(CASE WHEN compartment_level_2 IN ('PAROLE', 'PROBATION') 
                THEN compartment_level_2 END)) OVER(PARTITION BY person_id, date, compartment_level_1, metric_source) AS cnt,
            FROM dedup_step_1_cte
        )
    )
    ,
    dedup_step_3_cte AS 
    /* 
    Row aggregation step that dedups each day to a single person per metric_source, along with an associated
    array of session attributes (compartment_level_2, supervision officer, and location) in struct form.
    */
    (
    SELECT 
        person_id,
        date,
        state_code,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        compartment_location,
        correctional_level,
        supervising_officer_external_id,
        case_type,
        session_attributes,
    FROM
        (
        SELECT 
            *, 
            /* Deduplicate population metrics for a single person/date by selecting the row with highest priority 
            compartment_level_2, the highest correctional level and alphabetically first officer name and location name. */
            ROW_NUMBER() OVER(PARTITION BY person_id, date, compartment_level_1, metric_source 
                ORDER BY COALESCE(dedup.priority,999), supervising_officer_external_id, compartment_location) AS rn,
            ARRAY_AGG(STRUCT(compartment_level_2, supervising_officer_external_id, compartment_location)) 
                OVER(PARTITION BY person_id, date, compartment_level_1, metric_source) AS session_attributes
        FROM dedup_step_2_cte
        LEFT JOIN dedup_correctional_level
            USING (person_id, date, state_code, metric_source, compartment_level_1)
        LEFT JOIN `{project_id}.{analyst_dataset}.compartment_level_2_dedup_priority` dedup
            USING(compartment_level_1, compartment_level_2)
        )
    WHERE rn = 1
    )
    ,
    dedup_step_4_cte AS 
    /*
    Dedup across metric_source (INCARCERATION_POPULATION, SUPERVISION_POPULATION, SUPERVISION_OUT_OF_STATE_POPULATION),
    prioritizing in that order. Additionally this query ensures that all person / dates are less than or equal to the
    previously calculated last_day_of_data. This handles edge cases where population metric sources have different 
    max dates and the person is listed as being on multiple population metrics on those respective dates. Without this
    logic, the example above would cause a person to show up with multiple active sessions.
    */
    (
    SELECT
        person_id,
        date,
        state_code,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        compartment_location,
        correctional_level,
        supervising_officer_external_id,
        case_type,
        session_attributes,
    FROM 
        (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY person_id, date ORDER BY 
                CASE WHEN metric_source = 'INCARCERATION_POPULATION' THEN 1 
                    WHEN metric_source = 'SUPERVISION_POPULATION'  THEN 2 
                    WHEN metric_source = 'SUPERVISION_OUT_OF_STATE_POPULATION' THEN 3 END ASC) AS rn
        FROM  dedup_step_3_cte
        )
    JOIN last_day_of_data_by_state USING(state_code)
    WHERE rn = 1 
        AND date<=last_day_of_data 
    )
    ,
    sessionized_cte AS 
    /*
    Aggregate across distinct sub-sessions (continuous dates within metric_source, compartment, location, and person_id)
    and get the range of dates that define the session
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
        correctional_level,
        supervising_officer_external_id,
        case_type,
        ANY_VALUE(session_attributes) session_attributes,
        MIN(date) AS start_date,
        MAX(date) AS end_date
    FROM 
        (
        /*
        Create groups used to identify unique sessions. This is the same technique used above to identify continuous 
        dates, but now restricted to continuous dates within a compartment
        */
        SELECT *,
            DATE_SUB(DATE, INTERVAL ROW_NUMBER() OVER(PARTITION BY person_id, metric_source, 
                compartment_level_1, compartment_level_2, compartment_location, 
                correctional_level, supervising_officer_external_id, case_type
                ORDER BY date ASC) DAY) AS group_continuous_dates_in_compartment
        FROM dedup_step_4_cte
        )
    GROUP BY group_continuous_dates_in_compartment, person_id, state_code, metric_source,
        compartment_level_1, compartment_level_2,
        compartment_location, correctional_level, supervising_officer_external_id, case_type
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
        s.correctional_level,
        s.supervising_officer_external_id,
        s.case_type,
        s.session_attributes,
        s.start_date,
        CASE WHEN s.end_date < l.last_day_of_data THEN s.end_date END AS end_date,
        l.last_day_of_data
    FROM sessionized_cte s
    LEFT JOIN last_day_of_data_by_state l
        USING(state_code)
    )
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY start_date ASC) AS dataflow_session_id,
    FROM sessionized_null_end_date_cte
"""

DATAFLOW_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=DATAFLOW_SESSIONS_VIEW_NAME,
    view_query_template=DATAFLOW_SESSIONS_QUERY_TEMPLATE,
    description=DATAFLOW_SESSIONS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
    supported_states="', '".join(DATAFLOW_SESSIONS_SUPPORTED_STATES),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DATAFLOW_SESSIONS_VIEW_BUILDER.build_and_print()
