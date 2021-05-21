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
"""Sessionized view of each individual. Session defined as continuous stay within a compartment"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSIONS_VIEW_NAME = "compartment_sessions"

COMPARTMENT_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual. Session defined as continuous stay within a compartment"""

COMPARTMENT_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH dataflow_session_gaps AS
    /*
    Take dataflow_sessions output and fill gaps between sessions. At this point these are identified with a 
    metric_source value of "INFERRED" but the compartment values are not specified yet. Once dataflow metrics are joined
    logic is implemented to determine what compartment the gap should represent. This gives full session coverage for
    each person from the start of their first session to the last day for which we have data.
    */
    (
    SELECT 
        person_id,
        dataflow_session_id,
        state_code,
        'INFERRED' AS metric_source,
        CAST(NULL AS STRING) AS compartment_level_1,
        CAST(NULL AS STRING) AS compartment_level_2,
        start_date,
        end_date,
        MIN(last_day_of_data) OVER(PARTITION BY state_code) AS last_day_of_data
    FROM
        (
        SELECT 
            person_id,
            dataflow_session_id,
            state_code,
            --new session starts the day after the current row's end date
            DATE_ADD(end_date, INTERVAL 1 DAY) as start_date,
            --new session ends the day before the following row's start date
            DATE_SUB(LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date ASC), INTERVAL 1 DAY) AS end_date,
            last_day_of_data
        FROM `{project_id}.{analyst_dataset}.dataflow_sessions_materialized` 
        )
    /*
    This where clause ensures that these new release records are only created when there is a gap in sessions.
    The release record start date will be greater than the release record end date when constructed from continuous 
    sessions, and will therefore be excluded. In cases where there is a currently active session, no release record will 
    be created because the release record start date will be null.
    */
    WHERE COALESCE(end_date, '9999-01-01') >= start_date
    )
    ,
    dataflow_session_full_coverage AS
    /*
    Union together the incarceration and supervision sessions with the newly created inferred sessions.
    */
    (
    SELECT 
        person_id,
        dataflow_session_id,
        state_code,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        start_date,
        end_date,
        last_day_of_data
    FROM `{project_id}.{analyst_dataset}.dataflow_sessions_materialized` 
    UNION ALL
    SELECT * FROM dataflow_session_gaps
    )
    ,
    dataflow_sessions_with_session_ids AS
    /*
    Create session_ids on the dataflow_sessions table which now has complete coverage. This is done by identifying cases
    where a person's compartment_level_1 or compartment_level_2 values change from the preceding session.
    */
    (
    SELECT     
        *,
        SUM(CASE WHEN new_compartment_level_1 OR new_compartment_level_2 THEN 1 ELSE 0 END)
            OVER(PARTITION BY person_id ORDER BY start_date) AS session_id
    FROM 
        (
        SELECT 
            *,
            COALESCE(LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date),'') != COALESCE(compartment_level_1,'') AS new_compartment_level_1,
            COALESCE(LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date),'') != COALESCE(compartment_level_2,'') AS new_compartment_level_2
        FROM dataflow_session_full_coverage
        )
    )
    ,
    sessions_aggregated AS
    /*
    Aggregate dataflow_sessions to compartment_sessions by using the newly created session_id
    */
    (
    SELECT 
        person_id,
        session_id,
        state_code,
        metric_source,
        compartment_level_1,
        compartment_level_2,
        last_day_of_data,
        MIN(start_date) AS start_date,
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
        CASE WHEN metric_source != 'INFERRED' THEN MIN(dataflow_session_id) END AS dataflow_session_id_start,
        CASE WHEN metric_source != 'INFERRED' THEN MAX(dataflow_session_id) END AS dataflow_session_id_end
    FROM dataflow_sessions_with_session_ids
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 1,2,3,4,5,6,7
    )
    ,
    sessions_joined_with_dataflow AS
    /*
    Join the sessions_aggregated cte to dataflow metrics to get start and end reasons. Also calculate inflow and 
    outflow compartments. This is all information needed to categorize gaps into compartments.
    */
    (
    SELECT 
        sessions.person_id,
        sessions.session_id,
        sessions.dataflow_session_id_start,
        sessions.dataflow_session_id_end,
        sessions.state_code,
        sessions.compartment_level_1,
        sessions.compartment_level_2,
        sessions.start_date,
        sessions.end_date,
        starts.start_reason,
        starts.start_sub_reason,
        ends.end_reason,
        sessions.metric_source,
        sessions.last_day_of_data,
        MIN(sessions.start_date) OVER(PARTITION BY sessions.person_id) AS earliest_start_date,
        LAG(sessions.compartment_level_1) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS inflow_from_level_1,
        LAG(sessions.compartment_level_2) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS inflow_from_level_2,
        LEAD(sessions.compartment_level_1) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS outflow_to_level_1,
        LEAD(sessions.compartment_level_2) OVER(PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS outflow_to_level_2,
        LAG(ends.end_reason) OVER (PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS prev_end_reason,
        LEAD(starts.start_reason) OVER (PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS next_start_reason,
    FROM sessions_aggregated AS sessions
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_session_start_reasons_materialized` starts
        ON sessions.person_id =  starts.person_id
        AND sessions.start_date = starts.start_date
        AND (sessions.compartment_level_1 = starts.compartment_level_1
        OR (starts.compartment_level_1 = 'SUPERVISION' AND sessions.compartment_level_1 = 'SUPERVISION_OUT_OF_STATE')
        OR (starts.compartment_level_1 = 'INCARCERATION' AND sessions.compartment_level_1 = 'INCARCERATION_OUT_OF_STATE'))
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_session_end_reasons_materialized` ends
    -- The release date will be a day after the session end date as the population metrics count a person towards 
    -- population based on full days within that compartment
        ON ends.end_date = DATE_ADD(sessions.end_date, INTERVAL 1 DAY)
        AND ends.person_id = sessions.person_id
        AND (sessions.compartment_level_1 = ends.compartment_level_1
        OR (ends.compartment_level_1 = 'SUPERVISION' AND sessions.compartment_level_1 = 'SUPERVISION_OUT_OF_STATE')
        OR (ends.compartment_level_1 = 'INCARCERATION' AND sessions.compartment_level_1 = 'INCARCERATION_OUT_OF_STATE'))
    )
    ,
    sessions_with_inferred_compartments AS
    /*
    The subquery uses start reasons, end reasons, inflows and outflows to categorize people into compartments. 
    Additional compartments include "RELEASE", "ABSCONSION", "DEATH", "ERRONEOUS_RELEASE", "PENDING_CUSTODY",
    "PENDING_SUPERVISION", "SUSPENSION", "INCARCERATION - OUT_OF_STATE", and "SUPERVISION - OUT_OF_STATE". Gaps where a 
    person inflows and outflows to the same compartment_level_1 and compartment_level_2 AND has null start and end 
    reasons are infilled with the same compartment values as the adjacent sessions. Ultimately the "DEATH" compartment 
    gets dropped downstream as it will always (barring data issues) be an active compartment with no outflows.
    */
    (
    SELECT 
        person_id,
        session_id,
        dataflow_session_id_start,
        dataflow_session_id_end,
        state_code,
        COALESCE(
            CASE WHEN inferred_release THEN 'RELEASE'
                WHEN inferred_escape THEN 'ABSCONSION'
                WHEN inferred_death THEN 'DEATH' 
                WHEN inferred_erroneous THEN 'ERRONEOUS_RELEASE'
                WHEN inferred_missing_data THEN LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_pending_custody THEN 'PENDING_CUSTODY' 
                WHEN inferred_pending_supervision THEN 'PENDING_SUPERVISION'
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('INCARCERATION','SUPERVISION') THEN CONCAT(LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date), '_', 'OUT_OF_STATE')
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('SUPERVISION_OUT_OF_STATE') THEN 'SUPERVISION_OUT_OF_STATE'
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('INCARCERATION_OUT_OF_STATE') THEN 'INCARCERATION_OUT_OF_STATE'
                WHEN inferred_suspension THEN 'SUSPENSION'
                ELSE compartment_level_1 END, 'INTERNAL_UNKNOWN') AS compartment_level_1,
        COALESCE(
            CASE WHEN inferred_release THEN 'RELEASE'
                WHEN inferred_escape THEN 'ABSCONSION'
                WHEN inferred_death THEN 'DEATH'
                WHEN inferred_erroneous THEN 'ERRONEOUS_RELEASE'
                WHEN inferred_missing_data THEN LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_pending_custody THEN 'PENDING_CUSTODY'
                WHEN inferred_pending_supervision THEN 'PENDING_SUPERVISION'
                WHEN inferred_oos THEN LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_suspension THEN 'SUSPENSION'
                ELSE compartment_level_2 END, 'INTERNAL_UNKNOWN') AS compartment_level_2,
        start_date,
        end_date,
        start_reason,
        start_sub_reason,
        end_reason,
        metric_source,
        last_day_of_data,
        earliest_start_date
    FROM 
        (
        SELECT 
            *,
            metric_source = 'INFERRED'
                AND prev_end_reason IN ('SENTENCE_SERVED','COMMUTED','DISCHARGE','EXPIRATION','PARDONED') AS inferred_release,
            metric_source = 'INFERRED' 
                AND (prev_end_reason IN ('ABSCONSION','ESCAPE') OR next_start_reason IN ('RETURN_FROM_ABSCONSION','RETURN_FROM_ESCAPE')) AS inferred_escape,
            metric_source = 'INFERRED' 
                AND prev_end_reason = 'DEATH' AS inferred_death,
            metric_source = 'INFERRED'
                AND (prev_end_reason = 'RELEASED_IN_ERROR' OR next_start_reason = 'RETURN_FROM_ERRONEOUS_RELEASE') AS inferred_erroneous,
            metric_source = 'INFERRED'
                AND inflow_from_level_1 = 'SUPERVISION'
                AND (prev_end_reason in ('REVOCATION','RETURN_TO_INCARCERATION') 
                    OR next_start_reason in ('REVOCATION','ADMITTED_FROM_SUPERVISION','DUAL_REVOCATION','PAROLE_REVOCATION','PROBATION_REVOCATION')) AS inferred_pending_custody,
            metric_source = 'INFERRED'
                AND inflow_from_level_1 = 'INCARCERATION'
                AND prev_end_reason = 'CONDITIONAL_RELEASE' AS inferred_pending_supervision,
            metric_source = 'INFERRED'
                AND prev_end_reason = 'TRANSFER_OUT_OF_STATE' AS inferred_oos,
            metric_source = 'INFERRED'
                AND prev_end_reason = 'SUSPENSION' AS inferred_suspension,
            metric_source = 'INFERRED'
                AND inflow_from_level_1 = outflow_to_level_1 AND inflow_from_level_2 = outflow_to_level_2
                AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
                AND COALESCE(next_start_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')  AS inferred_missing_data, 
        FROM sessions_joined_with_dataflow
        )
    )
    ,
    session_with_ids_2 AS
    /*
    Recalculate session ids now that compartments have been inferred
    */
    (
    SELECT     
        * EXCEPT (session_id),
        SUM(CASE WHEN new_compartment_level_1 OR new_compartment_level_2 THEN 1 ELSE 0 END)
            OVER(PARTITION BY person_id ORDER BY start_date) AS session_id,
    FROM 
        (
        SELECT 
            *,
            COALESCE(LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date),'') != COALESCE(compartment_level_1,'') AS new_compartment_level_1,
            COALESCE(LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date),'') != COALESCE(compartment_level_2,'') AS new_compartment_level_2
        FROM sessions_with_inferred_compartments
        )
    )
    ,
    sessions_aggregated_2 AS
    /*
    Reaggregate sessions with newly inferred compartments. Also drop "DEATH" compartments (except for those which have an end date meaning there 
    is some sort of data bug).
    */
    (
    SELECT 
        person_id,
        session_id,
        state_code,
        compartment_level_1,
        compartment_level_2,
        earliest_start_date,
        last_day_of_data,
        MIN(start_date) AS start_date,
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
        SUM(CASE WHEN metric_source = 'INFERRED' THEN DATE_DIFF(COALESCE(end_date, last_day_of_data), start_date, DAY) + 1 ELSE 0 END) AS session_days_inferred,
    FROM session_with_ids_2
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 1,2,3,4,5,6,7
    )
    ,
    sessions_additional_attributes AS
    /*
    This step joins in and calculates all of the additional fieds that we need. (1) recalculates session inflows, outflows,
    days in session; (2) joins to get the start/end reasons, (3) joins to get start/end demographics
    */
    (
    SELECT 
        s.person_id,
        s.session_id,
        first.dataflow_session_id_start,
        last.dataflow_session_id_end,
        s.start_date,
        s.end_date,
        s.state_code,
        s.compartment_level_1,
        s.compartment_level_2,
        DATE_DIFF(COALESCE(s.end_date, first.last_day_of_data), s.start_date, DAY) + 1 AS session_length_days,
        s.session_days_inferred,
        first.start_reason,
        first.start_sub_reason,
        last.end_reason,
        s.earliest_start_date,
        s.last_day_of_data,
        LAG(s.compartment_level_1) OVER(PARTITION BY s.person_id ORDER BY s.start_date ASC) AS inflow_from_level_1,
        LAG(s.compartment_level_2) OVER(PARTITION BY s.person_id ORDER BY s.start_date ASC) AS inflow_from_level_2,
        LEAD(s.compartment_level_1) OVER(PARTITION BY s.person_id ORDER BY s.start_date ASC) AS outflow_to_level_1,
        LEAD(s.compartment_level_2) OVER(PARTITION BY s.person_id ORDER BY s.start_date ASC) AS outflow_to_level_2,
        start_of_session.compartment_location AS compartment_location_start,
        end_of_session.compartment_location AS compartment_location_end,
        start_of_session.correctional_level AS correctional_level_start,
        end_of_session.correctional_level AS correctional_level_end,
        CAST(FLOOR(DATE_DIFF(s.start_date, demographics.birthdate, DAY) / 365.25) AS INT64) AS age_start,
        CAST(FLOOR(DATE_DIFF(COALESCE(s.end_date, first.last_day_of_data), demographics.birthdate, DAY) / 365.25) AS INT64) AS age_end,     
        demographics.gender,
        demographics.prioritized_race_or_ethnicity,
        assessment_start.assessment_score AS assessment_score_start,
        assessment_end.assessment_score AS assessment_score_end,
        start_of_session.supervising_officer_external_id AS supervising_officer_external_id_start, 
        end_of_session.supervising_officer_external_id AS supervising_officer_external_id_end,  
    FROM sessions_aggregated_2 s
    LEFT JOIN session_with_ids_2 first
        ON s.person_id = first.person_id
        AND s.start_date = first.start_date
    LEFT JOIN session_with_ids_2 last
        ON s.person_id = last.person_id
        AND COALESCE(s.end_date,'9999-01-01') = COALESCE(last.end_date,'9999-01-01')
    LEFT JOIN `{project_id}.{analyst_dataset}.dataflow_sessions_materialized` AS start_of_session
        ON s.person_id = start_of_session.person_id
        AND s.start_date = start_of_session.start_date
    LEFT JOIN `{project_id}.{analyst_dataset}.dataflow_sessions_materialized` AS end_of_session
        ON s.person_id = end_of_session.person_id
        AND COALESCE(s.end_date, first.last_day_of_data) = COALESCE(end_of_session.end_date, end_of_session.last_day_of_data)
    LEFT JOIN `{project_id}.{analyst_dataset}.person_demographics_materialized` demographics
        ON s.person_id = demographics.person_id
    LEFT JOIN `{project_id}.{analyst_dataset}.assessment_score_sessions_materialized` assessment_start
        ON s.start_date BETWEEN assessment_start.assessment_date AND COALESCE(assessment_start.score_end_date, '9999-01-01')
        AND s.person_id = assessment_start.person_id
    LEFT JOIN `{project_id}.{analyst_dataset}.assessment_score_sessions_materialized` assessment_end
        ON COALESCE(s.end_date, '9999-01-01') BETWEEN assessment_end.assessment_date AND COALESCE(assessment_end.score_end_date, '9999-01-01')
        AND s.person_id = assessment_end.person_id
    )
    /*
    This is the same as the previous CTE but with age and assessment score buckets calculated
    */
    SELECT 
        *,
        CASE WHEN age_start <=24 THEN '<25'
            WHEN age_start <=29 THEN '25-29'
            WHEN age_start <=34 THEN '30-34'
            WHEN age_start <=39 THEN '35-39'
            WHEN age_start >=40 THEN '40+' END as age_bucket_start,
        CASE WHEN age_end <=24 THEN '<25'
            WHEN age_end <=29 THEN '25-29'
            WHEN age_end <=34 THEN '30-34'
            WHEN age_end <=39 THEN '35-39'
            WHEN age_end >=40 THEN '40+' END as age_bucket_end,
        CASE WHEN assessment_score_start<=23 THEN '0-23'
            WHEN assessment_score_start<=29 THEN '24-29'
            WHEN assessment_score_start<=38 THEN '30-38'
            WHEN assessment_score_start>=39 THEN '39+' END as assessment_score_bucket_start,
        CASE WHEN assessment_score_end<=23 THEN '0-23'
            WHEN assessment_score_end<=29 THEN '24-29'
            WHEN assessment_score_end<=38 THEN '30-38'
            WHEN assessment_score_end>=39 THEN '39+' END as assessment_score_bucket_end
    FROM sessions_additional_attributes
    WHERE NOT (compartment_level_1 = 'DEATH' AND end_date IS NULL)
    ORDER BY person_id, session_id
"""
COMPARTMENT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_VIEW_BUILDER.build_and_print()
