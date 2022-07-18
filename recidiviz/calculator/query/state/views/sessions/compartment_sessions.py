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
"""Sessionized view of each individual. Session defined as continuous stay within a compartment"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSIONS_VIEW_NAME = "compartment_sessions"

COMPARTMENT_SESSIONS_VIEW_DESCRIPTION = """
## Overview

Compartment sessions is our most frequently used sessionized view. This table is unique on `person_id` and `session_id`. In this view a new session is triggered every time a `compartment_level_1` or `compartment_level_2` value changes. A "compartment" can generally be defined as someone's legal status within incarceration or supervision.

In the data we’ve constructed we have a hierarchy of compartment level 1 and compartment level 2. The main compartment level 1 values are used to represent incarceration and supervision. Level 2 values further categorize the legal status. For example, the supervision level 1 sessions most frequently have level 2 values of probation or parole. This view also has columns for the preceding and ensuing sessions, referred to as "inflow" and "outflow" compartments.

It is important to note that a compartment does not always equal legal status. There have been a number of cases where for analysis or reporting purposes it has been useful for us to define these slightly differently. For example, excluded facilities in ID trigger their own new compartment (`INCARCERATION_OUT_OF_STATE`) even if a person is of the same legal status because we want to be able to view those events separately.

Compartment sessions differs from other sessionized views in that the edges should correspond with dataflow start and end events incarceration admission metrics, incarceration releases, supervision starts, and supervision terminations). As such, we leverage these start/end reason values to make inference in cases where we have missing data (a person shows up in neither incarceration or supervision population metrics). The following are the `compartment_level_1` values that are generated from population metrics:

1. `INCARCERATION`
2. `SUPERVISION`
3. `SUPERVISION_OUT_OF_STATE`

## Field Definitions

|	Field	|	Description	|
|	--------------------	|	--------------------	|
|	person_id	|	Unique person identifier	|
|	session_id	|	Ordered session number per person	|
|	dataflow_session_id_start	|	Session ID from `dataflow_sessions` at start of compartment session. Dataflow sessions is a more finely disaggregated view of sessions where a new session is triggered by a change in any attribute (compartment, location, supervision officer, case type, etc)	|
|	dataflow_session_id_end	|	Session ID from `dataflow_sessions` at end of compartment session	|
|	start_date	|	Day that a person's session starts. This will correspond with the incarceration admission or supervision start date	|
|	end_date	|	Last full-day of a person's session. The release or termination date will be the `end_date` + 1 day	|
|	state_code	|	State	|
|	compartment_level_1	|	Level 1 Compartment. Possible values are: <br>-`INCARCERATION`<br>-`INCARCERATION_OUT_OF_STATE`<br>-`SUPERVISION`<br>-`SUPERVISION_OUT_OF_STATE`<br>-`RELEASE`<br>-`INTERNAL_UNKNOWN`, <br>-`PENDING_CUSTODY`<br>-`PENDING_SUPERVISION`<br>-`SUSPENSION`<br>ERRONEOUS_RELEASE	|
|	compartment_level_2	|	Level 2 Compartment. Possible values for the incarceration compartments are: <br>-`GENERAL`<br>-`PAROLE_BOARD_HOLD`<br>-`TREATMENT_IN_PRISON` <br>-`SHOCK_INCARCERATION`<br>-`ABSCONSION`<br>-`INTERNAL_UNKNOWN`<br>-`COMMUNITY_CONFINEMENT`<br>-`TEMPORARY_CUSTODY`<br><br>Possible values for the supervision compartments are: <br>-`PROBATION`<br>-`PAROLE`<br>-`ABSCONSION`<br>-`DUAL`<br>-`BENCH_WARRANT`<br>-`INFORMAL_PROBATION`<br>-`INTERNAL_UNKNOWN`<br><br>All other `compartment_level_1` values have the same value for `compartment_level_1` and `comparmtent_level_2`	|
|	session_length_days	|	Length of session in days. For active sessions this is the number of days between session start and the most recent day of data. The minimum value of this field is `1` in cases where the person has the same `start_date` and `end_date` (they spent one full day in the compartment)	|
|	session_days_inferred	|	The number of days (out of the total `session_length_days`) that are inferred. This type onference happens when we have a gap between the same compartment with no dataflow start/end events indicating a compartment transition. As an example let's say someone was in GENERAL for 30 days, then we had a gap in population data of 5 days, and then they were in GENERAL again for 30 days. If there are no dataflow events at the transition edges, we infer that that person was in GENERAL for that entire time. They would have a session length of 65 days with a value of `5` for `session_days_inferred`	|
|	start_reason	|	Reason for the session start. This is pulled from `compartment_session_start_reasons` which is generated from the union of the incarceration admission and supervision start dataflow metrics. Start events are deduplicated to unique person/days within supervision and within incarceration and then joined to sessions generated from the population metric. This field is not fully hydrated.	|
|	start_sub_reason	|	This field represents the most severe violation associated with an admission and is only populated for incarceration commitments from supervision, which includes both revocation admissions and sanction admissions.	|
|	end_reason	|	Reason for the session end. This is pulled from `compartment_session_end_reasons` which is generated from the union of the incarceration release and supervision termination dataflow metrics. End events are deduplicated to unique person/days within supervision and within incarceration and then joined to sessions generated from the population metric. If a session is currently active this field will be NULL. This field is not fully hydrated.	|
|	is_inferred_start_reason	|	Indicator for whether the start reason is inferred based on the transition inflow	|
|	is_inferred_end_reason	|	Indicator for whether the end reason is inferred based on the transition outflow	|
|	start_reason_original	|	Original start reason (will bet the same as the start reason unless the original is overwritten because of inference)	|
|	end_reason_original	|	Original end reason (will bet the same as the end reason unless the original is overwritten because of inference)	|
|	earliest_start_date	|	The first date, across all sessions, that a person appears in our population data	|
|	last_day_of_data	|	The last day for which we have data, specific to a state. The is is calculated as the state min of the max day of which we have population data across supervision and population metrics within a state. For example, if in ID the max incarceration population date value is 2021-09-01 and the max supervision population date value is 2021-09-02, we would say that the last day of data for ID is 2021-09-01.	|
|	inflow_from_level_1	|	Compartment level 1 value of the preceding session	|
|	inflow_from_level_2	|	Compartment level 2 value of the preceding session	|
|	outflow_to_level_1	|	Compartment level 1 value of the subsequent session	|
|	outflow_to_level_2	|	Compartment level 2 value of the subsequent session	|
|	compartment_location_start	|	Facility or supervision district at the start of the session	|
|	compartment_location_end	|	Facility or supervision district at the end of the session	|
|	correctional_level_start	|	A person's custody level (for incarceration sessions) or supervision level (for supervision sessions) as of the start of the session	|
|	correctional_level_end	|	A person's custody level (for incarceration sessions) or supervision level (for supervision sessions) as of the end of the session	|
|	age_start	|	Age at start of session	|
|	age_end	|	Age at end of session	|
|	gender	|	Gender	|
|	prioritized_race_or_ethnicity	|	Person's race or ethnicity. In cases where multiple race / ethnicities are listed, the least represented one for that state is chosen	|
|	assessment_score_start	|	Assessment score at start of session	|
|	assessment_score_end	|	Assessment score at end of session	|
|	supervising_officer_external_id_start	|	Supervision officer at start of session (only populated for supervision sessions)	|
|	supervising_officer_external_id_end	|	Supervision officer at end of session (only populated for supervision sessions)	|
|	age_bucket_start	|	Age bucket at start of session	|
|	age_bucket_end	|	Age bucket at end of session	|
|	assessment_score_bucket_start	|	Assessment score bucket at start of session	|
|	assessment_score_bucket_end	|	Asessment score bucket at end of session	|

## Methodology

At a high-level, the following steps are taken to generate `compartment_sessions`

1. Aggregate dataflow sessions to compartment sessions
    
    1. Uses the `compartment_level_1` and `compartment_level_2` fields to identify when these values change and creates a new `session_id` every time they do within a `person_id`

2. Join to dataflow start/end reasons
    
    1. Session start dates are joined to `compartment_session_start_reasons_materialized` and session end dates are joined to `compartment_session_end_reasons_materialized`. These views already handle deduplication in cases where there is more than one event on a given person/day. 

3. Use start/end reasons to infer compartment values when there are gaps in population data
    
    1. There are two categories of sessions compartment inference worth distinguishing: 
        
        1. **Cases that lead to further aggregation**. This is occurs when we have a gap in data with identical compartment values for the periods surrounding that gap. If this occurs _and_ there are no matching dataflow events that indicate a transition, we assume that this is missing data and that this gap should take on the compartment values of its neighboring sessions. When this occurs, those adjacent sessions will then be re-aggregated into one larger compartment session. The `session_days_inferred` can be used to identify compartment sessions that have some portion of its time inferred by this methodology 
        2. **Cases that take on an inferred compartment value**. This refers to cases where the session gap gets a new compartment value (not equal to one of our population metric-derived values) based on the dataflow start/end reasons. The most common and straightforward example of this is the `RELEASE` compartment. This occurs when we have a gap in the data where the preceding session end reason is one that would indicate the person leaving the system (`SENTENCE_SERVED`, `COMMUTED`, `DISCHARGE`, `EXPIRATION`, `PARDONED`). The full set of inferred compartment values is listed below along with the criteria used to determine compartment values.
            1. `RELEASE` - preceding end reason indicates transition to liberty
            2. `PENDING_CUSTODY` - preceding end reason of is a supervision session ending in revocation or the subsequent start reason indicates a revocation or sanction admission
            3. `PENDING_SUPERVISION` - previous end reason is a incarceration session ending in conditional release
            4. `SUSPENSION` - previous end reason is suspension
            5. `ERRONEOUS_RELEASE` - previous end reason or subsequent start reason indicate erroneous release
            6. `INCARCERATION_OUT_OF_STATE` - previous end reason indicates an incarceration transfer out of state
            7. `INTERNAL_UNKNOWN` - the value given to any gap between sessions that does not meet one of the above criteria
            
4. Use transitions and corresponding look-up table to infer start/end reasons in cases where there is no dataflow event that joins to the transition
    1. Transitions that eligible for inferred start/end reasons are maintained in `static_reference_tables.session_inferred_start_reasons_materialized` and `static_reference_tables.session_inferred_end_reasons_materialized`. The non-materialized view version of these tables maintain a live connection with the Google Sheet "Compartment Session Inferred Transition Reasons" which is located in the DADS shared folder. Materialized versions are then created by running the SQL script `update_static_reference_inferred_start_end_reasons.sql`, which is located with the rest of the sessions views.
    2. The inferred transition look-up tables specify compartment level 1 and level 2 values and corresponding transition compartments (inflows for start reasons and outflows for end reasons). Cases where this transition is observed without the valid specified start/end reason will take on the value specified in this table. The field `original_start_reason` indicates in what cases the original value gets overwritten. This can be specified as "ALL" (any start reason / end reason gets overwritten); a specific start / end reason (only transitions with that value will be overwritten); or left blank (only cases where the start/end reason is missing will it be inferred).

4. Join back to dataflow sessions and other demographic tables to get session characteristics
    
    1. Lastly, the re-aggregated compartment sessions are joined back to other views to add additional session characteristics 
"""

MO_DATA_GAP_DAYS = "10"

COMPARTMENT_SESSIONS_QUERY_TEMPLATE = """
    WITH session_attributes_unnested AS 
    (
    SELECT DISTINCT
        person_id, 
        dataflow_session_id,
        state_code,
        start_date,
        end_date,
        session_attributes.metric_source,
        session_attributes.compartment_level_1 AS compartment_level_1,
        session_attributes.compartment_level_2 AS compartment_level_2,
        session_attributes.supervising_officer_external_id AS supervising_officer_external_id,
        session_attributes.compartment_location AS compartment_location,
        session_attributes.correctional_level AS correctional_level,
        session_attributes.case_type,
        session_attributes.judicial_district_code,
        last_day_of_data,
    FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    WHERE session_attributes.compartment_level_1 != 'INCARCERATION_NOT_INCLUDED_IN_STATE'
    )
    ,
    dual_recategorization_cte AS
    (
    SELECT DISTINCT
        person_id,
        dataflow_session_id,
        state_code,
        start_date,
        end_date,
        compartment_level_1,
        CASE WHEN cnt > 1 AND compartment_level_1 = 'SUPERVISION' THEN 'DUAL' ELSE compartment_level_2 END AS compartment_level_2,
        supervising_officer_external_id,
        compartment_location,
        correctional_level,
        case_type,
        judicial_district_code,
        metric_source,
        last_day_of_data,
    FROM 
        (
        SELECT 
            *,
        COUNT(DISTINCT(CASE WHEN compartment_level_2 IN ('PAROLE', 'PROBATION') 
            THEN compartment_level_2 END)) OVER(PARTITION BY person_id, state_code, dataflow_session_id, compartment_level_1) AS cnt,
        FROM session_attributes_unnested
        )
    )
    ,
    dedup_compartment_cte AS 
    (
    SELECT 
        cte.person_id,
        cte.dataflow_session_id,
        cte.state_code,
        cte.metric_source,
        cte.compartment_level_1,
        cte.compartment_level_2,
        cte.supervising_officer_external_id,
        cte.compartment_location,
        cte.correctional_level,
        cte.case_type,
        cte.judicial_district_code,
        cte.start_date,
        cte.end_date,
        cte.last_day_of_data,
    FROM dual_recategorization_cte cte
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_1_dedup_priority` cl1_dedup
        USING(compartment_level_1)
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_2_dedup_priority` cl2_dedup
        USING(compartment_level_1, compartment_level_2)
    LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_dedup_priority` sl_dedup
        ON cte.correctional_level = sl_dedup.correctional_level
        AND cte.compartment_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE')
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, dataflow_session_id 
        ORDER BY COALESCE(cl1_dedup.priority, 999), 
                COALESCE(cl2_dedup.priority, 999),
                COALESCE(correctional_level_priority, 999),
                NULLIF(supervising_officer_external_id, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(compartment_location, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(case_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(judicial_district_code, 'EXTERNAL_UNKNOWN') NULLS LAST
                ) = 1
    )
    ,
    session_gaps_cte AS
    /*
    Take dataflow_sessions output and fill gaps between sessions. At this point these are identified with a 
    metric_source value of "INFERRED" but the compartment values are not specified yet. Once dataflow metrics are joined
    logic is implemented to determine what compartment the gap should represent. This gives full session coverage for
    each person from the start of their first session to the last day for which we have data. Session attribute values
    are also not specified but created as empty strings to allow for a later union with dataflow metrics.
    */
    (
    SELECT 
        person_id,
        dataflow_session_id,
        state_code,
        'INFERRED' AS metric_source,
        CAST(NULL AS STRING) AS compartment_level_1,
        CAST(NULL AS STRING) AS compartment_level_2,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS compartment_location,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS case_type,
        CAST(NULL AS STRING) AS judicial_district_code,
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
        FROM dedup_compartment_cte 
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
    session_full_coverage_cte AS
    /*
    Union together the incarceration and supervision sessions with the newly created inferred sessions.
    */
    (
    SELECT *
    FROM dedup_compartment_cte 
    UNION ALL
    SELECT * FROM session_gaps_cte
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
        FROM session_full_coverage_cte
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
        CASE WHEN metric_source != 'INFERRED' THEN MAX(dataflow_session_id) END AS dataflow_session_id_end,
        ANY_VALUE(supervising_officer_external_id_start) AS supervising_officer_external_id_start,
        ANY_VALUE(supervising_officer_external_id_end) AS supervising_officer_external_id_end,
        ANY_VALUE(compartment_location_start) AS compartment_location_start,
        ANY_VALUE(compartment_location_end) AS compartment_location_end,
        ANY_VALUE(correctional_level_start) AS correctional_level_start,
        ANY_VALUE(correctional_level_end) AS correctional_level_end,
        ANY_VALUE(case_type_start) AS case_type_start,
        ANY_VALUE(case_type_end) AS case_type_end,
        ANY_VALUE(judicial_district_code_start) AS judicial_district_code_start,
        ANY_VALUE(judicial_district_code_end) AS judicial_district_code_end,
        
        
    FROM 
        /*
        This logic is used to calculate the first non-null session attribute for a given person_id and session_id (and
        last non-null session attribute for the _end values
        */ 
        (
        SELECT *,
            FIRST_VALUE(supervising_officer_external_id IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id ASC) AS supervising_officer_external_id_start,
            FIRST_VALUE(supervising_officer_external_id IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id DESC) AS supervising_officer_external_id_end,
            FIRST_VALUE(compartment_location IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id ASC) AS compartment_location_start,
            FIRST_VALUE(compartment_location IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id DESC) AS compartment_location_end,
            FIRST_VALUE(correctional_level IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id ASC) AS correctional_level_start,
            FIRST_VALUE(correctional_level IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id DESC) AS correctional_level_end,
            FIRST_VALUE(case_type IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id ASC) AS case_type_start,
            FIRST_VALUE(case_type IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id DESC) AS case_type_end,
            FIRST_VALUE(judicial_district_code IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id ASC) AS judicial_district_code_start,
            FIRST_VALUE(judicial_district_code IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code 
                                                                ORDER BY dataflow_session_id DESC) AS judicial_district_code_end,
        FROM dataflow_sessions_with_session_ids
        )
    GROUP BY 1,2,3,4,5,6,7
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
        sessions.supervising_officer_external_id_start,
        sessions.supervising_officer_external_id_end,
        sessions.compartment_location_start,
        sessions.compartment_location_end,
        sessions.correctional_level_start,
        sessions.correctional_level_end,
        sessions.case_type_start,
        sessions.case_type_end,
        sessions.judicial_district_code_start,
        sessions.judicial_district_code_end,
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
        LEAD(starts.start_sub_reason) OVER (PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS next_start_sub_reason,
        LAG(sessions.end_date) OVER (PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS prev_end_date,
        LEAD(sessions.start_date) OVER (PARTITION BY sessions.person_id ORDER BY sessions.start_date ASC) AS next_start_date,
    FROM sessions_aggregated AS sessions
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_session_start_reasons_materialized` starts
        ON sessions.person_id =  starts.person_id
        AND sessions.start_date = starts.start_date
        AND (sessions.compartment_level_1 = starts.compartment_level_1
        OR (starts.compartment_level_1 = 'SUPERVISION' AND sessions.compartment_level_1 = 'SUPERVISION_OUT_OF_STATE')
        OR (starts.compartment_level_1 = 'INCARCERATION' AND sessions.compartment_level_1 = 'INCARCERATION_OUT_OF_STATE'))
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_session_end_reasons_materialized` ends
        ON ends.end_date = sessions.end_date
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
    "PENDING_SUPERVISION", "SUSPENSION", "INCARCERATION - OUT_OF_STATE", and "SUPERVISION - OUT_OF_STATE". The
    "ABSCONSION" compartment type is also applied to non-inferred sessions that have "ABSCONSION" as the start reason.
    Gaps where a person inflows and outflows to the same compartment_level_1 and compartment_level_2 AND has null start
    and end reasons are infilled with the same compartment values as the adjacent sessions. Ultimately the "DEATH"
    compartment gets dropped downstream as it will always (barring data issues) be an active compartment with no outflows.
    */
    (
    SELECT 
        person_id,
        session_id,
        dataflow_session_id_start,
        dataflow_session_id_end,
        state_code,
        COALESCE(
            CASE WHEN inferred_release OR inferred_mo_release THEN 'RELEASE'
                WHEN inferred_escape
                    OR (start_reason = 'ABSCONSION' AND COALESCE(compartment_level_2, "INTERNAL_UNKNOWN") NOT IN ("BENCH_WARRANT", "ABSCONSION"))
                    THEN LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_death THEN 'DEATH'
                WHEN inferred_erroneous THEN 'ERRONEOUS_RELEASE'
                WHEN inferred_missing_data THEN LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_pending_custody OR inferred_mo_pending_custody THEN 'PENDING_CUSTODY'
                WHEN inferred_pending_supervision THEN 'PENDING_SUPERVISION'
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('INCARCERATION','SUPERVISION') THEN CONCAT(LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date), '_', 'OUT_OF_STATE')
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('SUPERVISION_OUT_OF_STATE') THEN 'SUPERVISION_OUT_OF_STATE'
                WHEN inferred_oos AND LAG(compartment_level_1) OVER(PARTITION BY person_id ORDER BY start_date) IN ('INCARCERATION_OUT_OF_STATE') THEN 'INCARCERATION_OUT_OF_STATE'
                WHEN inferred_suspension OR inferred_mo_suspension THEN 'SUSPENSION'
                ELSE compartment_level_1 END, 'INTERNAL_UNKNOWN') AS compartment_level_1,
        COALESCE(
            CASE WHEN inferred_release OR inferred_mo_release THEN 'RELEASE'
                WHEN inferred_escape
                    OR (start_reason = 'ABSCONSION' AND COALESCE(compartment_level_2, "INTERNAL_UNKNOWN") NOT IN ("BENCH_WARRANT", "ABSCONSION"))
                    THEN 'ABSCONSION'
                WHEN inferred_death THEN 'DEATH'
                WHEN inferred_erroneous THEN 'ERRONEOUS_RELEASE'
                WHEN inferred_missing_data THEN LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_pending_custody OR inferred_mo_pending_custody THEN 'PENDING_CUSTODY'
                WHEN inferred_pending_supervision THEN 'PENDING_SUPERVISION'
                WHEN inferred_oos THEN LAG(compartment_level_2) OVER(PARTITION BY person_id ORDER BY start_date)
                WHEN inferred_suspension OR inferred_mo_suspension THEN 'SUSPENSION'
                ELSE compartment_level_2 END, 'INTERNAL_UNKNOWN') AS compartment_level_2,
        supervising_officer_external_id_start,
        supervising_officer_external_id_end,
        compartment_location_start,
        compartment_location_end,
        correctional_level_start,
        correctional_level_end,
        case_type_start,
        case_type_end,
        judicial_district_code_start,
        judicial_district_code_end,
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
                AND prev_end_reason IN (
                        'SENTENCE_SERVED','COMMUTED','DISCHARGE','EXPIRATION','PARDONED',
                        'RELEASED_FROM_ERRONEOUS_ADMISSION', 'RELEASED_FROM_TEMPORARY_CUSTODY',
                        'TEMPORARY_RELEASE', 'VACATED'
                    )  AS inferred_release,
            metric_source = 'INFERRED' 
                AND (prev_end_reason IN ('ABSCONSION','ESCAPE')
                     OR next_start_reason IN ('RETURN_FROM_ABSCONSION','RETURN_FROM_ESCAPE')
                     OR (next_start_reason = 'REVOCATION' AND next_start_sub_reason = 'ABSCONDED')
                    ) AS inferred_escape,
            metric_source = 'INFERRED' 
                AND prev_end_reason = 'DEATH' AS inferred_death,
            metric_source = 'INFERRED'
                AND (prev_end_reason = 'RELEASED_IN_ERROR' OR next_start_reason = 'RETURN_FROM_ERRONEOUS_RELEASE') AS inferred_erroneous,
            metric_source = 'INFERRED'
                AND inflow_from_level_1 = 'SUPERVISION'
                AND (prev_end_reason in ('REVOCATION', 'ADMITTED_TO_INCARCERATION') 
                    OR (next_start_reason IN ('REVOCATION', 'SANCTION_ADMISSION'))) AS inferred_pending_custody,
            metric_source = 'INFERRED'
                AND inflow_from_level_1 = 'INCARCERATION'
                AND prev_end_reason IN ('CONDITIONAL_RELEASE', 'RELEASED_TO_SUPERVISION') AS inferred_pending_supervision,
            metric_source = 'INFERRED'
                AND prev_end_reason = 'TRANSFER_TO_OTHER_JURISDICTION' AS inferred_oos,
            metric_source = 'INFERRED'
                AND prev_end_reason = 'SUSPENSION' AS inferred_suspension,
            metric_source = 'INFERRED'
                AND inflow_from_level_1 = outflow_to_level_1 AND inflow_from_level_2 = outflow_to_level_2
                AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER', 'STATUS_CHANGE')
                AND COALESCE(next_start_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER', 'STATUS_CHANGE')
                AND (state_code != 'US_MO' OR DATE_DIFF(next_start_date, prev_end_date, DAY) <= {mo_data_gap_days}) AS inferred_missing_data, 
            metric_source = 'INFERRED'
                AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
                AND COALESCE(next_start_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
                AND state_code = 'US_MO'
                AND DATE_DIFF(COALESCE(next_start_date, last_day_of_data), prev_end_date, DAY) > {mo_data_gap_days} AS inferred_mo_release,
            metric_source = 'INFERRED'
                AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
                AND next_start_reason = 'RETURN_FROM_SUSPENSION'
                AND state_code = 'US_MO' AS inferred_mo_suspension,
            metric_source = 'INFERRED'
                AND COALESCE(prev_end_reason,'INTERNAL_UNKNOWN') IN ('INTERNAL_UNKNOWN', 'TRANSFER_WITHIN_STATE', 'TRANSFER')
                AND next_start_reason IN ('NEW_ADMISSION', 'TEMPORARY_CUSTODY')
                AND state_code = 'US_MO' AS inferred_mo_pending_custody,
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
    Reaggregate sessions with newly inferred compartments.
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
        ANY_VALUE(supervising_officer_external_id_start) AS supervising_officer_external_id_start,
        ANY_VALUE(supervising_officer_external_id_end) AS supervising_officer_external_id_end,
        ANY_VALUE(compartment_location_start) AS compartment_location_start,
        ANY_VALUE(compartment_location_end) AS compartment_location_end,
        ANY_VALUE(correctional_level_start) AS correctional_level_start,
        ANY_VALUE(correctional_level_end) AS correctional_level_end,    
        ANY_VALUE(case_type_start) AS case_type_start,
        ANY_VALUE(case_type_end) AS case_type_end, 
        ANY_VALUE(judicial_district_code_start) AS judicial_district_code_start,
        ANY_VALUE(judicial_district_code_end) AS judicial_district_code_end, 
        SUM(CASE WHEN metric_source = 'INFERRED' THEN DATE_DIFF(COALESCE(end_date, last_day_of_data), start_date, DAY) + 1 ELSE 0 END) AS session_days_inferred,
    FROM
        (
        /*
        This logic is used to calculate the first non-null session attribute for a given person_id and session_id (and
        last non-null session attribute for the _end values)
        */
        SELECT
            * EXCEPT(supervising_officer_external_id_start, supervising_officer_external_id_end, compartment_location_start, compartment_location_end, correctional_level_start, correctional_level_end, case_type_start, case_type_end, judicial_district_code_start, judicial_district_code_end),
            FIRST_VALUE(supervising_officer_external_id_start IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date ASC) AS supervising_officer_external_id_start,
            FIRST_VALUE(supervising_officer_external_id_end IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date DESC) AS supervising_officer_external_id_end,
            FIRST_VALUE(compartment_location_start IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date ASC) AS compartment_location_start,
            FIRST_VALUE(compartment_location_end IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date DESC) AS compartment_location_end,
            FIRST_VALUE(correctional_level_start IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date ASC) AS correctional_level_start,
            FIRST_VALUE(correctional_level_end IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date DESC) AS correctional_level_end,
            FIRST_VALUE(case_type_start IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date ASC) AS case_type_start,
            FIRST_VALUE(case_type_end IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date DESC) AS case_type_end,
            FIRST_VALUE(judicial_district_code_start IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date ASC) AS judicial_district_code_start,
            FIRST_VALUE(judicial_district_code_end IGNORE NULLS) OVER(PARTITION BY person_id, session_id, state_code
                                                                ORDER BY start_date DESC) AS judicial_district_code_end,
        FROM session_with_ids_2
        )
    GROUP BY 1,2,3,4,5,6,7
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
        s.supervising_officer_external_id_start,
        s.supervising_officer_external_id_end,
        s.compartment_location_start,
        s.compartment_location_end,
        s.correctional_level_start,
        s.correctional_level_end,
        s.case_type_start,
        s.case_type_end,
        s.judicial_district_code_start,
        s.judicial_district_code_end,
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
        CAST(FLOOR(DATE_DIFF(s.start_date, demographics.birthdate, DAY) / 365.25) AS INT64) AS age_start,
        CAST(FLOOR(DATE_DIFF(COALESCE(s.end_date, first.last_day_of_data), demographics.birthdate, DAY) / 365.25) AS INT64) AS age_end,     
        demographics.gender,
        demographics.prioritized_race_or_ethnicity,
        assessment_end.assessment_score AS assessment_score_end,
    FROM sessions_aggregated_2 s
    LEFT JOIN session_with_ids_2 first
        ON s.person_id = first.person_id
        AND s.start_date = first.start_date
    LEFT JOIN session_with_ids_2 last
        ON s.person_id = last.person_id
        AND COALESCE(s.end_date,'9999-01-01') = COALESCE(last.end_date,'9999-01-01')
    LEFT JOIN `{project_id}.{sessions_dataset}.person_demographics_materialized` demographics
        ON s.person_id = demographics.person_id
        AND s.state_code = demographics.state_code
    LEFT JOIN `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` assessment_end
        ON COALESCE(s.end_date, '9999-01-01') BETWEEN assessment_end.assessment_date AND COALESCE(assessment_end.score_end_date, '9999-01-01')
        AND s.person_id = assessment_end.person_id
        AND s.state_code = assessment_end.state_code
    ),
    sessions_with_assessment_score_start AS
    /*
    Hydrate the `assessment_score_start` column with the earliest assessment session
    that overlaps the compartment session. Include assessments that were administered
    after the compartment session started if no assessment exists prior to the session.
    */
    (
    SELECT
        sessions.*,
        assessment_start.assessment_score AS assessment_score_start,
        ROW_NUMBER()
            OVER(PARTITION BY sessions.state_code, sessions.person_id, sessions.session_id
            ORDER BY assessment_start.assessment_date ASC
            ) AS assmt_score_start_order,
    FROM sessions_additional_attributes sessions
    LEFT JOIN `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` assessment_start
        ON assessment_start.assessment_date <= COALESCE(sessions.end_date, '9999-01-01')
        AND sessions.start_date <= COALESCE(assessment_start.score_end_date, '9999-01-01')
        AND assessment_start.person_id = sessions.person_id
        AND assessment_start.state_code = sessions.state_code
    WHERE TRUE
    QUALIFY assmt_score_start_order = 1
    )
    /*
    This cte makes two main updates (1) creates new fields with age and assessment score buckets, (2) infers start and end reasons
    for sessions that exist in the session_inferred_end_reasons and session_inferred_start_reasons tables. 
    */
    SELECT 
        person_id,
        session_id,
        dataflow_session_id_start,
        dataflow_session_id_end,
        start_date,
        end_date,
        sessions.state_code,
        sessions.compartment_level_1,
        sessions.compartment_level_2,
        session_length_days,
        session_days_inferred,
        /*
        Calculate start and end reasons by taking the one in the look-up table if it exists. Otherwise use the original. 
        Also calculate boolean flags to identify whether the start/end reason is inferred. 
        */
        COALESCE(inferred_start.start_reason, sessions.start_reason) start_reason,
        sessions.start_sub_reason,
        COALESCE(inferred_end.end_reason, sessions.end_reason) end_reason,
        COALESCE(inferred_start.is_inferred_start_reason, 0) AS is_inferred_start_reason,
        COALESCE(inferred_end.is_inferred_end_reason, 0) AS is_inferred_end_reason,
        sessions.start_reason AS start_reason_original,
        sessions.end_reason AS end_reason_original,
        earliest_start_date,
        last_day_of_data,
        sessions.inflow_from_level_1,
        sessions.inflow_from_level_2,
        sessions.outflow_to_level_1,
        sessions.outflow_to_level_2,
        age_start,
        age_end,     
        gender,
        prioritized_race_or_ethnicity,
        assessment_score_start,
        assessment_score_end,
        supervising_officer_external_id_start,
        supervising_officer_external_id_end,
        compartment_location_start,
        compartment_location_end,
        correctional_level_start,
        correctional_level_end,
        case_type_start,
        case_type_end,
        judicial_district_code_start,
        judicial_district_code_end,
        CASE WHEN age_start <=24 THEN '<25'
            WHEN age_start <=29 THEN '25-29'
            WHEN age_start <=39 THEN '30-39'
            WHEN age_start <=49 THEN '40-49'
            WHEN age_start <=59 THEN '50-59'
            WHEN age_start <=69 THEN '60-69'
            WHEN age_start >=70 THEN '70+' END as age_bucket_start,
        CASE WHEN age_end <=24 THEN '<25'
            WHEN age_end <=29 THEN '25-29'
            WHEN age_end <=39 THEN '30-39'
            WHEN age_end <=49 THEN '40-49'
            WHEN age_end <=59 THEN '50-59'
            WHEN age_end <=69 THEN '60-69'
            WHEN age_end >=70 THEN '70+' END as age_bucket_end,
        CASE WHEN assessment_score_start<=23 THEN '0-23'
            WHEN assessment_score_start<=29 THEN '24-29'
            WHEN assessment_score_start<=38 THEN '30-38'
            WHEN assessment_score_start>=39 THEN '39+' END as assessment_score_bucket_start,
        CASE WHEN assessment_score_end<=23 THEN '0-23'
            WHEN assessment_score_end<=29 THEN '24-29'
            WHEN assessment_score_end<=38 THEN '30-38'
            WHEN assessment_score_end>=39 THEN '39+' END as assessment_score_bucket_end
    FROM sessions_with_assessment_score_start sessions
    -- TODO(#8129): Productionalize start/end reason inference tables
    LEFT JOIN `{project_id}.{static_reference_tables_dataset}.session_inferred_start_reasons_materialized` inferred_start
        ON sessions.compartment_level_1 = inferred_start.compartment_level_1 
        AND sessions.compartment_level_2 = inferred_start.compartment_level_2
        AND COALESCE(sessions.inflow_from_level_1, 'NONE') = COALESCE(inferred_start.inflow_from_level_1, 'NONE')
        AND COALESCE(sessions.inflow_from_level_2, 'NONE') = COALESCE(inferred_start.inflow_from_level_2, 'NONE')
        AND sessions.state_code = inferred_start.state_code
        AND inferred_start.is_inferred_start_reason = 1
        -- Don't join or consider inferred if the session transition already has the correct reason
        AND COALESCE(inferred_start.start_reason, 'NONE') != COALESCE(sessions.start_reason, 'NONE')
        -- Needs to match the specified reason or can have "ALL" specified to overwrite any reason. If left blank
        -- only sessions with null start/end reason values will be inferred
        AND (inferred_start.original_start_reason = 'ALL'
            OR COALESCE(inferred_start.original_start_reason,'NONE') = COALESCE(sessions.start_reason,'NONE'))
    LEFT JOIN `{project_id}.{static_reference_tables_dataset}.session_inferred_end_reasons_materialized` inferred_end
        ON sessions.compartment_level_1 = inferred_end.compartment_level_1 
        AND sessions.compartment_level_2 = inferred_end.compartment_level_2
        AND sessions.outflow_to_level_1 = inferred_end.outflow_to_level_1 
        AND sessions.outflow_to_level_2 = inferred_end.outflow_to_level_2 
        AND sessions.state_code = inferred_end.state_code
        AND inferred_end.is_inferred_end_reason = 1 
        -- Don't join or consider inferred if the session transition already has the correct reason
        AND COALESCE(inferred_end.end_reason, 'NONE') != COALESCE(sessions.end_reason, 'NONE')
        -- Needs to match the specified reason or can have "ALL" specified to overwrite any reason. If left blank
        -- only sessions with null start/end reason values will be inferred
        AND (inferred_end.original_end_reason = 'ALL'
            OR COALESCE(inferred_end.original_end_reason,'NONE') = COALESCE(sessions.end_reason,'NONE'))
    WHERE NOT (sessions.compartment_level_1 = 'DEATH' AND end_date IS NULL)
"""
COMPARTMENT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    mo_data_gap_days=MO_DATA_GAP_DAYS,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_VIEW_BUILDER.build_and_print()
