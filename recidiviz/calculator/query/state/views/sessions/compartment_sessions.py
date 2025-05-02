# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
|	sub_session_id_start	|	Session ID from `compartment_sub_sessions` at start of compartment session|
|	sub_session_id_end	|	Session ID from `compartment_sub_sessions` at end of compartment session	|
|	dataflow_session_id_start	|	Session ID from `dataflow_sessions` at start of compartment session. Dataflow sessions is a more finely disaggregated view of sessions where a new session is triggered by a change in any attribute (compartment, location, supervision officer, case type, etc)	|
|	dataflow_session_id_end	|	Session ID from `dataflow_sessions` at end of compartment session	|
|	start_date	|	Day that a person's session starts. This will correspond with the incarceration admission or supervision start date	|
|	end_date	|	Last full-day of a person's session. The release or termination date will be the `end_date` + 1 day	|
|	end_date_exclusive	|	The day that a person's session ends.	|
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
|	compartment_location_start	|	Facility or supervision office & district location at the start of the session	|
|	facility_name_start	|	Facility (display name) location at the start of the session	|
|	supervision_office_name_start	|	Supervision office (display name) location at the start of the session	|
|	supervision_district_name_start	|	Supervision district (display name) location at the start of the session	|
|	supervision_region_name_start	|	Supervision region location at the start of the session	|
|	compartment_location_end	|	Facility or supervision office & district at the end of the session	|
|	facility_name_end	|	Facility (display name) location at the end of the session	|
|	supervision_office_name_end	|	Supervision office (display name) location at the end of the session	|
|	supervision_district_name_end	|	Supervision district (display name) location at the end of the session	|
|	supervision_region_name_end	|	Supervision region location at the end of the session	|
|	correctional_level_start	|	A person's custody level (for incarceration sessions) or supervision level (for supervision sessions) as of the start of the session	|
|	correctional_level_end	|	A person's custody level (for incarceration sessions) or supervision level (for supervision sessions) as of the end of the session	|
|	housing_unit_start	|	Housing unit at the start of an incarceration session	|
|	housing_unit_end	|	Housing unit at the end of an incarceration session	|
|	housing_unit_category_start	|	Housing unit category at the start of an incarceration session	|
|	housing_unit_category_end	|	Housing unit category at the end of an incarceration session	|
|	housing_unit_type_start	|	Housing unit type at the start of an incarceration session	|
|	housing_unit_type_end	|	Housing unit type at the end of an incarceration session	|
|	age_start	|	Age at start of session	|
|	age_end	|	Age at end of session	|
|	gender	|	Gender	|
|	prioritized_race_or_ethnicity	|	Person's race or ethnicity. In cases where multiple race / ethnicities are listed, the least represented one for that state is chosen	|
|	assessment_score_start	|	Assessment score at start of session	|
|	assessment_score_end	|	Assessment score at end of session	|
|	risk_assessment_score_start	|	Risk-assessment score at start of session	|
|	risk_assessment_score_end	|	Risk-assessment score at end of session	|
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
            1. `LIBERTY` - preceding end reason indicates transition to liberty
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

# TODO(#39399): Deprecate `assessment_score_start` and `assessment_score_end` fields
# once they're no longer used anywhere.
# TODO(#39399): Do we want score buckets for `risk_assessment_score_start` and
# `risk_assessment_score_end` too?
COMPARTMENT_SESSIONS_QUERY_TEMPLATE = """
    WITH sessions_aggregated AS
    /*
    This CTE aggregates together sub-sessions to compartment sessions, leveraging the `session_id` field that is created
    in `compartment_sub_sessions`. This CTE creates the _start and _end attributes in compartment_sessions from the
    values in sub-sessions and intentionally pulls attributes in a slightly different manner depending on whether we
    want the value that is true at the time of compartment session start versus the first non-null value that overlaps
    with the compartment session.

    All of these are done using the approach of constructing an ARRAY, specifying whether NULLS are included or not,
    ordering those values by the sub_session_id, and then taking the value that represents either the first or last
    value in a compartment session. This approach is taken because of the finding that using ANY_VALUE as part of a
    GROUP BY was not including / excluding NULLS as we would expect. For all of the attributes, the start versus end
    values are determined based on whether the sub_session_id is sorted ascending or descending.

    The differentiation between RESPECT NULLS and IGNORE NULLS when the arrays are constructed is used to specify
    whether we want to take the first non-null value in cases where the attribute is not present at time of compartment
    session start. This is applied to the following 5 fields: `compartment_location`, `supervising_officer_external_id`,
     `correctional_level`, `case_type`, and `assessment_score`.

    Additionally, there are two attributes (`prioritized_race_or_ethnicity` and `gender`) that do not have start/end
    values and therefore are aggregated with ANY_VALUE.
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
        CASE WHEN LOGICAL_AND(end_date_exclusive IS NOT NULL) THEN MAX(end_date_exclusive) END AS end_date_exclusive,
        ARRAY_AGG(dataflow_session_id RESPECT NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS dataflow_session_id_start,
        ARRAY_AGG(dataflow_session_id RESPECT NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS dataflow_session_id_end,
        ARRAY_AGG(sub_session_id RESPECT NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS sub_session_id_start,
        ARRAY_AGG(sub_session_id RESPECT NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS sub_session_id_end,
        ARRAY_AGG(start_reason RESPECT NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS start_reason,
        ARRAY_AGG(start_sub_reason RESPECT NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS start_sub_reason,
        ARRAY_AGG(end_reason RESPECT NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS end_reason,
        ARRAY_AGG(inflow_from_level_1 RESPECT NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS inflow_from_level_1,
        ARRAY_AGG(inflow_from_level_2 RESPECT NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS inflow_from_level_2,
        ARRAY_AGG(outflow_to_level_1 RESPECT NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS outflow_to_level_1,
        ARRAY_AGG(outflow_to_level_2 RESPECT NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS outflow_to_level_2,
        ARRAY_AGG(supervising_officer_external_id IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS supervising_officer_external_id_start,
        ARRAY_AGG(supervising_officer_external_id IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS supervising_officer_external_id_end,
        ARRAY_AGG(compartment_location IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS compartment_location_start,
        ARRAY_AGG(facility_name IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS facility_name_start,
        ARRAY_AGG(supervision_office_name IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS supervision_office_name_start,
        ARRAY_AGG(supervision_district_name IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS supervision_district_name_start,
        ARRAY_AGG(supervision_region_name IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS supervision_region_name_start,
        ARRAY_AGG(compartment_location IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS compartment_location_end,
        ARRAY_AGG(facility_name IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS facility_name_end,
        ARRAY_AGG(supervision_office_name IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS supervision_office_name_end,
        ARRAY_AGG(supervision_district_name IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS supervision_district_name_end,
        ARRAY_AGG(supervision_region_name IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS supervision_region_name_end,
        ARRAY_AGG(correctional_level IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS correctional_level_start,
        ARRAY_AGG(correctional_level IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS correctional_level_end,
        ARRAY_AGG(housing_unit IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS housing_unit_start,
        ARRAY_AGG(housing_unit IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS housing_unit_end,
        ARRAY_AGG(housing_unit_category IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS housing_unit_category_start,
        ARRAY_AGG(housing_unit_category IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS housing_unit_category_end,
        ARRAY_AGG(housing_unit_type IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS housing_unit_type_start,
        ARRAY_AGG(housing_unit_type IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS housing_unit_type_end,
        ARRAY_AGG(case_type IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS case_type_start,
        ARRAY_AGG(case_type IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS case_type_end,
        ANY_VALUE(gender) AS gender,
        ARRAY_AGG(custodial_authority IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS custodial_authority_start,
        ARRAY_AGG(custodial_authority IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS custodial_authority_end,
        ANY_VALUE(prioritized_race_or_ethnicity) AS prioritized_race_or_ethnicity,
        ARRAY_AGG(age RESPECT NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS age_start,
        --Null out the age_end for active inferred sub-sessions
        ARRAY_AGG(CASE WHEN NOT (end_date_exclusive IS NULL AND metric_source = 'INFERRED') THEN age END
             RESPECT NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)] AS age_end,
        -- TODO(#17265): Consider removing this logic following investigation of unknown assessment scores
        NULLIF(ARRAY_AGG(assessment_score IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)],-999) AS assessment_score_start,
        NULLIF(ARRAY_AGG(assessment_score IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)],-999) AS assessment_score_end,
        NULLIF(ARRAY_AGG(risk_assessment_score IGNORE NULLS ORDER BY sub_session_id ASC LIMIT 1)[SAFE_OFFSET(0)], -999) AS risk_assessment_score_start,
        NULLIF(ARRAY_AGG(risk_assessment_score IGNORE NULLS ORDER BY sub_session_id DESC LIMIT 1)[SAFE_OFFSET(0)], -999) AS risk_assessment_score_end,
        SUM(CASE WHEN metric_source = 'INFERRED'
            THEN DATE_DIFF(COALESCE(DATE_SUB(end_date_exclusive,INTERVAL 1 DAY), last_day_of_data), start_date, DAY) + 1 ELSE 0 END) AS session_days_inferred,
        SUM(DATE_DIFF(COALESCE(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), last_day_of_data), start_date, DAY)+1) AS session_length_days,
    FROM `{project_id}.{sessions_dataset}.compartment_sub_sessions_materialized`
    GROUP BY 1,2,3,4,5,6,7
    )
    SELECT
        person_id,
        session_id,
        sub_session_id_start,
        sub_session_id_end,
        dataflow_session_id_start,
        dataflow_session_id_end,
        start_date,
        DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) AS end_date,
        end_date_exclusive,
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
        risk_assessment_score_start,
        risk_assessment_score_end,
        supervising_officer_external_id_start,
        supervising_officer_external_id_end,
        compartment_location_start,
        facility_name_start,
        supervision_office_name_start,
        supervision_district_name_start,
        supervision_region_name_start,
        compartment_location_end,
        facility_name_end,
        supervision_office_name_end,
        supervision_district_name_end,
        supervision_region_name_end,
        correctional_level_start,
        correctional_level_end,
        housing_unit_start,
        housing_unit_end,
        housing_unit_category_start,
        housing_unit_category_end,
        housing_unit_type_start,
        housing_unit_type_end,
        case_type_start,
        case_type_end,
        custodial_authority_start,
        custodial_authority_end,
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
    FROM sessions_aggregated sessions
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
    WHERE NOT (sessions.compartment_level_1 = 'DEATH' AND end_date_exclusive IS NULL)
"""

COMPARTMENT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=COMPARTMENT_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_VIEW_BUILDER.build_and_print()
