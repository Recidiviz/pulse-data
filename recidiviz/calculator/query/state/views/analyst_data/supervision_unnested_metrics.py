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
"""View tracking daily metrics at the officer-office level"""
from typing import List, Tuple

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# define supervision metrics constants
SUPERVISION_METRICS_START_DATE = "2016-01-01"
SUPERVISION_METRICS_SUPPORTED_LEVELS = [
    "supervising_officer_external_id",
    "supervision_office",
    "supervision_district",
    "state_code",
]
# define shorter names for supervision objects. This is because the tables already
# have 'supervision' in the name, so shorter column names will be easier to work with.
SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES = {
    "supervising_officer_external_id": "officer",
    "supervision_office": "office",
    "supervision_district": "district",
    "state_code": "state",
}
SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_RENAME = {
    "supervising_officer_external_id": "state_code, supervising_officer_external_id AS officer_id",
    "supervision_office": "state_code, supervision_district AS district, supervision_office AS office",
    "supervision_district": "state_code, supervision_district AS district",
    "state_code": "state_code",
}
SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS = {
    "supervising_officer_external_id": "state_code, officer_id",
    "supervision_office": "state_code, district, office",
    "supervision_district": "state_code, district",
    "state_code": "state_code",
}
# define window lengths in days supported by the pipeline
SUPERVISION_METRICS_SUPPORTED_WINDOW_DAYS = [365]


def get_supervision_unnested_metrics_view_strings_by_level(
    level: str,
) -> Tuple[str, str, str]:
    """
    Takes as input the level of the metrics dataframe, i.e. one from
    SUPERVISION_METRICS_SUPPORTED_LEVELS.

    Returns a list with four strings:
    1. view_id
    2. view_description
    3. query_template
    """

    if level not in SUPERVISION_METRICS_SUPPORTED_LEVELS:
        raise ValueError(f"`level` must be in {SUPERVISION_METRICS_SUPPORTED_LEVELS}")

    level_name = SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES[level]
    view_id = f"supervision_{level_name}_unnested_metrics"
    index_cols_long = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_RENAME[level]
    index_cols = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS[level]
    window_days_string = ", ".join(
        [
            str(SUPERVISION_METRICS_SUPPORTED_WINDOW_DAYS[0])
            if len(SUPERVISION_METRICS_SUPPORTED_WINDOW_DAYS) == 1
            else str(x)
            for x in SUPERVISION_METRICS_SUPPORTED_WINDOW_DAYS
        ]
    )

    view_description = f"""
Tracks daily {level_name}-level metrics. 
Does not materialize since this view is an intermediate step between source
events/spans tables and the supervision_{level_name}_metrics table.
"""

    # get client-period source table
    if level == "supervising_officer_external_id":
        table = "supervision_officer_sessions_materialized"
    elif level == "state_code":
        table = "compartment_sessions_materialized"
    else:
        table = "location_sessions_materialized"
    client_period_table = f"{{project_id}}.{{sessions_dataset}}.{table}"

    query_template = f"""
/*{{description}}*/

-- unnested date array starting at SUPERVISION_METRICS_START_DATE 
-- through the last day of the most recent complete month
WITH date_array AS (
    SELECT
        date,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "{SUPERVISION_METRICS_START_DATE}",
            DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
            INTERVAL 1 DAY
        )) AS date
)

-- client assignments to {level_name}
, {level_name}_assignments AS (
    SELECT
        {index_cols_long},
        person_id,
        start_date AS assignment_date,
        end_date,
    FROM
        `{client_period_table}`
    WHERE
        {level} IS NOT NULL
        {"AND compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')" if level_name == "state" else ""}
)

-- get day-level associations between {level_name} and client, which may be overlapping
, {level_name}_client_periods_unnested AS (
    SELECT
        {index_cols},
        person_id,
        date,
    FROM
        {level_name}_assignments
    INNER JOIN
        date_array
    ON
        date BETWEEN assignment_date AND IFNULL(end_date, "9999-01-01")
)

#################
# Person-event count metrics
# All metrics here are derived from `person_events`
#################

, unnested_person_events AS (
    SELECT
        {index_cols},
        person_id,
        date,
        event,
        event_attributes,
    FROM
        {level_name}_client_periods_unnested
    INNER JOIN (
        SELECT
            state_code,
            person_id,
            event_date AS date,
            event,
            event_attributes,
        FROM
            `{{project_id}}.{{analyst_dataset}}.person_events_materialized`
    )
    USING
        (state_code, person_id, date)
)

-- now calculate aggregations using unnested_person_events
-- note that one should use COUNT DISTINCT since person_events is not deduped on day
, person_events_agg AS (
    SELECT
        {index_cols},
        date,
        
        -- transitions from supervision
        COUNT(DISTINCT IF(
            event = "LIBERTY_START"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.inflow_from_level_1") IN 
                ("SUPERVISION", "SUPERVISION_OUT_OF_STATE"),
            person_id, NULL
        )) AS successful_completions,
        COUNT(DISTINCT IF(
            event = "INCARCERATION_START"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.inflow_from_level_1") IN 
                ("SUPERVISION", "SUPERVISION_OUT_OF_STATE"),
            person_id, NULL
        )) AS incarcerations_all,
        COUNT(DISTINCT IF(
            event = "INCARCERATION_START_TEMPORARY"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.inflow_from_level_1") IN 
                ("SUPERVISION", "SUPERVISION_OUT_OF_STATE"),
            person_id, NULL
        )) AS incarcerations_temporary,
        
        -- absconsions/bench warrants
        COUNT(DISTINCT IF(
            event = "ABSCONSION_BENCH_WARRANT", person_id, NULL
        )) AS absconsions_bench_warrants,
        
        -- early discharge requests
        COUNT(DISTINCT IF(
            event = "EARLY_DISCHARGE_REQUEST", person_id, NULL
        )) AS early_discharge_requests,
        
        -- supervision level changes
        COUNT(DISTINCT IF(
            event = "SUPERVISION_LEVEL_CHANGE"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.change_type") = "DOWNGRADE",
            person_id, NULL
        )) AS supervision_downgrades,
        COUNT(DISTINCT IF(
            event = "SUPERVISION_LEVEL_CHANGE"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.change_type") = "UPGRADE",
            person_id, NULL
        )) AS supervision_upgrades,
        COUNT(DISTINCT IF(
            event = "SUPERVISION_LEVEL_CHANGE"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.new_supervision_level") = 
                "LIMITED",
            person_id, NULL
        )) AS supervision_downgrades_to_limited,
        
        -- violations, max one per day
        COUNT(DISTINCT IF(
            event = "VIOLATION", person_id, NULL
        )) AS violations,
        COUNT(DISTINCT IF(
            event = "VIOLATION"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.violation_type") = "ABSCONDED",
            person_id, NULL
        )) AS violations_absconded,
        COUNT(DISTINCT IF(
            event = "VIOLATION"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.violation_type") IN 
                ("FELONY", "LAW", "MISDEMEANOR"),
            person_id, NULL
        )) AS violations_new_crime,
        COUNT(DISTINCT IF(
            event = "VIOLATION"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.violation_type") = "TECHNICAL",
            person_id, NULL
        )) AS violations_technical,     
        
        -- violation responses, max one per day
        COUNT(DISTINCT IF(
            event = "VIOLATION_RESPONSE", person_id, NULL
        )) AS violation_responses,
        COUNT(DISTINCT IF(
            event = "VIOLATION_RESPONSE"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.most_serious_violation_type")
                = "ABSCONDED",
            person_id, NULL
        )) AS violation_responses_absconded,
        COUNT(DISTINCT IF(
            event = "VIOLATION_RESPONSE"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.most_serious_violation_type")
                IN ("FELONY", "LAW", "MISDEMEANOR"),
            person_id, NULL
        )) AS violation_responses_new_crime,
        COUNT(DISTINCT IF(
            event = "VIOLATION_RESPONSE"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.most_serious_violation_type") 
                = "TECHNICAL",
            person_id, NULL
        )) AS violation_responses_technical,   
        
        -- drug screens
        COUNT(DISTINCT IF(
            event = "DRUG_SCREEN", person_id, NULL
        )) AS drug_screens_all,
        COUNT(DISTINCT IF(
            event = "DRUG_SCREEN"
            AND SAFE_CAST(
                JSON_EXTRACT_SCALAR(event_attributes, "$.is_positive_result") AS BOOL
            ), person_id, NULL
        )) AS drug_screens_positive,
        
        -- assessment score metrics
        COUNT(DISTINCT IF(
            event = "RISK_SCORE_ASSESSMENT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.assessment_type") = "LSIR",
            person_id, NULL
        )) AS lsir_assessments_any_officer,
        COUNT(DISTINCT IF(
            event = "RISK_SCORE_ASSESSMENT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.assessment_type") = "LSIR"
            AND SAFE_CAST(
                JSON_EXTRACT_SCALAR(event_attributes, "$.assessment_score_change") 
                AS INT
            ) > 0,
            person_id, NULL
        )) AS lsir_risk_increase_any_officer,
        COUNT(DISTINCT IF(
            event = "RISK_SCORE_ASSESSMENT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.assessment_type") = "LSIR"
            AND SAFE_CAST(
                JSON_EXTRACT_SCALAR(event_attributes, "$.assessment_score_change") 
                AS INT
            ) < 0,
            person_id, NULL
        )) AS lsir_risk_decrease_any_officer,
        AVG(IF(
            event = "RISK_SCORE_ASSESSMENT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.assessment_type") = "LSIR",
            # note this includes re-assessments with no score change
            SAFE_CAST(
                JSON_EXTRACT_SCALAR(event_attributes, "$.assessment_score_change") 
                AS INT
            ), NULL
        )) AS avg_lsir_score_change_any_officer,
        
        -- contacts
        COUNT(DISTINCT IF(
            event = "SUPERVISION_CONTACT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_status") = "COMPLETED",
            person_id, NULL
        )) AS contacts_completed,
        COUNT(DISTINCT IF(
            event = "SUPERVISION_CONTACT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_status") = "ATTEMPTED",
            person_id, NULL
        )) AS contacts_attempted,
        COUNT(DISTINCT IF(
            event = "SUPERVISION_CONTACT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_status") = "COMPLETED"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.location") = "RESIDENCE"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_type") IN
                ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT"),
            person_id, NULL
        )) AS contacts_home_visit,
        COUNT(DISTINCT IF(
            event = "SUPERVISION_CONTACT"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_status") = "COMPLETED"
            AND JSON_EXTRACT_SCALAR(event_attributes, "$.contact_type") IN 
                ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT"),
            person_id, NULL
        )) AS contacts_face_to_face,
         
        -- employment
        COUNT(DISTINCT IF(
            event = "EMPLOYMENT_STATUS_CHANGE"
            AND SAFE_CAST(
                JSON_EXTRACT_SCALAR(event_attributes, "$.is_employed") AS BOOL
            ), person_id, NULL
        )) AS employment_gained,
        COUNT(DISTINCT IF(
            event = "EMPLOYMENT_STATUS_CHANGE"
            AND NOT SAFE_CAST(
                JSON_EXTRACT_SCALAR(event_attributes, "$.is_employed") AS BOOL
            ), person_id, NULL
        )) AS employment_lost,
    FROM
        unnested_person_events
    GROUP BY
        {index_cols}, date
)

#################
# Person-span metrics
# All metrics here are derived from `person_spans`
#################

, person_spans_agg AS (
    SELECT
        {index_cols},
        date,

        -- compartment_sessions-derived metrics
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") = 
                "SUPERVISION",
            ps.person_id, NULL
        )) AS daily_population,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") = 
                "SUPERVISION_OUT_OF_STATE",
            ps.person_id, NULL
        )) AS population_out_of_state,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_2") IN
                ("PAROLE", "DUAL"),
            ps.person_id, NULL
        )) AS population_parole,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_2") IN
                ("PROBATION", "INFORMAL_PROBATION"),
            ps.person_id, NULL
        )) AS population_probation,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") =
                "SUPERVISION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_2") =
                "COMMUNITY_CONFINEMENT",
            ps.person_id, NULL
        )) AS population_community_confinement,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") IN
                ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_2") IN (
                "BENCH_WARRANT", "ABSCONSION", "INTERNAL_UNKNOWN"
            ), ps.person_id, NULL
        )) AS population_other_supervision_type,

        -- person_demographics-derived metrics
        COUNT(DISTINCT IF(
            span = "PERSON_DEMOGRAPHICS"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.gender") = "FEMALE",
            ps.person_id, NULL
        )) AS population_female,
        COUNT(DISTINCT IF(
            span = "PERSON_DEMOGRAPHICS"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.prioritized_race_or_ethnicity") 
                != "WHITE",
            ps.person_id, NULL
        )) AS population_nonwhite,
        AVG(IF(
            span = "PERSON_DEMOGRAPHICS",
            DATE_DIFF(
                date,
                SAFE_CAST(JSON_EXTRACT_SCALAR(span_attributes, "$.birthdate") AS DATE),
                DAY
            ) / 365.25, NULL
        )) AS avg_age,
                
        -- case types
        -- TODO(#10631) replace with case_type_sessions once built
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") = "GENERAL",
            ps.person_id, NULL
        )) AS population_general_case_type,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") = 
                "DOMESTIC_VIOLENCE",
            ps.person_id, NULL
        )) AS population_domestic_violence_case_type,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") = 
                "SEX_OFFENSE",
            ps.person_id, NULL
        )) AS population_sex_offense_case_type,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") = 
                "DRUG_COURT",
            ps.person_id, NULL
        )) AS population_drug_case_type,        
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") IN (
                "SERIOUS_MENTAL_ILLNESS", "MENTAL_HEALTH_COURT"
            ), ps.person_id, NULL
        )) AS population_mental_health_case_type,  
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") NOT IN (
                "GENERAL", "DOMESTIC_VIOLENCE", "SEX_OFFENSE", "DRUG_COURT", 
                "SERIOUS_MENTAL_ILLNESS", "MENTAL_HEALTH_COURT"
            ), ps.person_id, NULL
        )) AS population_other_case_type,
        COUNT(DISTINCT IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.case_type_start") IS NULL,
            ps.person_id, NULL
        )) AS population_unknown_case_type,        
        
        -- assessment_score_sessions-derived metrics
        AVG(IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_type") = "LSIR",
            SAFE_CAST(
                JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_score") AS INT
            ), NULL
        )) AS avg_lsir_score,
        AVG(IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_type") = "LSIR",
            DATE_DIFF(date, ps.start_date, DAY), NULL
        )) AS avg_days_since_latest_lsir,
        COUNT(DISTINCT IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND SAFE_CAST(
                JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_score") AS INT
            ) IS NULL,
            ps.person_id, NULL
        )) AS population_no_lsir_score,
        COUNT(DISTINCT IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_level") IN 
                ("LOW", "LOW_MEDIUM", "MINIMUM"),
            ps.person_id, NULL
        )) AS population_low_risk_level,
        COUNT(DISTINCT IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_level") IN 
                ("HIGH", "MEDIUM_HIGH", "MAXIMUM", "VERY_HIGH"),
            ps.person_id, NULL
        )) AS population_high_risk_level,
        COUNT(DISTINCT IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND (
                JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_level") IS NULL
                OR JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_level") LIKE 
                "%UNKNOWN"
            ), ps.person_id, NULL
        )) AS population_unknown_risk_level,
            
        -- employment_sessions-derived metrics
        COUNT(DISTINCT IF(
            span = "EMPLOYMENT_PERIOD",
            ps.person_id, NULL
        )) AS population_is_employed,

        -- contact_sessions-derived metrics
        AVG(IF(
            span = "COMPLETED_CONTACT_SESSION",
            DATE_DIFF(date, ps.start_date, DAY), NULL
        )) AS avg_days_since_latest_completed_contact,
        COUNT(DISTINCT IF(
            span = "COMPLETED_CONTACT_SESSION"
            AND DATE_DIFF(date, ps.start_date, DAY) > 365, ps.person_id, NULL
        )) AS population_no_completed_contact_past_1yr,

    FROM
        date_array d
    INNER JOIN
        `{{project_id}}.{{analyst_dataset}}.person_spans_materialized` ps
    ON
        d.date BETWEEN ps.start_date AND IFNULL(ps.end_date, "9999-01-01")
    -- assignment here is date person assigned to {level_name}
    INNER JOIN (
        SELECT * EXCEPT(state_code) FROM {level_name}_assignments
    ) assign ON
        d.date BETWEEN assign.assignment_date AND IFNULL(assign.end_date, "9999-01-01")
        AND ps.person_id = assign.person_id
        
    GROUP BY {index_cols}, date

)

-- add person span metrics dependent on value at {level_name}-assignment and not current
-- value, e.g. assessment score at assignment regardless of current value)
, person_spans_assignment_agg AS (

    SELECT
        {index_cols},
        date,
        
        -- current average lsir score from assignment
        AVG(IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_type") = "LSIR",
            SAFE_CAST(JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_score") AS INT),
            NULL
        )) AS avg_lsir_score_at_assignment,
        
        -- current count of clients without lsir scores at assignment
        COUNT(DISTINCT IF(
            span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_type") = "LSIR"
            AND SAFE_CAST(JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_score") 
                AS INT) IS NULL,
            assign.person_id, NULL
        )) AS population_no_lsir_score_at_assignment,
        
    FROM
        date_array d
    INNER JOIN (
        SELECT * EXCEPT(state_code) FROM {level_name}_assignments
    ) assign ON
        d.date BETWEEN assign.assignment_date AND IFNULL(assign.end_date, "9999-01-01")
    INNER JOIN
        `{{project_id}}.{{analyst_dataset}}.person_spans_materialized` ps
    ON
        -- next line is what's different from person_spans_agg
        assign.assignment_date BETWEEN ps.start_date AND IFNULL(ps.end_date, "9999-01-01")
        AND assign.person_id = ps.person_id
        
    GROUP BY {index_cols}, date
        
)

#################
# Window Metrics
# Calculates metrics such as days incarcerated, days employed, and employment stability, 
# over a window following initial assignment to {level_name}.
# These metrics depend on {level_name} assignment date and are calculated from
# number of events in `person_events` or time in `person_spans`.
#################

-- define supported window lengths (for now, just one year)
-- later we'll append "_365", etc. to the end of each metric
-- Should we require that windows be complete to include the metric?
, supported_windows AS (
    SELECT
        {window_days_string} AS window_length,
)

-- first, get window event metrics at person-assignment_date level
-- we'll do this separately from span metrics to avoid duplicated rows
, window_event_metrics AS (
    
    SELECT
        assign.person_id,
        {index_cols},
        date,
        window_length,
        
        -- days from {level_name} assignment to first incarceration (within window)
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "INCARCERATION_START", event_date, NULL),
                DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_incarceration,
         
         -- days to first absconsion/bench warrant (legal status)
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "ABSCONSION_BENCH_WARRANT", event_date, NULL),
                DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_absconsion_bench_warrant,
         
        -- days to first absconsion violation
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "VIOLATION" AND JSON_EXTRACT_SCALAR(
                    event_attributes, "$.violation_type") = "ABSCONDED",
                    event_date, NULL
                ), DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_absconsion_violation,
         
        -- days to first new crime violation
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "VIOLATION" AND JSON_EXTRACT_SCALAR(
                    event_attributes, "$.violation_type") IN 
                    ("FELONY", "LAW", "MISDEMEANOR"),
                    event_date, NULL
                ), DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_new_crime_violation,
         
        -- days to first technical violation
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "VIOLATION" AND JSON_EXTRACT_SCALAR(
                    event_attributes, "$.violation_type") = "TECHNICAL",
                    event_date, NULL
                ), DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_technical_violation,
         
        -- days to first absconsion violation response
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "VIOLATION_RESPONSE" AND JSON_EXTRACT_SCALAR(
                    event_attributes, "$.most_severe_violation_type") = "ABSCONDED",
                    event_date, NULL
                ), DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_absconsion_violation_response,
         
        -- days to first new crime violation response
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "VIOLATION_RESPONSE" AND JSON_EXTRACT_SCALAR(
                    event_attributes, "$.most_severe_violation_type") IN 
                    ("FELONY", "LAW", "MISDEMEANOR"),
                    event_date, NULL
                ), DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_new_crime_violation_response,
         
        -- days to first technical violation response
        DATE_DIFF(
            MIN(IFNULL(
                IF(event = "VIOLATION_RESPONSE" AND JSON_EXTRACT_SCALAR(
                    event_attributes, "$.most_severe_violation_type") = "TECHNICAL",
                    event_date, NULL
                ), DATE_ADD(date, INTERVAL window_length DAY))),
            date, DAY)
         AS days_to_first_technical_violation_response,

    FROM date_array d
    CROSS JOIN supported_windows
    
    -- assignment here is date person assigned to {level_name}
    INNER JOIN 
        {level_name}_assignments assign
    ON
        d.date = assign.assignment_date
        
    -- join person events 
    -- person events must fall within `window_length` of assignment
    -- left join -> nulls filled for all metrics
    LEFT JOIN (
        SELECT * EXCEPT(state_code) FROM
            `{{project_id}}.{{analyst_dataset}}.person_events_materialized`
    ) pe ON
        event_date BETWEEN assign.assignment_date 
            AND DATE_ADD(assign.assignment_date, INTERVAL window_length DAY)
        AND pe.person_id = assign.person_id
        
    GROUP BY assign.person_id, {index_cols}, date, window_length
)
    
-- second, get window span metrics
, window_span_metrics AS (

    SELECT
        assign.person_id,
        {index_cols},
        date,
        window_length,
        
        -- days incarcerated within window of {level_name} assignment
        SUM(IF(
            span = "COMPARTMENT_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.compartment_level_1") = 
                "INCARCERATION",
            DATE_DIFF(
                LEAST(
                    IFNULL(ps.end_date, CURRENT_DATE("US/Eastern")), 
                    DATE_ADD(date, INTERVAL 365 DAY)
                -- the GREATEST here is unecessary but is a safety measure in case 
                -- we ever allow {level_name} assignments to happen while not supervised
                ), GREATEST(date, ps.start_date), DAY
            ), 0
        )) AS days_incarcerated,
        
        -- days employed within window of {level_name} assignment
        SUM(IF(
            span = "EMPLOYMENT_STATUS_SESSION"
            AND SAFE_CAST(
                JSON_EXTRACT_SCALAR(span_attributes, "$.is_employed") AS BOOL
            ), DATE_DIFF(
                LEAST(
                    IFNULL(ps.end_date, CURRENT_DATE("US/Eastern")), 
                    DATE_ADD(date, INTERVAL 365 DAY)
                ), GREATEST(date, ps.start_date), DAY
            ), 0
        )) AS days_employed,

        -- longest stint with consistent employer within window of {level_name} assignment
        MAX(IF(
            span = "EMPLOYMENT_PERIOD",
            DATE_DIFF(
                LEAST(
                    IFNULL(ps.end_date, CURRENT_DATE("US/Eastern")), 
                    DATE_ADD(date, INTERVAL 365 DAY)
                ), GREATEST(date, ps.start_date), DAY
            ), 0
        )) AS max_days_stable_employment,

        -- number of unique client-employers
        COUNT(DISTINCT IF(
            span = "EMPLOYMENT_PERIOD",
            CONCAT(
                assign.person_id,
                JSON_EXTRACT_SCALAR(span_attributes, "$.employer_name")
            ),
            NULL
        )) AS num_unique_employers,
        
        -- assessment score at assignment
        -- we use an aggregation here, but it should be one score. If >=2 scores
        -- on the same day, take the largest.
        MAX(IF(
            ps.start_date <= assign.assignment_date -- score overlaps assignment
            AND span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_type") = "LSIR",
            SAFE_CAST(JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_score") AS INT),
            NULL
        )) AS lsir_score_at_assignment_cohort,
        
        -- number of people with scores at assignment
        COUNT(DISTINCT IF(
            ps.start_date <= assign.assignment_date -- score overlaps assignment
            AND span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(span_attributes, "$.assessment_type") = "LSIR"
            AND SAFE_CAST(JSON_EXTRACT_SCALAR(
                span_attributes, "$.assessment_score") AS INT) IS NOT NULL,
            assign.person_id, NULL
        )) AS lsir_score_present_at_assignment_cohort,
    
    FROM date_array d
    CROSS JOIN supported_windows
    
    -- assignment here is date person assigned to {level_name}
    INNER JOIN
        {level_name}_assignments assign
    ON
        d.date = assign.assignment_date
        
    -- join person spans
    -- person spans must overlap some period of time between assignment date and
    --  assignment date + window. Left join to allow zeros.
    LEFT JOIN (
        SELECT * EXCEPT(state_code) FROM
            `{{project_id}}.{{analyst_dataset}}.person_spans_materialized`
    ) ps ON
        ps.person_id = assign.person_id
        AND (
            ps.start_date BETWEEN assign.assignment_date 
                AND DATE_ADD(assign.assignment_date, INTERVAL window_length DAY)
            OR assign.assignment_date BETWEEN ps.start_date 
                AND IFNULL(ps.end_date, "9999-01-01")
        )
        
    GROUP BY assign.person_id, {index_cols}, date, window_length
)

-- third, get window span metrics requiring joins on start _and_ end of the window
, window_span_change_metrics AS (

    SELECT
        assign.person_id,
        {index_cols},
        date,
        window_length,
        
        -- number of clients reassessed lsir within window
        COUNT(DISTINCT IF(
            ps_assignment.span = "ASSESSMENT_SCORE_SESSION"
            AND ps_window_end.span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(
                ps_assignment.span_attributes, "$.assessment_type"
            ) = "LSIR"
            AND JSON_EXTRACT_SCALAR(
                ps_window_end.span_attributes, "$.assessment_type"
            ) = "LSIR",
            assign.person_id, NULL
        )) AS lsir_clients_reassessed,
        
        -- assessment score difference from assignment to end of window, if re-assessed
        AVG(IF(
            ps_assignment.span = "ASSESSMENT_SCORE_SESSION"
            AND ps_window_end.span = "ASSESSMENT_SCORE_SESSION"
            AND JSON_EXTRACT_SCALAR(
                ps_assignment.span_attributes, "$.assessment_type"
            ) = "LSIR"
            AND JSON_EXTRACT_SCALAR(
                ps_window_end.span_attributes, "$.assessment_type"
            ) = "LSIR",
            SAFE_CAST(JSON_EXTRACT_SCALAR(
                ps_window_end.span_attributes, "$.assessment_score") AS INT
            ) - SAFE_CAST(JSON_EXTRACT_SCALAR(
                ps_assignment.span_attributes, "$.assessment_score") AS INT
            ), NULL
        )) AS avg_lsir_score_change,
    
    FROM date_array d
    CROSS JOIN supported_windows
    
    -- assignment here is date person assigned to {level_name}
    INNER JOIN 
        {level_name}_assignments assign
    ON
        d.date = assign.assignment_date
        
    -- join person spans at assignment
    INNER JOIN (
        SELECT * EXCEPT(state_code) FROM
            `{{project_id}}.{{analyst_dataset}}.person_spans_materialized`
    ) ps_assignment ON
        ps_assignment.person_id = assign.person_id
        AND date BETWEEN ps_assignment.start_date 
                AND IFNULL(ps_assignment.end_date, "9999-01-01")
                
    -- join person spans at end of window
    INNER JOIN (
        SELECT * EXCEPT(state_code) FROM
            `{{project_id}}.{{analyst_dataset}}.person_spans_materialized`
    ) ps_window_end ON
        ps_window_end.person_id = ps_assignment.person_id
        AND ps_window_end.span = ps_assignment.span
        AND DATE_ADD(date, INTERVAL window_length DAY) BETWEEN 
            ps_window_end.start_date AND IFNULL(ps_window_end.end_date, "9999-01-01")
        -- require the spans not be identical
        AND ps_assignment.start_date != ps_window_end.start_date
    
    GROUP BY assign.person_id, {index_cols}, date, window_length
)

##################
# Aggregate window metrics to {level_name}-level (across clients)
##################

, window_metrics_agg AS
(
    SELECT
        -- index columns
        {index_cols},
        date,
        window_length,
        
        -- {level_name}-assignments within window
        -- these do not get '_X' appended
        COUNT(DISTINCT person_id) AS clients_assigned,
        SUM(lsir_score_present_at_assignment_cohort) AS lsir_score_present_at_assignment_cohort,
        AVG(lsir_score_at_assignment_cohort) AS avg_lsir_score_at_assignment_cohort,
        
        -- aggregate window metrics leaving nulls on days where no clients assigned
        -- all metric names will eventually have '_X' appended where X is window_length
        COUNT(DISTINCT person_id) * DATE_DIFF(
            LEAST(
                CURRENT_DATE("US/Eastern"),
                DATE_ADD(date, INTERVAL window_length DAY)
            ), date, DAY) AS days_since_assignment,
        SUM(days_to_first_incarceration) AS days_to_first_incarceration,
        SUM(days_to_first_absconsion_bench_warrant) AS days_to_first_absconsion_bench_warrant,
        SUM(days_to_first_absconsion_violation) AS days_to_first_absconsion_violation,
        SUM(days_to_first_new_crime_violation) AS days_to_first_new_crime_violation,
        SUM(days_to_first_technical_violation) AS days_to_first_technical_violation,
        SUM(days_to_first_absconsion_violation_response) AS days_to_first_absconsion_violation_response,
        SUM(days_to_first_new_crime_violation_response) AS days_to_first_new_crime_violation_response,
        SUM(days_to_first_technical_violation_response) AS days_to_first_technical_violation_response,
        SUM(days_incarcerated) AS days_incarcerated,
        SUM(days_employed) AS days_employed,
        SUM(max_days_stable_employment) AS max_days_stable_employment,
        SUM(num_unique_employers) AS num_unique_employers,
        SUM(lsir_clients_reassessed) AS lsir_clients_reassessed,
        AVG(avg_lsir_score_change) AS avg_lsir_score_change,

    FROM
        window_event_metrics
    FULL OUTER JOIN
        window_span_metrics
    USING
        (person_id, {index_cols}, date, window_length)
    FULL OUTER JOIN
        window_span_change_metrics
    USING
        (person_id, {index_cols}, date, window_length)
    GROUP BY {index_cols}, date, window_length
)

-- widen metrics and append window lengths to metric 
, window_metrics_wide AS (
    SELECT *
    FROM window_metrics_agg
    PIVOT (
        -- select columns to 'aggregate' - note there won't be any aggregations because
        -- each cell will have one observation
        MIN(days_since_assignment) AS days_since_assignment,
        MIN(days_to_first_incarceration) AS days_to_first_incarceration,
        MIN(days_to_first_absconsion_bench_warrant) AS days_to_first_absconsion_bench_warrant,
        MIN(days_to_first_absconsion_violation) AS days_to_first_absconsion_violation,
        MIN(days_to_first_new_crime_violation) AS days_to_first_new_crime_violation,
        MIN(days_to_first_technical_violation) AS days_to_first_technical_violation,
        MIN(days_to_first_absconsion_violation_response) AS days_to_first_absconsion_violation_response,
        MIN(days_to_first_new_crime_violation_response) AS days_to_first_new_crime_violation_response,
        MIN(days_to_first_technical_violation_response) AS days_to_first_technical_violation_response,
        MIN(days_incarcerated) AS days_incarcerated,
        MIN(days_employed) AS days_employed,
        MIN(max_days_stable_employment) AS max_days_stable_employment,
        MIN(num_unique_employers) AS num_unique_employers,
        MIN(lsir_clients_reassessed) AS lsir_clients_reassessed,
        MIN(avg_lsir_score_change) AS avg_lsir_score_change
    
    -- select column and values to pivot on
    FOR window_length IN ({window_days_string})
    )
)

# Join person span metrics, person event metrics, and window metrics
SELECT *
FROM person_spans_agg
FULL OUTER JOIN person_events_agg
USING ({index_cols}, date)
FULL OUTER JOIN person_spans_assignment_agg
USING ({index_cols}, date)
FULL OUTER JOIN window_metrics_wide
USING ({index_cols}, date)
"""

    return view_id, view_description, query_template


# init object to hold view builders
SUPERVISION_UNNESTED_METRICS_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = []

for level_string in SUPERVISION_METRICS_SUPPORTED_LEVELS:

    (
        view_id_string,
        view_description_string,
        query_template_string,
    ) = get_supervision_unnested_metrics_view_strings_by_level(level_string)

    clustering_fields = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS[
        level_string
    ].split(", ")

    SUPERVISION_UNNESTED_METRICS_VIEW_BUILDERS.append(
        SimpleBigQueryViewBuilder(
            dataset_id=ANALYST_VIEWS_DATASET,
            view_id=view_id_string,
            view_query_template=query_template_string,
            description=view_description_string,
            analyst_dataset=ANALYST_VIEWS_DATASET,
            sessions_dataset=SESSIONS_DATASET,
            clustering_fields=clustering_fields,
            should_materialize=True,
        )
    )

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in SUPERVISION_UNNESTED_METRICS_VIEW_BUILDERS:
            view_builder.build_and_print()
