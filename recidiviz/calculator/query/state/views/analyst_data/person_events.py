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
"""Creates the view builder and view for client (person) events concatenated in a common
format."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    REFERENCE_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_collector import (
    SingleTaskEligibilityBigQueryViewCollector,
)
from recidiviz.task_eligibility.task_eligiblity_spans import TASK_ELIGIBILITY_DATASET_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_RAW_DATASET = "us_id_raw_data_up_to_date_views"

PERSON_EVENTS_VIEW_NAME = "person_events"

PERSON_EVENTS_VIEW_DESCRIPTION = """
View concatenating client (person) events in a common format.

Note that event_attributes is a json-formatted string of the form:

```
"{{
    attribute_name: attribute_value
}}"
```

Where both `attribute_name` and `attribute_value` are strings.

Such a formatted string can be generated directly by concatenating names and values. 
Alternatively, one can use:

```
TO_JSON_STRING(ARRAY_AGG(STRUCT(
    `attribute_name_1`,
    `attribute_name_2`
))[OFFSET(0)]) AS event_attributes,
```

to generate the json-formatted string. Such an approach is more readable but requires
grouping by person-event-event_date when events should only be tracked once per 
person-event-event_date. In situations where events can occur multiple times per 
person-day, the GROUP BY clause should include all attribute name columns as well.
"""

TASK_VIEW_BUILDERS = SingleTaskEligibilityBigQueryViewCollector()

PERSON_EVENTS_QUERY_TEMPLATE = """

-- transitions to liberty or supervision
-- only include in-state starts. So out-of-state to in-state is included, but 
-- liberty to out-of-state excluded.
SELECT
    state_code,
    person_id,
    CONCAT(compartment_level_1, "_START") AS event,
    start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        inflow_from_level_1,
        inflow_from_level_2,
        IFNULL(start_reason, "INTERNAL_UNKNOWN") AS start_reason
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized`
WHERE
    compartment_level_1 IN ("LIBERTY", "PENDING_CUSTODY", "SUPERVISION")
GROUP BY 1, 2, 3, 4

UNION ALL

-- transitions to incarceration
-- this differs from the other compartment level 1 starts since 
-- most_severe_violation_type is included
SELECT
    state_code,
    person_id,
    "INCARCERATION_START" AS event,
    start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        inflow_from_level_1,
        inflow_from_level_2,
        IFNULL(start_reason, "INTERNAL_UNKNOWN") AS start_reason,
        IFNULL(most_severe_violation_type, "INTERNAL_UNKNOWN") AS most_severe_violation_type
    ))[OFFSET(0)]) AS event_attributes,
FROM (
    SELECT
        a.state_code,
        a.person_id,
        a.start_date,
        a.inflow_from_level_1,
        a.inflow_from_level_2,
        a.start_reason,
        -- Get the first non-null start_sub_reason among incarceration starts occurring during the super session
        b.start_sub_reason AS most_severe_violation_type,
    FROM
        `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` a
    LEFT JOIN
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` b
    ON 
        a.person_id = b.person_id
        AND b.session_id BETWEEN a.session_id_start AND a.session_id_end
        AND b.start_sub_reason IS NOT NULL
    WHERE
        a.compartment_level_1 = "INCARCERATION"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.person_id, a.start_date
        ORDER BY b.start_date
    ) = 1
)
GROUP BY 1, 2, 3, 4

UNION ALL
    
-- transitions to temporary incarceration
-- only include in-state starts
SELECT
    state_code,
    person_id,
    "INCARCERATION_START_TEMPORARY" AS event,
    start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        compartment_level_2,
        inflow_from_level_1,
        inflow_from_level_2,
        IFNULL(start_reason, "INTERNAL_UNKNOWN") AS start_reason,
        IFNULL(start_sub_reason, "INTERNAL_UNKNOWN") AS most_severe_violation_type
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
WHERE
    compartment_level_1 = "INCARCERATION"
    AND compartment_level_2 IN (
        "PAROLE_BOARD_HOLD", "PENDING_CUSTODY", "TEMPORARY_CUSTODY", "SUSPENSION", 
        "SHOCK_INCARCERATION"
    )
GROUP BY 1, 2, 3, 4
    
UNION ALL

-- transitions to compartment level 2s
-- only include in-state starts
SELECT
    state_code,
    person_id,
    CONCAT(compartment_level_1, "-", compartment_leveL_2, "_START") AS event,
    start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        compartment_level_1,
        compartment_level_2,
        inflow_from_level_1,
        inflow_from_level_2,
        IFNULL(start_reason, "INTERNAL_UNKNOWN") AS start_reason,
        IFNULL(start_sub_reason, "INTERNAL_UNKNOWN") AS start_sub_reason
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
WHERE
    compartment_level_1 IN ("INCARCERATION", "LIBERTY", "SUPERVISION")
GROUP BY 1, 2, 3, 4
    
UNION ALL

-- supervision officer assigned (transition from NULL officer session to at least one 
-- non-NULL officer), can happen multiple times per person-day
SELECT
    state_code,
    person_id,
    "SUPERVISING_OFFICER_NEW_ASSIGNMENT" AS event,
    start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        supervising_officer_external_id
    ))[OFFSET(0)]) AS event_attributes,
FROM (
    SELECT *
    FROM `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized`
    QUALIFY 
        -- ORDER BY includes officer_id to make ordering deterministic, in the
        -- (rare) case multiple officers start on same day
        LAG(supervising_officer_external_id) OVER (PARTITION BY 
            person_id ORDER BY start_date, supervising_officer_external_id) IS NULL
        AND supervising_officer_external_id IS NOT NULL
)
GROUP BY 1, 2, 3, 4, supervising_officer_external_id

UNION ALL

-- supervision officer changed (transition between two supervision officer sessions 
-- with non-null officers), can happen multiple times per person-day
SELECT
    state_code,
    person_id,
    "SUPERVISING_OFFICER_CHANGE" AS event,
    start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        supervising_officer_external_id_new,
        supervising_officer_external_id_previous
    ))[OFFSET(0)]) AS event_attributes,
FROM (
    SELECT
        * EXCEPT(supervising_officer_external_id),
        supervising_officer_external_id AS supervising_officer_external_id_new,
        LAG(supervising_officer_external_id) OVER (
            PARTITION BY person_id ORDER BY start_date, supervising_officer_external_id
        ) AS supervising_officer_external_id_previous,
    FROM
        `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized`
    QUALIFY
        COALESCE(
            -- ORDER BY includes officer_id to make ordering deterministic, in the
            -- (rare) case multiple officers start on same day
            LAG(supervising_officer_external_id) OVER (PARTITION BY person_id 
                ORDER BY start_date, supervising_officer_external_id) != 
                supervising_officer_external_id, 
            FALSE
        )
)
GROUP BY 1, 2, 3, 4, supervising_officer_external_id_new,
    supervising_officer_external_id_previous

UNION ALL

-- (valid) early discharge requests
-- keep one request per person-day, regardless of decision
SELECT DISTINCT
    state_code,
    person_id,
    "EARLY_DISCHARGE_REQUEST" AS event,
    request_date AS event_date,
    CAST(NULL AS STRING) AS event_attributes,
FROM
    `{project_id}.{state_base_dataset}.state_early_discharge`
WHERE
    decision_status != "INVALID"
    AND request_date IS NOT NULL
    
UNION ALL

-- (valid) early discharge request decisions
-- keep one decision per person-day-decision_type
SELECT
    state_code,
    person_id,
    "EARLY_DISCHARGE_REQUEST_DECISION" AS event,
    decision_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        decision
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{state_base_dataset}.state_early_discharge`
WHERE
    decision_status != "INVALID"
    AND decision_date IS NOT NULL
GROUP BY 1, 2, 3, 4, decision

UNION ALL

-- supervision level changes
SELECT
    state_code,
    person_id,
    "SUPERVISION_LEVEL_CHANGE" AS event,
    start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        IF(supervision_downgrade > 0, "DOWNGRADE", "UPGRADE") AS change_type,
        previous_supervision_level,
        supervision_level AS new_supervision_level
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized`
WHERE
    supervision_downgrade > 0 OR supervision_upgrade > 0
GROUP BY 1, 2, 3, 4

UNION ALL

-- violations, keep one per person-day-event_attributes
SELECT
    state_code,
    person_id,
    "VIOLATION" AS event,
    event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        IFNULL(violation_type, "INTERNAL_UNKNOWN") AS violation_type,
        IFNULL(violation_type_subtype, "INTERNAL_UNKNOWN") AS violation_type_subtype,
        IFNULL(violation_type_subtype_raw_text, "EXTERNAL_UNKNOWN") AS violation_type_subtype_raw_text,
        IFNULL(most_severe_response_decision, "INTERNAL_UNKNOWN") AS most_severe_response_decision,
        CAST(is_most_severe_violation_type_of_all_violations AS STRING) AS is_most_severe_violation_type,
        CAST(response_date AS STRING) AS response_date,
        CAST(is_inferred_violation_date AS STRING) AS is_inferred_violation_date
    ))[OFFSET(0)]) AS event_attributes,
FROM (
    SELECT
        state_code,
        person_id,
        IFNULL(violation_date, response_date) AS event_date,
        violation_type,
        violation_type_subtype,
        violation_type_subtype_raw_text,
        most_severe_response_decision,
        is_most_severe_violation_type_of_all_violations,
        -- Indicates that event_date is hydrated with response_date because violation_date is NULL
        violation_date IS NULL AS is_inferred_violation_date,
        -- Deduplicates to the earliest response date associated with the violation date
        MIN(response_date) AS response_date
    FROM
        `{project_id}.{dataflow_dataset}.most_recent_violation_with_response_metrics_materialized`
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)
WHERE
    event_date IS NOT NULL
GROUP BY 1, 2, 3, 4, 
    violation_type, 
    violation_type_subtype,
    violation_type_subtype_raw_text,
    most_severe_response_decision, 
    is_most_severe_violation_type_of_all_violations,
    response_date, 
    is_inferred_violation_date

UNION ALL

-- violation responses, source table is deduped per person-day
SELECT
    state_code,
    person_id,
    "VIOLATION_RESPONSE" AS event,
    response_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        IFNULL(most_serious_violation_type, "INTERNAL_UNKNOWN") AS most_serious_violation_type,
        IFNULL(most_serious_violation_sub_type, "INTERNAL_UNKNOWN") AS most_serious_violation_sub_type,
        most_severe_response_decision
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.violation_responses_materialized`
WHERE
    most_severe_response_decision IS NOT NULL
GROUP BY 1, 2, 3, 4

UNION ALL

-- absconsions
SELECT
    state_code,
    person_id,
    "ABSCONSION_BENCH_WARRANT" AS event,
    start_date AS event_date,
    CAST(NULL AS STRING) AS event_attributes,
FROM 
    `{project_id}.{sessions_dataset}.absconsion_bench_warrant_sessions_materialized`

UNION ALL

-- drug screens, keep one per person-day-event_attributes
SELECT 
    state_code,
    person_id,
    "DRUG_SCREEN" AS event,
    drug_screen_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        is_positive_result,
        IFNULL(substance_detected, "INTERNAL_UNKNOWN") AS substance_detected,
        is_initial_within_supervision_super_session
    ))[OFFSET(0)]) AS event_attributes,
FROM (
    SELECT
        d.state_code,
        d.person_id,
        drug_screen_date,
        CAST(is_positive_result AS STRING) AS is_positive_result,
        substance_detected,
        CAST(
            d.is_positive_result AND
            ROW_NUMBER() OVER (PARTITION BY 
                sss.person_id, sss.supervision_super_session_id, is_positive_result
                ORDER BY drug_screen_date
            ) = 1
        AS STRING) AS is_initial_within_supervision_super_session,
    FROM
        `{project_id}.{sessions_dataset}.drug_screens_preprocessed_materialized` d
    INNER JOIN
        `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` sss
    ON
        d.person_id = sss.person_id
        AND d.drug_screen_date BETWEEN sss.start_date AND IFNULL(sss.end_date, "9999-01-01")
    WHERE
        is_positive_result IS NOT NULL
)
GROUP BY 1, 2, 3, 4, is_positive_result, substance_detected,
    is_initial_within_supervision_super_session
    
UNION ALL

-- contacts, keep one contact per person-day-event_attributes
SELECT
    state_code,
    person_id,
    "SUPERVISION_CONTACT" AS event,
    contact_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        IFNULL(contact_type, "INTERNAL_UNKNOWN") AS contact_type,
        IFNULL(location, "INTERNAL_UNKNOWN") AS contact_location,
        IFNULL(status, "INTERNAL_UNKNOWN") AS contact_status
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{state_base_dataset}.state_supervision_contact`
GROUP BY 1, 2, 3, 4, contact_type, location, status

UNION ALL

-- employment status changes
SELECT
    state_code,
    person_id,
    "EMPLOYMENT_STATUS_CHANGE" AS event,
    employment_status_start_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        CAST(is_employed AS STRING) AS is_employed
    ))[OFFSET(0)]) AS event_attributes,
FROM (
    SELECT *
    FROM `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized`
    QUALIFY 
        # only keep transitions where the person gained or lost employment
        # edge case: treat jobs at supervision start as employment gains
        # but no job at supervision start is not an employment loss
        is_employed != IFNULL(LAG(is_employed) OVER (
            PARTITION BY person_id ORDER BY employment_status_start_date
        ), FALSE)
)
GROUP BY 1, 2, 3, 4
    
UNION ALL

-- assessments, source table is deduped per person-assessment_date,
-- but I'm also grouping by assessment_type in case assessment_score_sessions
-- eventually allows overlaps (e.g. multiple ORAS types)
SELECT
    state_code,
    person_id,
    "RISK_SCORE_ASSESSMENT" AS event,
    assessment_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        assessment_type,
        assessment_score,
        assessment_score_change
    ))[OFFSET(0)]) AS event_attributes, 
FROM (
    SELECT
        a.state_code,
        a.person_id,
        assessment_type,
        assessment_date,
        CAST(assessment_score AS STRING) AS assessment_score,     
        # assessment score change within the same SSS
        IFNULL(CAST(assessment_score - LAG(assessment_score) OVER (PARTITION BY 
            a.state_code, a.person_id, assessment_type, sss.start_date
            ORDER BY assessment_date
        ) AS STRING), CAST(NULL AS STRING)) AS assessment_score_change,
    FROM
        `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` a
    LEFT JOIN
        `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` sss
    ON
        a.state_code = sss.state_code
        AND a.person_id = sss.person_id
        AND a.assessment_date BETWEEN sss.start_date AND COALESCE(sss.end_date, "9999-01-01")
    WHERE
        assessment_score IS NOT NULL
        AND assessment_type IS NOT NULL
)
GROUP BY 1, 2, 3, 4, assessment_type

UNION ALL

-- rewards and sanctions, currently US_ID only
-- TODO(#14993): reference officer behavior responses state agnostic view once created
-- keep one sanction/reward per person-day-response_type
SELECT
    "US_ID" AS state_code,
    person_id,
    UPPER(response_reward_sanction) AS event,
    action_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        response_type
    ))[OFFSET(0)]) AS event_attributes, 
FROM 
    `{project_id}.{analyst_dataset}.us_id_behavior_responses_materialized`
GROUP BY 1, 2, 3, 4, response_type

UNION ALL

-- program starts
-- TODO(#10254): reference state & program-agnostic table once created
-- keep one start per person-day-program_name
SELECT DISTINCT
    "US_ID" AS state_code,
    person_id,
    "PROGRAM_START" AS event,
    DATE(start_date) AS event_date,
    TO_JSON_STRING(STRUCT(
        "GEO_CIS" AS program_name
    )) AS event_attributes,
FROM 
    `{project_id}.{us_id_raw_dataset}.geo_cis_participants_latest` c
LEFT JOIN 
    `{project_id}.{state_base_dataset}.state_person_external_id` p
ON 
    c.person_external_id = p.external_id
    AND p.state_code = "US_ID"

UNION ALL

-- GEO CIS ends
-- TODO(#10254): reference state & program-agnostic table once created
-- keep one end per person-day-program_name
SELECT DISTINCT
    "US_ID" AS state_code,
    person_id,
    "PROGRAM_END" AS event,
    DATE(end_date) AS event_date,
    TO_JSON_STRING(STRUCT(
        "GEO_CIS" AS program_name
    )) AS event_attributes,
FROM 
    `{project_id}.{us_id_raw_dataset}.geo_cis_participants_latest` c
LEFT JOIN 
    `{project_id}.{state_base_dataset}.state_person_external_id` p
ON 
    c.person_external_id = p.external_id
    AND p.state_code = "US_ID"
WHERE 
    end_date IS NOT NULL

UNION ALL

-- opportunity eligibility starts and ends
-- keep one eligibility start per person-day-task_name
-- TODO(#17252): pull from collapsed eligibility table instead of `all_tasks_materialized`
SELECT
    state_code,
    person_id,
    "TASK_ELIGIBILITY_START" AS event,
    start_date AS event_date,
    TO_JSON_STRING(STRUCT(
        task_name
    )) AS event_attributes,
FROM
    `{project_id}.{task_eligibility_dataset}.all_tasks_materialized`
WHERE
    is_eligible

UNION ALL

-- TODO(#14994): remove once ID and PA downgrades are added to `all_tasks_materialized`
-- keep one eligibility start per person-day-task_name
SELECT DISTINCT
    state_code,
    person_id,
    "TASK_ELIGIBILITY_START" AS event,
    start_date AS event_date,
    TO_JSON_STRING(STRUCT(
        "SUPERVISION_LEVEL_DOWNGRADE" AS task_name
    )) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    recommended_supervision_downgrade_level IS NOT NULL

UNION ALL 

-- surfaced eligible opportunities granted
-- supervision downgrade recommendations corrected after being surfaced to staff
-- TODO(#14994): remove once ID and PA downgrades are added to `all_tasks_materialized`
-- keep one eligibility grant per person-day-task_name
SELECT DISTINCT
    state_code,
    person_id,
    "TASK_COMPLETED" AS event,
    end_date AS event_date,
    TO_JSON_STRING(STRUCT(
        "SUPERVISION_LEVEL_DOWNGRADE" AS task_name
    )) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    mismatch_corrected
    
UNION ALL

-- late responses to eligible opportunities eligibility
-- 7 days
-- TODO(#17252): pull from collapsed eligibility table instead of `all_tasks_materialized`
SELECT
    state_code,
    person_id,
    "TASK_ELIGIBLE_7_DAYS" AS event,
    DATE_ADD(start_date, INTERVAL 7 DAY) AS event_date,
    TO_JSON_STRING(STRUCT(
        task_name
    )) AS event_attributes,
FROM
    `{project_id}.{task_eligibility_dataset}.all_tasks_materialized`
WHERE
    is_eligible
    AND LEAST(
            IFNULL(end_date, CURRENT_DATE("US/Eastern")),
            CURRENT_DATE("US/Eastern")
        ) > DATE_ADD(start_date, INTERVAL 7 DAY)

UNION ALL

-- TODO(#14994): remove once ID and PA downgrades are added to `all_tasks_materialized`
SELECT DISTINCT
    state_code,
    person_id,
    "TASK_ELIGIBLE_7_DAYS" AS event,
    DATE_ADD(start_date, INTERVAL 7 DAY) AS event_date,
    TO_JSON_STRING(STRUCT(
        "SUPERVISION_LEVEL_DOWNGRADE" AS task_name
    )) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    recommended_supervision_downgrade_level IS NOT NULL
    AND LEAST(
            IFNULL(end_date, CURRENT_DATE("US/Eastern")),
            CURRENT_DATE("US/Eastern")
        ) > DATE_ADD(start_date, INTERVAL 7 DAY)
    
UNION ALL

-- 30 days
-- TODO(#17252): pull from collapsed eligibility table instead of `all_tasks_materialized`
SELECT
    state_code,
    person_id,
    "TASK_ELIGIBLE_30_DAYS" AS event,
    DATE_ADD(start_date, INTERVAL 30 DAY) AS event_date,
    TO_JSON_STRING(STRUCT(
        task_name
    )) AS event_attributes,
FROM
    `{project_id}.{task_eligibility_dataset}.all_tasks_materialized`
WHERE
    is_eligible
    AND LEAST(
            IFNULL(end_date, CURRENT_DATE("US/Eastern")),
            CURRENT_DATE("US/Eastern")
        ) > DATE_ADD(start_date, INTERVAL 30 DAY)

UNION ALL

-- TODO(#14994): remove once ID and PA downgrades are added to `all_tasks_materialized`
SELECT DISTINCT
    state_code,
    person_id,
    "TASK_ELIGIBLE_30_DAYS" AS event,
    DATE_ADD(start_date, INTERVAL 30 DAY) AS event_date,
    TO_JSON_STRING(STRUCT(
        "SUPERVISION_LEVEL_DOWNGRADE" AS task_name
    )) AS event_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    recommended_supervision_downgrade_level IS NOT NULL
    AND LEAST(
            IFNULL(end_date, CURRENT_DATE("US/Eastern")),
            CURRENT_DATE("US/Eastern")
        ) > DATE_ADD(start_date, INTERVAL 30 DAY)
    
UNION ALL

-- assigned to tracked experiment
-- keep one variant assignment per person-day-experiment-variant
SELECT
    state_code,
    person_id,
    "VARIANT_ASSIGNMENT" AS event,
    variant_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        experiment_id,
        variant_id
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{experiments_dataset}.person_assignments_materialized`
GROUP BY 1, 2, 3, 4, experiment_id, variant_id

UNION ALL

-- treatment referrals
SELECT
    state_code,
    person_id,
    "TREATMENT_REFERRAL" AS event,
    referral_date AS event_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        program_id,
        referring_agent_id,
        referral_metadata,
        participation_status
    ))[OFFSET(0)]) AS event_attributes,
FROM
    `{project_id}.{state_base_dataset}.state_program_assignment`
GROUP BY 1, 2, 3, 4, program_id, referring_agent_id, referral_metadata, participation_status

UNION ALL

-- task completed events
SELECT
    state_code,
    person_id,
    "TASK_COMPLETED" AS event,
    end_date AS event_date,
    TO_JSON_STRING(
        STRUCT(task_name, completion_event_type AS task_type)
    ) AS event_attributes,
FROM
    `{project_id}.{task_eligibility_dataset}.all_tasks_materialized`
INNER JOIN `{project_id}.{reference_views_dataset}.task_to_completion_event`
    USING (task_name)
WHERE
    end_reason = "TASK_COMPLETED"
"""

PERSON_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PERSON_EVENTS_VIEW_NAME,
    view_query_template=PERSON_EVENTS_QUERY_TEMPLATE,
    description=PERSON_EVENTS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    us_id_raw_dataset=US_ID_RAW_DATASET,
    experiments_dataset=EXPERIMENTS_DATASET,
    task_eligibility_dataset=TASK_ELIGIBILITY_DATASET_ID,
    should_materialize=True,
    clustering_fields=["state_code", "event"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_EVENTS_VIEW_BUILDER.build_and_print()
