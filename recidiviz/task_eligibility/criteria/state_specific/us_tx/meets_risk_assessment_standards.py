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
"""Defines a criteria view that shows spans of time for which supervision clients
meet risk assessment standards
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_MEETS_RISK_ASSESSMENT_STANDARDS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which supervision clients
meet risk assessment standards.
"""

_QUERY_TEMPLATE = f"""
WITH
-- Create periods with case type and supervision level information
comp_sub_sessions AS (
   SELECT 
      person_id,
      state_code,
      start_date,
      end_date_exclusive,
      case_type
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` AS sub_sessions
    WHERE case_type is not null
        AND state_code ='US_TX'
),
-- Aggregate above periods by case_type
case_type_periods AS (
    {aggregate_adjacent_spans(
        table_name='comp_sub_sessions',
        attribute=['case_type'],
        session_id_output_name='case_type_periods',
        end_date_field_name='end_date_exclusive'
    )}
),

-- Pull all types of events that may trigger an assessment to come due

supervision_start_dates AS (
  SELECT 
    comp_sessions.person_id, 
    comp_sessions.start_date as event_date, 
    "supervision_start_no_prior_assessment" as event_type, 
    2 as event_priority,
    case_types.case_type,
    CAST(NULL AS STRING) as assessment_level,
    CAST(NULL AS STRING) as assessment_type,
    CAST(NULL AS DATE) as assessment_date
  FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` AS comp_sessions
  -- Only include start dates without an assessment in the prior 6 months 
  -- because those clients need to be assessed with SRT or RT within 30 days. 
  -- Otherwise, the PO can use the completed assessment.
  LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_assessment` AS state_assessment
    ON comp_sessions.person_id = state_assessment.person_id
      AND comp_sessions.start_date > state_assessment.assessment_date
      AND comp_sessions.start_date < DATE_ADD(state_assessment.assessment_date, INTERVAL 6 MONTH)
  -- Pull what the case type is at the start of the supervision period
  LEFT JOIN case_type_periods as case_types
    ON comp_sessions.start_date >= case_types.start_date
      AND comp_sessions.start_date < case_types.end_date_exclusive
      AND comp_sessions.person_id = case_types.person_id
  WHERE comp_sessions.state_code = 'US_TX'
    AND compartment_level_2 = 'PAROLE'
    AND state_assessment.assessment_date is null
),

assessments_6_months_before_supervision AS (
  -- POs can use assessments completed by re-entry that are less than 6 months
  -- before supervision start. Technically there are requirements for which type
  -- of assessment must be completed, but we need incarceration data to implement
  -- that check, so we accept any assessment type here.
  SELECT 
    state_assessment.person_id, 
    -- Assessment due timeline starts from supervision start date rather than
    -- assessment completed date
    comp_sessions.start_date as event_date, 
    "supervision_start_with_prior_assessment" as event_type, 
    1 as event_priority,
    case_types.case_type,
    state_assessment.assessment_level,
    state_assessment.assessment_type,
    state_assessment.assessment_date
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_assessment` as state_assessment
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` as comp_sessions
    ON comp_sessions.person_id = state_assessment.person_id
      AND state_assessment.assessment_date >= DATE_SUB(comp_sessions.start_date, INTERVAL 6 MONTH)
      AND state_assessment.assessment_date < comp_sessions.start_date
  -- Pull what the case type is at the start of the supervision period
  LEFT JOIN case_type_periods as case_types
    ON comp_sessions.start_date >= case_types.start_date
      AND comp_sessions.start_date < case_types.end_date_exclusive
      AND comp_sessions.person_id = case_types.person_id
  -- Only include the most recent assessment prior to supervision start
  QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_assessment.person_id, comp_sessions.start_date
        ORDER BY state_assessment.assessment_date DESC
    ) = 1
),

assessments_in_supervision AS (
  SELECT 
    state_assessment.person_id, 
    assessment_date as event_date, 
    "assessment_completed" as event_type, 
    5 as event_priority,
    case_types.case_type,
    state_assessment.assessment_level,
    state_assessment.assessment_type,
    state_assessment.assessment_date
  FROM `{{project_id}}.{{normalized_state_dataset}}.state_assessment` as state_assessment
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` as sessions
    ON sessions.person_id = state_assessment.person_id
      AND state_assessment.assessment_date >= sessions.start_date
      AND state_assessment.assessment_date < sessions.end_date
  LEFT JOIN case_type_periods as case_types
    ON state_assessment.assessment_date >= case_types.start_date
      AND state_assessment.assessment_date < case_types.end_date_exclusive
      AND state_assessment.person_id = case_types.person_id
),

-- Some assessments are triggered when a client changes case type
case_type_starts AS (
  -- This will be TC Phase 2 specifically
  SELECT 
    person_id, 
    start_date as event_date, 
    "case_type_start" as event_type, 
    3 as event_priority,
    case_type,
    CAST(NULL AS STRING) as assessment_level,
    CAST(NULL AS STRING) as assessment_type,
    CAST(NULL AS DATE) as assessment_date
  FROM case_type_periods
  WHERE case_type = "SUBSTANCE ABUSE"
),
case_type_ends AS (
  SELECT 
    person_id, 
    end_date_exclusive as event_date, 
    "case_type_end" as event_type, 
    4 as event_priority,
    case_type,
    -- Need to figure out how to get immediately prior assessment_level and assessment_type
    -- Then add SUPER-INTENSIVE SUPERVISION back in (v2)
    CAST(NULL AS STRING) as assessment_level,
    CAST(NULL AS STRING) as assessment_type,
    CAST(NULL AS DATE) as assessment_date
  FROM case_type_periods
  WHERE case_type in ('ELECTRONIC MONITORING')
),

event_triggers AS (
  SELECT *
  FROM supervision_start_dates
  UNION ALL
  SELECT *
  FROM assessments_6_months_before_supervision
  UNION ALL
  SELECT *
  FROM assessments_in_supervision
  UNION ALL
  SELECT *
  FROM case_type_starts
  UNION ALL
  SELECT *
  FROM case_type_ends
),

events_with_reference_data AS (
  SELECT 
    person_id,
    event_date,
    event_type,
    event_priority,
    case_type,
    assessment_level as last_assessment_level,
    assessment_type as last_assessment_type,
    assessment_date as last_assessment_date,
    IF(event_type = 'assessment_completed', last_assessment_level, NULL) AS this_assessment_level,
    IF(event_type = 'assessment_completed', last_assessment_type, NULL) AS this_assessment_type,
    CASE 
      -- Specific rule that SISP can only use CST not CSST
      WHEN case_type = 'SUPER-INTENSIVE SUPERVISION' 
        THEN REPLACE(COALESCE(event_ref.next_assessment_type_due, cadence_ref.next_assessment_type_due), "TX_CSST", "TX_CST")
      ELSE COALESCE(event_ref.next_assessment_type_due, cadence_ref.next_assessment_type_due)
      END AS due_assessment_type,
    CAST(COALESCE(event_ref.next_assessment_due_in_months, cadence_ref.next_assessment_due_in_months) AS INT64) AS due_assessment_months,
    DATE_ADD(event_date, INTERVAL CAST(COALESCE(event_ref.next_assessment_due_in_months, cadence_ref.next_assessment_due_in_months) AS INT64) MONTH) AS due_assessment_date,
    CASE 
        WHEN COALESCE(event_ref.next_assessment_due_in_months, cadence_ref.next_assessment_due_in_months) = "1" 
            THEN "1 MONTH"
        ELSE
            CONCAT(CAST(COALESCE(event_ref.next_assessment_due_in_months, cadence_ref.next_assessment_due_in_months) AS STRING), " MONTHS") 
    END AS frequency
  FROM event_triggers
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_RiskAssessmentEvents_latest` as event_ref
    USING (event_type, case_type)
  LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_RiskAssessmentCadence_latest` as cadence_ref
    ON event_triggers.assessment_level = cadence_ref.last_assessment_level
      AND event_triggers.assessment_type = cadence_ref.last_assessment_type
      AND CAST(event_ref.use_whats_next_table AS BOOL) = TRUE
),

next_events AS (
  SELECT
    person_id,
    event_date,
    event_type,
    case_type,
    last_assessment_type,
    last_assessment_level,
    last_assessment_date,
    this_assessment_type,
    this_assessment_level,
    due_assessment_months,
    due_assessment_date,
    due_assessment_type,
    LEAD(event_date) OVER (PARTITION BY person_id ORDER BY event_date ASC, event_priority DESC) AS next_event_date,
    LEAD(event_type) OVER (PARTITION BY person_id ORDER BY event_date ASC, event_priority DESC) AS next_event_type,
    LEAD(this_assessment_type) OVER (PARTITION BY person_id ORDER BY event_date ASC, event_priority DESC) AS next_assessment_type,
    frequency
FROM events_with_reference_data
),

-- TES output:
-- Meets criteria = True from date of the triggering event to either the next event OR next assessment due date, whichever comes first.
-- Meets criteria = False from next assessment due date to next triggering event, if the due date occurs before the next event. 
meets_criteria_true_records AS (
    SELECT
        person_id,
        "US_TX" as state_code,
        event_date as start_date,
        LEAST(due_assessment_date, COALESCE(next_event_date, due_assessment_date)) AS end_date,
        TRUE AS meets_criteria,
        event_date,
        event_type,
        case_type,
        last_assessment_type,
        last_assessment_level,
        due_assessment_months,
        due_assessment_date,
        due_assessment_type,
        next_event_date,
        next_event_type,
        next_assessment_type,
        frequency,
        TO_JSON(STRUCT(
          event_date,
          event_type,
          case_type,
          last_assessment_type,
          last_assessment_level,
          due_assessment_months,
          due_assessment_date,
          due_assessment_type,
          next_event_date,
          next_event_type,
          next_assessment_type,
          frequency
        )) AS reason
    FROM next_events
),
meets_criteria_false_records AS (
    SELECT
        person_id,
        "US_TX" as state_code,
        due_assessment_date as start_date,
        next_event_date AS end_date,
        FALSE AS meets_criteria,
        event_date,
        event_type,
        case_type,
        last_assessment_type,
        last_assessment_level,
        due_assessment_months,
        due_assessment_date,
        due_assessment_type,
        next_event_date,
        next_event_type,
        next_assessment_type,
        frequency,
        TO_JSON(STRUCT(
          event_date,
          event_type,
          case_type,
          last_assessment_type,
          last_assessment_level,
          due_assessment_months,
          due_assessment_date,
          due_assessment_type,
          next_event_date,
          next_event_type,
          next_assessment_type,
          frequency
        )) AS reason
    FROM next_events
    WHERE next_event_date IS NULL OR next_event_date > due_assessment_date
)

SELECT * FROM meets_criteria_true_records
UNION ALL
SELECT * FROM meets_criteria_false_records
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_TX,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset="us_tx_normalized_state",
    raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TX, instance=DirectIngestInstance.PRIMARY
    ),
    reasons_fields=[
        ReasonsField(
            name="event_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of the event that triggered an assessment to come due.",
        ),
        ReasonsField(
            name="event_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Type of the event that triggered an assessment to come due.",
        ),
        ReasonsField(
            name="case_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Supervision case type of the client.",
        ),
        ReasonsField(
            name="last_assessment_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Which TRAS was used in the last assessment.",
        ),
        ReasonsField(
            name="last_assessment_level",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Resulting level of the last assessment.",
        ),
        ReasonsField(
            name="due_assessment_months",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Months until the next assessment is due based on event type, assessment type, and assessment level.",
        ),
        ReasonsField(
            name="due_assessment_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Due date for the next assessment based on last event date and months until next assessment.",
        ),
        ReasonsField(
            name="due_assessment_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Type of the next due assessment based on event type, assessment type, and assessment level.",
        ),
        ReasonsField(
            name="next_event_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of the next event that would trigger a new assessment to be completed.",
        ),
        ReasonsField(
            name="next_event_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Type of the next event that would trigger a new assessment to be completed.",
        ),
        ReasonsField(
            name="next_assessment_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="If the next event was a completed assessment, the type of that assessment.",
        ),
        ReasonsField(
            name="frequency",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="The amount of time after the triggering event that the next assessment should be completed.",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
