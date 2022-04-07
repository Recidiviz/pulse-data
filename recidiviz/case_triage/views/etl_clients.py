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
"""Creates the view builder and view for listing all clients.

To output this query, run:
  python -m recidiviz.case_triage.views.etl_clients
"""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.case_triage.views.dataset_config import CASE_TRIAGE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_LIST_QUERY_TEMPLATE = """
WITH supervision_start_dates_from_sessions AS (
  SELECT
    person_id,
    state_code,
    compartment_level_2 AS supervision_type,
    start_date AS sessions_start_date
  FROM
    `{project_id}.{analyst_views_dataset}.compartment_sessions_materialized`
  WHERE
    compartment_level_1 = 'SUPERVISION'
    AND end_date IS NULL
),
days_with_po AS (
  SELECT
    person_id,
    state_code,
    DATE_DIFF(CURRENT_DATE, start_date, DAY) AS days_with_current_po
  FROM
    `{project_id}.{analyst_views_dataset}.supervision_officer_sessions_materialized`
  WHERE
    end_date IS NULL
),
days_on_supervision_level AS (
  SELECT
    person_id,
    state_code,
    DATE_DIFF(CURRENT_DATE, start_date, DAY) AS days_on_current_supervision_level
  FROM
    `{project_id}.{analyst_views_dataset}.supervision_level_sessions_materialized`
  WHERE
    end_date IS NULL
),
latest_assessments AS (
  SELECT
    person_id,
    state_code,
    assessment_date AS most_recent_assessment_date,
    assessment_score
  FROM
    `{project_id}.{analyst_views_dataset}.assessment_score_sessions_materialized`
  WHERE
    score_end_date IS NULL
),
latest_contacts AS (
  -- TODO(#8603): Once this table stops having duplicates, we can likely remove these MAXes.
  SELECT
    person_id,
    state_code,
    MAX(next_recommended_assessment_date) AS next_recommended_assessment_date,
    MAX(most_recent_face_to_face_date) AS most_recent_face_to_face_date,
    MAX(next_recommended_face_to_face_date) AS next_recommended_face_to_face_date,
    MAX(most_recent_home_visit_date) AS most_recent_home_visit_date,
    MAX(next_recommended_home_visit_date) AS next_recommended_home_visit_date,
    MAX(most_recent_treatment_collateral_contact_date) AS most_recent_treatment_collateral_contact_date,
    MAX(next_recommended_treatment_collateral_contact_date) AS next_recommended_treatment_collateral_contact_date
  FROM
    `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_case_compliance_metrics_materialized`
  WHERE
    person_external_id IS NOT NULL
    AND date_of_evaluation = CURRENT_DATE
  GROUP BY person_id, state_code
),
latest_employment AS (
    SELECT * EXCEPT (row_num) FROM (
        SELECT 
            employment_periods.state_code,
            employment_periods.person_external_id,
            employment_periods.employer,
            recorded_start_date AS employment_start_date,
            ROW_NUMBER() over (PARTITION BY person_external_id ORDER BY recorded_start_date DESC) as row_num
         FROM
            `{project_id}.{case_triage_dataset}.employment_periods_materialized` employment_periods
        WHERE
          NOT is_unemployed
          AND (recorded_end_date IS NULL OR recorded_end_date > CURRENT_DATE('US/Eastern'))
    ) employments
    WHERE row_num = 1
),
latest_periods AS (
  SELECT
    person_id,
    state_code,
    start_date
  FROM
    `{project_id}.{base_dataset}.state_supervision_period`
  WHERE
    termination_date IS NULL
    AND admission_reason != 'ABSCONSION'
),
most_recent_violations AS (
  SELECT 
    person_id,
    state_code,
    MAX(response_date) as most_recent_violation_date
  FROM 
    `{project_id}.{materialized_metrics_dataset}.most_recent_violation_with_response_metrics_materialized`
  GROUP BY 
    person_id, 
    state_code
),
supervision_start_dates AS (
  SELECT
    *,
    IF (sessions_start_date IS NOT NULL, sessions_start_date, start_date) AS supervision_start_date,
  FROM
    `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_supervision_population_metrics_materialized`
  LEFT JOIN
    `{project_id}.{base_dataset}.state_person`
  USING (person_id, gender, state_code)
  INNER JOIN
    latest_periods
  USING (person_id, state_code)
  -- TODO(#5463): When we ingest employment info, we should replace this joined table with the correct table.
  LEFT JOIN
    latest_employment
  USING (person_external_id, state_code)
  LEFT JOIN
    `{project_id}.{case_triage_dataset}.client_contact_info_materialized`
  USING (person_external_id, state_code)
  LEFT JOIN
    `{project_id}.{case_triage_dataset}.last_known_date_of_employment_materialized`
  USING (person_external_id, state_code)
  LEFT JOIN
    latest_assessments
  USING (person_id, state_code)
  LEFT JOIN
    latest_contacts
  USING (person_id, state_code)
  LEFT JOIN
    supervision_start_dates_from_sessions
  USING (person_id, state_code, supervision_type)
),
export_time AS (
  SELECT CURRENT_TIMESTAMP AS exported_at
)
SELECT
    {columns}
FROM
  supervision_start_dates
LEFT JOIN
  days_with_po
USING (person_id, state_code)
LEFT JOIN
  days_on_supervision_level
USING (person_id, state_code)
LEFT JOIN
  most_recent_violations
USING (person_id, state_code)
FULL OUTER JOIN
  export_time
ON TRUE
WHERE
  supervision_level IS NOT NULL
  AND supervising_officer_external_id IS NOT NULL;
"""

CLIENT_LIST_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=CASE_TRIAGE_DATASET,
    view_id="etl_clients",
    view_query_template=CLIENT_LIST_QUERY_TEMPLATE,
    analyst_views_dataset=SESSIONS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    case_triage_dataset=CASE_TRIAGE_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    columns=[
        "state_code",
        "supervising_officer_external_id",
        "person_external_id",
        "full_name",
        "email_address",
        "phone_number",
        "current_address",
        "gender",
        "birthdate",
        "supervision_start_date",
        "projected_end_date",
        "supervision_type",
        "case_type",
        "supervision_level",
        "employer",
        "employment_start_date",
        "last_known_date_of_employment",
        "most_recent_assessment_date",
        "next_recommended_assessment_date",
        "assessment_score",
        "most_recent_face_to_face_date",
        "next_recommended_face_to_face_date",
        "most_recent_home_visit_date",
        "next_recommended_home_visit_date",
        "most_recent_treatment_collateral_contact_date",
        "next_recommended_treatment_collateral_contact_date",
        "days_with_current_po",
        "days_on_current_supervision_level",
        "most_recent_violation_date",
        "exported_at",
    ],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_LIST_VIEW_BUILDER.build_and_print()
