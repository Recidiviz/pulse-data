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
"""Implements BQ View to fetch information on new top opportunities.

To start, this view only identifies people who are overdue for supervision downgrades.

TODO(#6615): This view is currently specific to Idaho, and it should be evolved
"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

TOP_OPPORTUNITIES_QUERY_VIEW = f"""
WITH overdue_downgrades AS (
  SELECT
    state_code,
    clients.supervising_officer_external_id,
    person_external_id,
    '{OpportunityType.OVERDUE_DOWNGRADE.value}' AS opportunity_type,
    TO_JSON_STRING(STRUCT(
      assessment_score AS assessmentScore,
      clients.most_recent_assessment_date AS latestAssessmentDate,
      recommended_supervision_downgrade_level AS recommendedSupervisionLevel
    )) AS opportunity_metadata
  FROM
    `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_case_compliance_metrics_materialized`
  INNER JOIN
    `{{project_id}}.{{case_triage_dataset}}.etl_clients_materialized` clients
  USING (person_external_id, state_code)
  WHERE
    date_of_supervision = CURRENT_DATE
    AND recommended_supervision_downgrade_level IS NOT NULL
),
earned_discharge_eligible AS (
  SELECT
    state_code,
    supervising_officer_external_id,
    person_external_id,
    '{OpportunityType.EARLY_DISCHARGE.value}' AS opportunity_type,
    TO_JSON_STRING(STRUCT(
        num_open_earned_discharge_requests AS openEarnedDischargeRequests,
        days_served AS daysServed,
        prop_sentence_served AS proportionSentenceServed,
        supervision_level AS currentSupervisionLevel,
        days_since_last_positive_urine_analysis_date AS daysSincePositiveUA,
        positive_urine_analysis_results_past_year_count AS countPositiveUAOneYear,
        is_employed AS hasEmployment,
        last_verified_employment_date AS latestVerifiedEmploymentDate,
        days_employed AS daysSinceEmploymentBegan,
        days_employed_in_session AS daysSinceEmploymentBeganOnSupervision,
        critical_contacts_count AS countCriticalContacts
        )) AS opportunity_metadata
  FROM 
    `{{project_id}}.{{case_triage_dataset}}.client_eligibility_criteria_materialized`
  WHERE 
    state_code = 'US_ID'
    # TODO(#7303): Add treatment completion as an additional blocking criteria
    AND case_type = 'GENERAL'
    AND supervision_type NOT IN ('INTERNAL_UNKNOWN', 'INFORMAL_PROBATION')
    AND days_served >= 365
    AND num_open_earned_discharge_requests = 0
    AND supervision_level IN ('MINIMUM', 'MEDIUM')
    AND critical_contacts_count = 0
),
limited_supervision_eligible AS (
  SELECT
    state_code,
    supervising_officer_external_id,
    person_external_id,
    '{OpportunityType.LIMITED_SUPERVISION_UNIT.value}' AS opportunity_type,
    TO_JSON_STRING(STRUCT(
        days_served AS daysServed,
        prop_sentence_served AS proportionSentenceServed,
        supervision_level AS currentSupervisionLevel,
        days_since_last_positive_urine_analysis_date AS daysSincePositiveUA,
        positive_urine_analysis_results_past_year_count AS countPositiveUAOneYear,
        is_employed AS hasEmployment,
        last_verified_employment_date AS latestVerifiedEmploymentDate,
        days_employed AS daysSinceEmploymentBegan,
        days_employed_in_session AS daysSinceEmploymentBeganOnSupervision,
        critical_contacts_count AS countCriticalContacts
        )) AS opportunity_metadata  
  FROM
    `{{project_id}}.{{case_triage_dataset}}.client_eligibility_criteria_materialized`
  WHERE 
    state_code = 'US_ID'
    # TODO(#7303): Add treatment completion as an additional blocking criteria
    AND case_type = 'GENERAL'
    AND supervision_type NOT IN ('INTERNAL_UNKNOWN', 'INFORMAL_PROBATION')
    AND supervision_level = 'MINIMUM'
    AND days_at_current_supervision_level >= 365 
),
export_time AS (
  SELECT CURRENT_TIMESTAMP AS exported_at
),
unioned_results AS (
  SELECT
    *
  FROM
    overdue_downgrades
  UNION ALL
  SELECT
    *
  FROM
    earned_discharge_eligible
  UNION ALL
  SELECT
    *
  FROM
    limited_supervision_eligible
)
SELECT
  {{columns}}
FROM
  unioned_results
FULL OUTER JOIN
  export_time
ON TRUE
"""


TOP_OPPORTUNITIES_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="etl_opportunities",
    view_query_template=TOP_OPPORTUNITIES_QUERY_VIEW,
    case_triage_dataset=VIEWS_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    columns=[
        "state_code",
        "supervising_officer_external_id",
        "person_external_id",
        "opportunity_type",
        "opportunity_metadata",
        "exported_at",
    ],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        TOP_OPPORTUNITIES_VIEW_BUILDER.build_and_print()
