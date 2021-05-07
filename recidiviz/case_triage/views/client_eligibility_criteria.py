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
"""Creates a view indicating attributes of the current supervision population that may be relevant to eligibility for decarceral opportunities"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    CASE_TRIAGE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_ELIGIBILITY_CRITERIA_VIEW_NAME = "client_eligibility_criteria"

CLIENT_ELIGIBILITY_CRITERIA_VIEW_DESCRIPTION = """View containing attributes for current clients on supervision that may impact eligibility for decarceral opportunities.
    Currently this view supports the following attributes:
    - `case_type`
    - `supervision_level`
    - `days_served`: Number of days served on current supervision session
    - `prop_sentence_served`: Proportion of projected maximum sentence length served
    - `num_open_earned_discharge_requests`: Number of open (not rejected) earned discharge requests made
    - `days_at_current_supervision_level`: Days since last supervision level upgrade/downgrade
    - `days_since_last_positive_urine_analysis_date`
    - `positive_urine_analysis_results_past_year_count`
    - `is_employed`: CIS employment data contains a current job title/employer that does indicate unemployment
    - `last_verified_employment_date`: Most recent employment verification date available in CIS
    - `days_employed`: Number of days since person began any form of employment
    - `critical_counts_count`: Count of number of emergency contacts in current supervision session
    """

CLIENT_ELIGIBILITY_CRITERIA_QUERY_TEMPLATE = """
    SELECT
      * EXCEPT(rn)
    FROM (
      SELECT
        clients.state_code,
        clients.person_external_id,
        p.person_id,
        supervising_officer_external_id,
        case_type,
        supervision_type,
        DATE_DIFF(CURRENT_DATE(), supervision_start_date, DAY) days_served,
        CASE
          WHEN projected_end_date > supervision_start_date THEN DATE_DIFF(CURRENT_DATE(), supervision_start_date, DAY)/DATE_DIFF(projected_end_date, supervision_start_date, DAY)
      END
        prop_sentence_served,
        COUNTIF(ed.decision != 'REQUEST_DENIED') OVER(PARTITION BY p.person_id) num_open_earned_discharge_requests,
        clients.supervision_level,
        DATE_DIFF(CURRENT_DATE(), levels.start_date, DAY) days_at_current_supervision_level,
        DATE_DIFF(CURRENT_DATE(), MAX(positive_urine_analysis_date) OVER(PARTITION BY p.person_id), DAY) days_since_last_positive_urine_analysis_date,
        COUNTIF(positive_urine_analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)) OVER(PARTITION BY p.person_id) positive_urine_analysis_results_past_year_count,
        is_employed,
        last_verified_date AS last_verified_employment_date,
        /* Using the earliest listed start date for any employment period in the employment session to calculate days employed.*/
        CASE WHEN is_employed THEN DATE_DIFF(CURRENT_DATE(), employment.earliest_employment_period_start_date, DAY) ELSE 0 END days_employed,
        /* Using the earliest start date within a given supervision session to calculate days employed while on supervision.*/
        CASE WHEN is_employed THEN DATE_DIFF(CURRENT_DATE(), employment.employment_status_start_date, DAY) ELSE 0 END days_employed_in_session,
        # TODO(#7305): Adjust critical count to include contacts occurring in JAIL or LAW ENFORCEMENT AGENCY
        COUNT(DISTINCT IF (contact_reason = 'EMERGENCY_CONTACT', contact_date, NULL)) OVER(PARTITION BY p.person_id) critical_contacts_count,
        ROW_NUMBER() OVER(PARTITION BY p.person_id) rn
        # TODO(#7303): Incorporate data about treatment enrollment or requirements as an additional elgibility criteria
      FROM `{project_id}.{case_triage_dataset}.etl_clients` clients
      JOIN `{project_id}.{base_dataset}.state_person_external_id` p
      ON clients.person_external_id = p.external_id
        AND clients.state_code = p.state_code
      JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
      ON p.person_id = sessions.person_id
        AND sessions.end_date IS NULL
        AND sessions.compartment_level_1 = 'SUPERVISION'
      LEFT JOIN `{project_id}.{analyst_dataset}.supervision_level_sessions_materialized` levels
      ON p.person_id = levels.person_id
        AND levels.end_date IS NULL
      LEFT JOIN `{project_id}.{base_dataset}.state_early_discharge` ed
      ON p.person_id = ed.person_id
        AND ed.request_date >= sessions.start_date
        AND decision_status != 'INVALID'
      LEFT JOIN `{project_id}.{base_dataset}.state_supervision_contact` contacts
      ON p.person_id = contacts.person_id
        AND contacts.contact_date >= sessions.start_date
      LEFT JOIN `{project_id}.{analyst_dataset}.us_id_employment_sessions_materialized` employment
      ON p.person_id = employment.person_id
        AND employment.employment_status_end_date IS NULL
      LEFT JOIN `{project_id}.{analyst_dataset}.us_id_positive_urine_analysis_sessions_materialized` ua
      ON p.person_id = ua.person_id )
    WHERE
      rn = 1
      AND state_code = 'US_ID'
"""

CLIENT_ELIGIBILITY_CRITERIA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=CASE_TRIAGE_DATASET,
    view_id=CLIENT_ELIGIBILITY_CRITERIA_VIEW_NAME,
    description=CLIENT_ELIGIBILITY_CRITERIA_VIEW_DESCRIPTION,
    view_query_template=CLIENT_ELIGIBILITY_CRITERIA_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    case_triage_dataset=CASE_TRIAGE_DATASET,
    base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_ELIGIBILITY_CRITERIA_VIEW_BUILDER.build_and_print()
