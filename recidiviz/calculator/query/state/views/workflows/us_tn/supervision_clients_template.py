#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View logic to prepare US_TN Workflows supervision clients."""
from recidiviz.calculator.query.bq_utils import columns_to_array

# This template returns a CTEs to be used in the `client_record.py` firestore ETL query
US_TN_SUPERVISION_CLIENTS_QUERY_TEMPLATE = f"""
    tn_supervision_level_downgrade_eligibility AS (
        SELECT 
            external_id AS person_external_id,
            "supervisionLevelDowngrade" AS opportunity_name,
        FROM `{{project_id}}.{{workflows_dataset}}.us_tn_supervision_level_downgrade_record_materialized`
    ),
    tn_expiration_eligibility AS (
        SELECT 
            external_id AS person_external_id,
            "usTnExpiration" AS opportunity_name,
            CAST(JSON_EXTRACT_SCALAR(single_reason.reason.eligible_date) AS DATE) AS expiration_date,
        FROM `{{project_id}}.{{workflows_dataset}}.us_tn_full_term_supervision_discharge_record_materialized` tes,
        UNNEST(JSON_QUERY_ARRAY(reasons)) AS single_reason
        WHERE STRING(single_reason.criteria_name) = 'SUPERVISION_PAST_FULL_TERM_COMPLETION_DATE_OR_UPCOMING_1_DAY'        
    ),
    tn_compliant_reporting_eligibility AS (
        SELECT
            *,
            CASE WHEN cr.compliant_reporting_eligible IS NOT NULL THEN "compliantReporting" ELSE null END AS opportunity_name,
        FROM `{{project_id}}.{{analyst_views_dataset}}.us_tn_compliant_reporting_logic_materialized` cr
    ),
    tn_clients AS (
        # Values set to NULL are not applicable for this state
        SELECT
            person_external_id,
            "US_TN" AS state_code,
            person_name,
            officer_id,
            supervision_type,
            supervision_level,
            supervision_level_start,
            address,
            phone_number,
            CAST(NULL AS STRING) AS email_address,
            earliest_supervision_start_date_in_latest_system AS supervision_start_date,
            COALESCE(tn_expiration_eligibility.expiration_date, 
                     tn_compliant_reporting_eligibility.expiration_date) AS expiration_date,
            current_balance,
            last_payment_amount,
            last_payment_date,
            special_conditions_on_current_sentences AS special_conditions,
            board_conditions,
            district,
            CAST(NULL AS ARRAY<STRING>) AS current_employer,
            CAST(NULL AS ARRAY<STRING>) AS current_employer_address,
            {columns_to_array(["tn_supervision_level_downgrade_eligibility.opportunity_name",
                               "tn_compliant_reporting_eligibility.opportunity_name",
                               "tn_expiration_eligibility.opportunity_name"])} AS all_eligible_opportunities,
            CAST(NULL AS ARRAY<STRUCT<type STRING, text STRING>>) as milestones,
        FROM tn_compliant_reporting_eligibility
        LEFT JOIN tn_supervision_level_downgrade_eligibility USING (person_external_id)
        FULL OUTER JOIN tn_expiration_eligibility USING (person_external_id)
    )
"""
