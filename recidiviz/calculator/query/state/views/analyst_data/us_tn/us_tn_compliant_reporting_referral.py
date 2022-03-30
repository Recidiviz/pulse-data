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
"""Creates a view to help autofill compliant reporting referral form"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_NAME = "us_tn_compliant_reporting_referral"

US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_DESCRIPTION = (
    """Creates a view to help autofill compliant reporting referral form"""
)

US_TN_COMPLIANT_REPORTING_REFERRAL_QUERY_TEMPLATE = """
    -- Keep all active sentences
    -- Bring in information about restitution 
    WITH sentences AS (
        SELECT 
            pp.person_id,
            pp.offender_id,
            COALESCE(CAST(vic.TotalRestitution AS FLOAT64),0) AS TotalRestitution,
            COALESCE(CAST(vic.MonthlyRestitution AS FLOAT64 ),0) AS MonthlyRestitution,
            vic.VictimName,
            FROM `{project_id}.sessions.us_tn_sentences_preprocessed_materialized` pp
            LEFT JOIN `{project_id}.us_tn_raw_data_up_to_date_views.JOVictim_latest` vic
                ON CAST(pp.external_id AS STRING) = CONCAT(vic.OffenderID, vic.ConvictionCounty, vic.CaseYear, vic.CaseNumber, FORMAT('%03d', CAST(vic.CountNumber AS INT64)))
            WHERE pp.sentence_status !='IN'
        ),
        aggregated_restitution AS (
            SELECT  person_id, 
                    offender_id, 
                    sum(TotalRestitution) AS total_restitution, 
                    sum(MonthlyRestitution) AS monthly_restitution, 
                    TO_JSON_STRING(ARRAY_AGG(DISTINCT VictimName IGNORE NULLS)) AS restitution_monthly_payment_to,
            FROM sentences
            GROUP BY 1,2
        ),
        -- Pull latest contact data
        contact_note_type AS (
                SELECT * EXCEPT(ContactNoteDateTime),
                    CAST(CAST(ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
                FROM `{project_id}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest`
        ),
        court_cost_contacts AS (
            -- Get latest contact related to court costs. Here's where looking for "fee status" and there appear to be 2 codes that give this status:
            -- CCFM (COURT COSTS/FINES MONITORING) and CCFT (COURT COSTS/FINES TERMINATED). There are other codes that mention fees (e.g. 
            --  ONFN - RECEIVED FEE PYMT BY MAIL, NO COMMENT) which are aren't capturing here.
            SELECT *
            FROM contact_note_type
            WHERE ContactNoteType LIKE 'CCF%'
            QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY contact_date DESC) = 1

        ),
        supervision_fee_contacts AS (
            -- Get latest contact related to supervision fees status
            SELECT *
            FROM contact_note_type
            WHERE ContactNoteType LIKE 'FEE%'
            QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY contact_date DESC) = 1
        )
        SELECT  compliant_reporting_eligible,
                staff_first_name AS po_first_name,
                staff_last_name AS po_last_name,
                first_name AS client_first_name,
                last_name AS client_last_name,
                current_date('US/Eastern') AS date_today,
                person_external_id AS tdoc_id,
                address AS physical_address,
                phone_number,
                null AS current_employer,
                DriverLicenseNumber AS drivers_license,
                null AS drivers_license_suspended,
                null AS drivers_license_revoked,
                conviction_county,
                'Circuit Court' AS court_name,
                TO_JSON_STRING(docket_numbers) AS all_dockets,
                TO_JSON_STRING(current_offenses) AS current_offenses,
                supervision_type,
                sentence_start_date,
                sentence_length_days,
                expiration_date,
                current_balance AS supervision_fee_assessed,
                CASE WHEN current_balance > 0 THEN 1 ELSE 0 END AS supervision_fee_arrearaged,
                current_balance AS supervision_fee_arrearaged_amount,
                TO_JSON_STRING(current_exemption_type) AS supervision_fee_exemption_type,
                null AS supervision_fee_exemption_expir_date,
                exemption_notes AS supervision_fee_waived,
                CASE WHEN court_cost_contacts.ContactNoteType = 'CCFT' THEN 1 
                     WHEN court_cost_contacts.ContactNoteType = 'CCFM' THEN 0
                     END AS court_costs_paid,
                null AS court_costs_balance,
                null AS court_costs_monthly_amt_1,
                null AS court_costs_monthly_amt_2,
                total_restitution AS restitution_amt,
                monthly_restitution AS restitution_monthly_payment,
                restitution_monthly_payment_to,
                null AS special_conditions_alc_drug_screen,
                last_drug_screen_date AS special_conditions_alc_drug_screen_date,
                null AS special_conditions_alc_drug_assessment,
                null AS special_conditions_alc_drug_assessment_complete,
                null AS special_conditions_alc_drug_assessment_complete_date,
                null AS special_conditions_alc_drug_treatment,
                null AS special_conditions_alc_drug_treatment_in_out,
                null AS special_conditions_alc_drug_treatment_current,
                null AS special_conditions_alc_drug_treatment_complete_date,
                null AS special_conditions_counseling,
                null AS special_conditions_counseling_anger_management,
                null AS special_conditions_counseling_anger_management_current,
                null AS special_conditions_counseling_anger_management_complete_date,
                null AS special_conditions_counseling_mental_health,
                null AS special_conditions_counseling_mental_health_current,
                null AS special_conditions_counseling_mental_health_complete_date,
                null AS special_conditions_community_service,
                null AS special_conditions_community_service_hours,
                null AS special_conditions_community_service_current,
                null AS special_conditions_community_service_completion_date,
                null AS special_conditions_programming,
                null AS special_conditions_programming_cognitive_behavior,
                null AS special_conditions_programming_cognitive_behavior_current,
                null AS special_conditions_programming_cognitive_behavior_completion_date,
                null AS special_conditions_programming_safe,
                null AS special_conditions_programming_safe_current,
                null AS special_conditions_programming_safe_completion_date,
                null AS special_conditions_programming_victim_impact,
                null AS special_conditions_programming_victim_impact_current,
                null AS special_conditions_programming_victim_impact_completion_date,
                null AS special_conditions_programming_fsw,
                null AS special_conditions_programming_fsw_current,
                null AS special_conditions_programming_fsw_completion_date,
        FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_logic_materialized` cr
        LEFT JOIN aggregated_restitution
            USING(person_id)
        LEFT JOIN `{project_id}.us_tn_raw_data_up_to_date_views.OffenderAttributes_latest` att
            ON cr.person_external_id = att.OffenderID
        LEFT JOIN (
            SELECT *
            FROM `{project_id}.us_tn_raw_data_up_to_date_views.ReleasePlan_latest` 
            WHERE TRUE
            QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY LastUpdateDate DESC) = 1
        ) rp
            ON cr.person_external_id = rp.OffenderID
        LEFT JOIN court_cost_contacts 
            ON cr.person_external_id = court_cost_contacts.OffenderID
            AND court_cost_contacts.contact_date >= sentence_start_date
        LEFT JOIN supervision_fee_contacts 
            ON cr.person_external_id = supervision_fee_contacts.OffenderID
            AND supervision_fee_contacts.contact_date >= sentence_start_date
"""

US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_REFERRAL_QUERY_TEMPLATE,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_BUILDER.build_and_print()
