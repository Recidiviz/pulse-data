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
"""View to prepare client reporting referral records for export to the frontend."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPLIANT_REPORTING_REFERRAL_RECORD_VIEW_NAME = "compliant_reporting_referral_record"

COMPLIANT_REPORTING_REFERRAL_RECORD_DESCRIPTION = """
    Compliant reporting referral records to be exported to Firestore to power the Compliant Reporting dashboard in TN.
    """

COMPLIANT_REPORTING_REFERRAL_RECORD_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        "US_TN" AS state_code,
        po_first_name,
        po_last_name,
        client_first_name,
        client_last_name,
        date_today,
        tdoc_id,
        compliant_reporting_eligible,
        physical_address,
        current_employer,
        drivers_license,
        drivers_license_suspended,
        drivers_license_revoked,
        conviction_county,
        court_name,
        all_dockets,
        current_offenses,
        supervision_type,
        sentence_start_date,
        sentence_length_days,
        expiration_date,
        supervision_fee_assessed,
        IFNULL(supervision_fee_arrearaged, 0)=1 AS supervision_fee_arrearaged,
        supervision_fee_arrearaged_amount,
        supervision_fee_exemption_type,
        supervision_fee_exemption_expir_date,
        supervision_fee_waived,
        IFNULL(court_costs_paid, 0)=1 AS court_costs_paid,
        court_costs_balance,
        court_costs_monthly_amt_1,
        court_costs_monthly_amt_2,
        restitution_amt,
        restitution_monthly_payment,
        restitution_monthly_payment_to,
        IFNULL(special_conditions_alc_drug_screen, 0)=1 AS special_conditions_alc_drug_screen,
        special_conditions_alc_drug_screen_date,
        special_conditions_alc_drug_assessment,
        IFNULL(special_conditions_alc_drug_assessment_complete, 0)=1 AS special_conditions_alc_drug_assessment_complete,
        special_conditions_alc_drug_assessment_complete_date,
        IFNULL(special_conditions_alc_drug_treatment, 0)=1 AS special_conditions_alc_drug_treatment,
        special_conditions_alc_drug_treatment_in_out,
        IFNULL(special_conditions_alc_drug_treatment_current, 0)=1 AS special_conditions_alc_drug_treatment_current,
        special_conditions_alc_drug_treatment_complete_date,
        IFNULL(special_conditions_counseling, 0)=1 AS special_conditions_counseling,
        special_conditions_counseling_anger_management,
        IFNULL(special_conditions_counseling_anger_management_current, 0)=1 AS special_conditions_counseling_anger_management_current,
        special_conditions_counseling_anger_management_complete_date,
        special_conditions_counseling_mental_health,
        special_conditions_counseling_mental_health_current,
        special_conditions_counseling_mental_health_complete_date,
        IFNULL(special_conditions_community_service, 0)=1 AS special_conditions_community_service,
        special_conditions_community_service_hours,
        IFNULL(special_conditions_community_service_current, 0)=1 AS special_conditions_community_service_current,
        special_conditions_community_service_completion_date,
        IFNULL(special_conditions_programming, 0)=1 AS special_conditions_programming,
        IFNULL(special_conditions_programming_cognitive_behavior, 0)=1 AS special_conditions_programming_cognitive_behavior,
        IFNULL(special_conditions_programming_cognitive_behavior_current, 0)=1 AS special_conditions_programming_cognitive_behavior_current,
        special_conditions_programming_cognitive_behavior_completion_date,
        IFNULL(special_conditions_programming_safe, 0)=1 AS special_conditions_programming_safe,
        IFNULL(special_conditions_programming_safe_current, 0)=1 AS special_conditions_programming_safe_current,
        special_conditions_programming_safe_completion_date,
        IFNULL(special_conditions_programming_victim_impact, 0)=1 AS special_conditions_programming_victim_impact,
        IFNULL(special_conditions_programming_victim_impact_current, 0)=1 AS special_conditions_programming_victim_impact_current,
        special_conditions_programming_victim_impact_completion_date,
        IFNULL(special_conditions_programming_fsw, 0)=1 AS special_conditions_programming_fsw,
        IFNULL(special_conditions_programming_fsw_current, 0)=1 AS special_conditions_programming_fsw_current,
        special_conditions_programming_fsw_completion_date
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_referral_materialized`
"""

COMPLIANT_REPORTING_REFERRAL_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=COMPLIANT_REPORTING_REFERRAL_RECORD_VIEW_NAME,
    view_query_template=COMPLIANT_REPORTING_REFERRAL_RECORD_QUERY_TEMPLATE,
    description=COMPLIANT_REPORTING_REFERRAL_RECORD_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPLIANT_REPORTING_REFERRAL_RECORD_VIEW_BUILDER.build_and_print()
