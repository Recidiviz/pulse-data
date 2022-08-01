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
"""View to prepare client records regarding compliant reporting for export to the frontend."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_RECORD_VIEW_NAME = "client_record"

CLIENT_RECORD_DESCRIPTION = """
    Nested client records to be exported to Firestore to power the Compliant Reporting dashboard in TN.
    """

CLIENT_RECORD_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    tn_clients AS (
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
            expiration_date,
            current_balance,
            last_payment_amount,
            last_payment_date,
            exemption_notes AS fee_exemptions,
            special_conditions_on_current_sentences AS special_conditions,
            SPE_note_due AS next_special_conditions_check,
            last_SPE_note AS last_special_conditions_note,
            last_SPET_date AS special_conditions_terminated_date,
            board_conditions,
            compliant_reporting_eligible,
            remaining_criteria_needed,
            eligible_level_start,
            current_offenses,
            past_offenses,
            lifetime_offenses_expired,
            judicial_district,
            last_DRUN AS drug_screens_past_year,
            sanctions_in_last_year AS sanctions_past_year,
            last_ARR_Note AS most_recent_arrest_check,
            zt_codes AS zero_tolerance_codes,
            fines_fees_eligible,
            earliest_supervision_start_date_in_latest_system,
            district,
            spe_flag AS special_conditions_flag,
            -- these fields should all be disregarded if remaining_criteria_needed is zero, 
            -- as there is additional override logic baked into that field 
            -- (i.e. the fields do not always agree and remaining_criteria_needed wins)
            IF (
                remaining_criteria_needed = 0,
                NULL, 
                eligible_time_on_supervision_level_bool = 1
            ) AS almost_eligible_time_on_supervision_level,
            IF (
                remaining_criteria_needed = 0,
                NULL, 
                date_supervision_level_eligible
            ) AS date_supervision_level_eligible,
            IF (
                remaining_criteria_needed = 0,
                NULL, 
                drug_screen_eligibility_bool = 1
            ) AS almost_eligible_drug_screen,
            IF (
                remaining_criteria_needed = 0,
                NULL, 
                fines_fees_eligible_bool = 1
            ) AS almost_eligible_fines_fees,
            IF (
                remaining_criteria_needed = 0,
                NULL, 
               cr_recent_rejection_eligible_bool = 1
            ) AS almost_eligible_recent_rejection,
            IF (
                remaining_criteria_needed = 0,
                NULL, 
                cr_rejections_past_3_months
            ) AS cr_rejections_past_3_months,
            IF (
                remaining_criteria_needed = 0,
                NULL, 
                eligible_serious_sanctions_bool = 1
            ) AS almost_eligible_serious_sanctions,
            IF (
                remaining_criteria_needed = 0,
                NULL, 
                date_serious_sanction_eligible
            ) AS date_serious_sanction_eligible,
        FROM `{project_id}.{analyst_views_dataset}.us_tn_compliant_reporting_logic_materialized`
    )

    SELECT
        *,
        # truncating hash string
        SUBSTRING(
            # hashing external ID to base64url
            REPLACE(
                REPLACE(
                    TO_BASE64(SHA256(state_code || person_external_id)), 
                    '+', 
                    '-'
                ), 
                '/', 
                '_'
            ), 
            1, 
            16
        ) AS pseudonymized_id,
    FROM tn_clients
"""

CLIENT_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENT_RECORD_VIEW_NAME,
    view_query_template=CLIENT_RECORD_QUERY_TEMPLATE,
    description=CLIENT_RECORD_DESCRIPTION,
    analyst_views_dataset=dataset_config.ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_VIEW_BUILDER.build_and_print()
