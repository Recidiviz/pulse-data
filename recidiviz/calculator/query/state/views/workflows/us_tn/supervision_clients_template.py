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

# This template returns a CTEs to be used in the `client_record.py` firestore ETL query
US_TN_SUPERVISION_CLIENTS_QUERY_TEMPLATE = """
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
            earliest_supervision_start_date_in_latest_system AS supervision_start_date,
            expiration_date,
            FALSE AS early_termination_eligible,
            current_balance,
            last_payment_amount,
            last_payment_date,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            exemption_notes AS fee_exemptions,
            special_conditions_on_current_sentences AS special_conditions,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            SPE_note_due AS next_special_conditions_check,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            last_SPE_note AS last_special_conditions_note,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            last_SPET_date AS special_conditions_terminated_date,
            board_conditions,
            compliant_reporting_eligible,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            remaining_criteria_needed,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            eligible_level_start,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            current_offenses,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            past_offenses,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            lifetime_offenses_expired,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            judicial_district,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            last_DRUN AS drug_screens_past_year,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            sanctions_in_last_year AS sanctions_past_year,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            last_ARR_Note AS most_recent_arrest_check,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            zt_codes AS zero_tolerance_codes,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            fines_fees_eligible,
            district,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            spe_flag AS special_conditions_flag,
            -- these fields should all be disregarded if remaining_criteria_needed is zero,
            -- as there is additional override logic baked into that field
            -- (i.e. the fields do not always agree and remaining_criteria_needed wins)
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
                eligible_time_on_supervision_level_bool = 1
            ) AS almost_eligible_time_on_supervision_level,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
                date_supervision_level_eligible
            ) AS date_supervision_level_eligible,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
                drug_screen_eligibility_bool = 1
            ) AS almost_eligible_drug_screen,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
                fines_fees_eligible_bool = 1
            ) AS almost_eligible_fines_fees,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
               cr_recent_rejection_eligible_bool = 1
            ) AS almost_eligible_recent_rejection,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
                cr_rejections_past_3_months
            ) AS cr_rejections_past_3_months,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
                eligible_serious_sanctions_bool = 1
            ) AS almost_eligible_serious_sanctions,
            -- TODO(#2276): Remove field once frontend and looker are migrated
            IF (
                remaining_criteria_needed = 0,
                NULL,
                date_serious_sanction_eligible
            ) AS date_serious_sanction_eligible,
        FROM `{project_id}.{analyst_views_dataset}.us_tn_compliant_reporting_logic_materialized`
    )
"""
