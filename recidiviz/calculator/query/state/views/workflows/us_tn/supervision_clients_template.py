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
            earliest_supervision_start_date_in_latest_system AS supervision_start_date,
            expiration_date,
            current_balance,
            last_payment_amount,
            last_payment_date,
            special_conditions_on_current_sentences AS special_conditions,
            board_conditions,
            district,
            compliant_reporting_eligible,
            FALSE AS early_termination_eligible,
            FALSE AS earned_discharge_eligible,
            FALSE AS limited_supervision_eligible,
        FROM `{project_id}.{analyst_views_dataset}.us_tn_compliant_reporting_logic_materialized`
    )
"""
