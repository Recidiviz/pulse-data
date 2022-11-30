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
"""View logic to prepare US_ID Workflows supervision clients."""
from recidiviz.calculator.query.bq_utils import array_concat_with_null
from recidiviz.calculator.query.state.views.workflows.firestore.client_record_ctes import (
    client_record_supervision_cte,
    client_record_supervision_level_cte,
    client_record_supervision_super_sessions_cte,
)

# This template returns a CTEs to be used in the `client_record.py` firestore ETL query
from recidiviz.calculator.query.state.views.workflows.us_id.shared_ctes import (
    us_id_latest_phone_number,
)

US_ID_SUPERVISION_CLIENTS_QUERY_TEMPLATE = f"""
    {client_record_supervision_cte("US_ID")}
    {client_record_supervision_level_cte("US_ID")}
    {client_record_supervision_super_sessions_cte("US_ID")}
    us_id_phone_numbers AS (
        # TODO(#14676): Pull from state_person.phone_number once hydrated
        {us_id_latest_phone_number()}
    ),
    join_id_clients AS (
        SELECT DISTINCT
          sc.person_id,
          sc.person_external_id,
          sp.full_name as person_name,
          sp.current_address as address,
          CAST(ph.phonenumber AS INT64) AS phone_number,
          sc.supervision_type,
          sc.officer_id,
          sc.district,
          sl.supervision_level,
          sl.supervision_level_start,
          ss.start_date AS supervision_start_date,
          FIRST_VALUE(sc.expiration_date IGNORE NULLS) OVER (
            PARTITION BY sc.person_id
            ORDER BY sc.expiration_date DESC
          ) AS expiration_date,
        FROM us_id_supervision_cases sc 
        INNER JOIN us_id_supervision_level_start sl USING(person_id)
        INNER JOIN us_id_supervision_super_sessions ss USING(person_id)
        INNER JOIN `{{project_id}}.{{state_dataset}}.state_person` sp USING(person_id)
        LEFT JOIN us_id_phone_numbers ph USING(person_external_id)
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_level_dedup_priority` sl_dedup
            ON sl.supervision_level=sl_dedup.correctional_level
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_2_dedup_priority` cl2_dedup
            ON "SUPERVISION" = cl2_dedup.compartment_level_1
            AND sc.supervision_type=cl2_dedup.compartment_level_2
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id
            ORDER BY COALESCE(sl_dedup.correctional_level_priority, 999),
            COALESCE(cl2_dedup.priority, 999),
            sc.officer_id
        ) = 1
    ),
    id_lsu_eligibility AS (
        SELECT
            external_id AS person_external_id,
            ["LSU"] AS eligible_opportunities,
        FROM `{{project_id}}.{{workflows_dataset}}.us_id_complete_transfer_to_limited_supervision_form_record_materialized`
    ),
    id_earned_discharge_eligibility AS (
        SELECT
            external_id AS person_external_id,
            ["earnedDischarge"] AS eligible_opportunities,
        FROM `{{project_id}}.{{workflows_dataset}}.us_id_complete_discharge_early_from_supervision_request_record_materialized`
    ),
    id_past_FTRD_eligibility AS (
        SELECT
            external_id AS person_external_id,
            ["pastFTRD"] AS eligible_opportunities,
        FROM `{{project_id}}.{{workflows_dataset}}.us_id_complete_full_term_discharge_from_supervision_request_record_materialized`
    ),
    id_clients AS (
        # Values set to NULL are not applicable for this state
        SELECT
            person_external_id,
            "US_ID" AS state_code,
            person_name,
            officer_id,
            supervision_type,
            supervision_level,
            supervision_level_start,
            address,
            phone_number,
            supervision_start_date,
            expiration_date,
            NULL AS current_balance,
            NULL AS last_payment_amount,
            CAST(NULL AS DATE) AS last_payment_date,
            CAST(NULL AS ARRAY<string>) AS special_conditions,
            CAST(NULL AS ARRAY<STRUCT<condition STRING, condition_description STRING>>) AS board_conditions,
            district,
            {array_concat_with_null([
                "id_earned_discharge_eligibility.eligible_opportunities",
                "id_lsu_eligibility.eligible_opportunities",
                "id_past_FTRD_eligibility.eligible_opportunities"
            ])} AS all_eligible_opportunities,
        FROM join_id_clients
        LEFT JOIN id_earned_discharge_eligibility USING(person_external_id)
        LEFT JOIN id_lsu_eligibility USING (person_external_id)
        LEFT JOIN id_past_FTRD_eligibility USING (person_external_id)
        # TODO(#15809): Remove this condition after we figure out why clients are missing person details
        WHERE person_name IS NOT NULL
    )
"""
