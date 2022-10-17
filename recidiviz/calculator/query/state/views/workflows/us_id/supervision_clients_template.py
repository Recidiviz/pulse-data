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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause

# This template returns a CTEs to be used in the `client_record.py` firestore ETL query
from recidiviz.calculator.query.state.views.workflows.us_id.shared_ctes import (
    us_id_latest_phone_number,
)

US_ID_SUPERVISION_CLIENTS_QUERY_TEMPLATE = f"""
    us_id_supervision_cases AS (
        # This CTE returns a row for each person and supervision sentence, so will return multiple rows if someone
        # is on dual supervision (parole and probation)
        SELECT
          dataflow.person_id,
          person_external_id,
          dataflow.supervision_type,
          supervising_officer_external_id AS officer_id,
          projected_end.projected_completion_date_max AS expiration_date
        FROM `{{project_id}}.{{dataflow_dataset}}.most_recent_single_day_supervision_population_metrics_materialized` dataflow
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
          ON dataflow.state_code = sessions.state_code
          AND dataflow.person_id = sessions.person_id
          AND sessions.compartment_level_1 = "SUPERVISION"
          AND sessions.end_date IS NULL
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` projected_end
            ON dataflow.state_code = projected_end.state_code
            AND dataflow.person_id = projected_end.person_id
            AND dataflow.date_of_supervision
                BETWEEN projected_end.start_date
                    AND {nonnull_end_date_exclusive_clause('projected_end.end_date')}
        WHERE dataflow.state_code = 'US_ID'
        AND supervising_officer_external_id IS NOT NULL
    ),
    us_id_supervision_level_start AS (
    # This CTE selects the most recent supervision level for each person with an active supervision period,
    # prioritizing the highest level in cases where one person is currently assigned to multiple levels
        SELECT
          sl.person_id,
          sl.start_date as supervision_level_start,  
          CASE 
              -- US_ID expressed preference for the raw text for DIVERSION cases
              WHEN sl.most_recent_active_supervision_level = 'DIVERSION'
              THEN session_attributes.correctional_level_raw_text
              ELSE sl.most_recent_active_supervision_level 
          END AS supervision_level,
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized` sl
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized` dataflow
              ON dataflow.person_id = sl.person_id
              AND dataflow.dataflow_session_id = sl.dataflow_session_id_start,
              UNNEST(session_attributes) as session_attributes
        WHERE sl.state_code = "US_ID"
        AND sl.end_date IS NULL
    ),
    us_id_supervision_super_sessions AS (
      # This CTE has 1 row per person with an active supervision period and the start_date corresponds to 
      # the earliest start date for dual supervision periods.
      SELECT
        person_id,
        start_date
      FROM `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`
      WHERE state_code = "US_ID"
      AND end_date IS NULL
    ),
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
            TRUE AS limited_supervision_eligible,
        FROM `{{project_id}}.{{workflows_dataset}}.us_id_complete_transfer_to_limited_supervision_form_record_materialized`
    ),
    id_earned_discharge_eligibility AS (
        SELECT
            external_id AS person_external_id,
            TRUE AS earned_discharge_eligible,
        FROM `{{project_id}}.{{workflows_dataset}}.us_id_complete_discharge_early_from_supervision_request_record_materialized`
    ),
    id_past_FTRD_eligibility AS (
        SELECT
            external_id AS person_external_id,
            TRUE AS past_FTRD_eligible,
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
            CAST(NULL AS STRING) AS district,
            CAST(NULL AS STRING) AS compliant_reporting_eligible,
            FALSE AS early_termination_eligible,
            IFNULL(earned_discharge_eligible, FALSE) AS earned_discharge_eligible,
            IFNULL(limited_supervision_eligible, FALSE) AS limited_supervision_eligible,
            IFNULL(past_FTRD_eligible, FALSE) AS past_FTRD_eligible,
            FALSE AS supervision_level_downgrade_eligible,
        FROM join_id_clients
        LEFT JOIN id_earned_discharge_eligibility USING(person_external_id)
        LEFT JOIN id_lsu_eligibility USING (person_external_id)
        LEFT JOIN id_past_FTRD_eligibility USING (person_external_id)
        # TODO(#15809): Remove this condition after we figure out why clients are missing person details
        WHERE person_name IS NOT NULL
    )
"""
