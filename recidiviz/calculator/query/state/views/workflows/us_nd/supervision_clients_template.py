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
"""View logic to prepare US_ND Workflows supervision clients."""
from recidiviz.calculator.query.bq_utils import (
    array_concat_with_null,
    nonnull_end_date_exclusive_clause,
)

# This template returns a CTEs to be used in the `client_record.py` firestore ETL query
US_ND_SUPERVISION_CLIENTS_QUERY_TEMPLATE = f"""
    nd_supervision_cases AS (
        # This CTE returns a row for each person and supervision sentence, so will return multiple rows if someone
        # is on dual supervision (parole and probation)
        SELECT
          dataflow.person_id,
          person_external_id,
          compartment_level_2 AS supervision_type,
          supervising_officer_external_id AS officer_id,
          level_2_supervision_location_external_id AS district,
          projected_end.projected_completion_date_max AS expiration_date
        FROM `{{project_id}}.{{dataflow_dataset}}.most_recent_supervision_population_span_metrics_materialized` dataflow
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
          ON dataflow.state_code = sessions.state_code
          AND dataflow.person_id = sessions.person_id
          AND sessions.compartment_level_1 = "SUPERVISION"
          AND sessions.end_date IS NULL
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_projected_completion_date_spans_materialized` projected_end
            ON dataflow.state_code = projected_end.state_code
            AND dataflow.person_id = projected_end.person_id
            AND CURRENT_DATE('US/Eastern')
                BETWEEN projected_end.start_date
                    AND {nonnull_end_date_exclusive_clause('projected_end.end_date')}
        WHERE dataflow.state_code = 'US_ND' AND dataflow.included_in_state_population
          AND dataflow.end_date_exclusive IS NULL
          AND supervising_officer_external_id IS NOT NULL
    ),
    nd_supervision_level_start AS (
    # This CTE selects the most recent supervision level for each person with an active supervision period,
    # prioritizing the highest level in cases where one person is currently assigned to multiple levels
        SELECT
          person_id,
          most_recent_active_supervision_level AS supervision_level,
          start_date as supervision_level_start,  
        FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_sessions_materialized`
        WHERE state_code = "US_ND"
        AND end_date IS NULL
    ),
    nd_supervision_super_sessions AS (
      # This CTE has 1 row per person with an active supervision period and the start_date corresponds to 
      # the earliest start date for dual supervision periods.
      SELECT
        person_id,
        start_date
      FROM `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`
      WHERE state_code = "US_ND"
      AND end_date IS NULL
    ),
    nd_phone_numbers AS (
        # TODO(#14676): Pull from state_person.phone_number once hydrated
        SELECT 
            state_code, 
            external_id AS person_external_id, 
            PHONE AS phone_number
        FROM `{{project_id}}.{{us_nd_raw_data}}.docstars_offenders_latest` doc
        INNER JOIN `{{project_id}}.{{state_dataset}}.state_person_external_id` pei
        ON doc.SID = pei.external_id
        AND pei.id_type = "US_ND_SID"
    ),
    join_nd_clients AS (
        SELECT DISTINCT
          sc.person_id,
          sc.person_external_id,
          sp.full_name as person_name,
          sp.current_address as address,
          CAST(ph.phone_number AS INT64) AS phone_number,
          supervision_type,
          sc.officer_id,
          sc.district,
          sl.supervision_level,
          sl.supervision_level_start,
          ss.start_date AS supervision_start_date,
          FIRST_VALUE(sc.expiration_date IGNORE NULLS) OVER (
            PARTITION BY sc.person_id
            ORDER BY sc.expiration_date DESC
          ) AS expiration_date,
        FROM nd_supervision_cases sc 
        INNER JOIN nd_supervision_level_start sl USING(person_id)
        INNER JOIN nd_supervision_super_sessions ss USING(person_id)
        INNER JOIN `{{project_id}}.{{state_dataset}}.state_person` sp USING(person_id)
        LEFT JOIN nd_phone_numbers ph USING(person_external_id)
    ),
    nd_eligibility AS (
        SELECT
            external_id AS person_external_id,
            ["earlyTermination"] AS eligible_opportunities,
        FROM `{{project_id}}.{{workflows_dataset}}.us_nd_complete_discharge_early_from_supervision_record_materialized`
    ),
    nd_clients AS (
        # Values set to NULL are not applicable for this state
        SELECT 
            person_external_id,
            "US_ND" AS state_code,
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
            {array_concat_with_null(["nd_eligibility.eligible_opportunities"])} AS all_eligible_opportunities,
        FROM join_nd_clients
        LEFT JOIN nd_eligibility USING(person_external_id)
    )
"""
