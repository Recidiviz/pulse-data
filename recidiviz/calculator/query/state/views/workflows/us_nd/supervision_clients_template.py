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

# This template returns a CTEs to be used in the `client_record.py` firestore ETL query
US_ND_SUPERVISION_CLIENTS_QUERY_TEMPLATE = """
    supervision_types AS (
        # This CTE determines whether the supervision_type should be set to DUAL (parole and probation)
        SELECT
            person_id,
            STRING_AGG(DISTINCT supervision_type, "-" ORDER BY supervision_type) AS supervision_types,
        FROM `{project_id}.{dataflow_dataset}.most_recent_single_day_supervision_population_metrics_materialized`
        WHERE state_code = 'US_ND'
        GROUP BY 1
    ),
    supervision_cases AS (
        # This CTE returns a row for each person and supervision sentence, so will return multiple rows if someone
        # is on dual supervision (parole and probation)
        SELECT
          dataflow.person_id,
          person_external_id,
          compartment_level_2 AS supervision_type,
          supervising_officer_external_id AS officer_id,
          projected_end_date AS expiration_date 
        FROM `{project_id}.{dataflow_dataset}.most_recent_single_day_supervision_population_metrics_materialized` dataflow
        INNER JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
          ON dataflow.state_code = sessions.state_code
          AND dataflow.person_id = sessions.person_id
          AND sessions.compartment_level_1 = "SUPERVISION"
          AND sessions.end_date IS NULL
        WHERE dataflow.state_code = 'US_ND'
        AND supervising_officer_external_id IS NOT NULL
    ),
    supervision_level_start AS (
    # This CTE selects the most recent supervision level for each person with an active supervision period,
    # prioritizing the highest level in cases where one person is currently assigned to multiple levels
        SELECT
          person_id,
          most_recent_active_supervision_level AS supervision_level,
          start_date as supervision_level_start,  
        FROM `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized`
        WHERE state_code = "US_ND"
        AND end_date IS NULL
    ),
    supervision_super_sessions AS (
      # This CTE has 1 row per person with an active supervision period and the start_date corresponds to 
      # the earliest start date for dual supervision periods.
      SELECT
        person_id,
        start_date
      FROM `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized`
      WHERE state_code = "US_ND"
      AND end_date IS NULL
    ),
    phone_numbers AS (
        # TODO(#14676): Pull from state_person.phone_number once hydrated
        SELECT 
            state_code, 
            external_id AS person_external_id, 
            PHONE AS phone_number
        FROM `{project_id}.{us_nd_raw_data}.docstars_offenders_latest` doc
        INNER JOIN `{project_id}.{state_dataset}.state_person_external_id` pei
        ON doc.SID = pei.external_id
        AND pei.id_type = "US_ND_SID"
    ),
    clients AS (
        SELECT DISTINCT
          sc.person_id,
          sc.person_external_id,
          sp.full_name as person_name,
          sp.current_address as address,
          CAST(ph.phone_number AS INT64) AS phone_number,
          supervision_type,
          sc.officer_id,
          sl.supervision_level,
          sl.supervision_level_start,
          ss.start_date AS supervision_start_date,
          sc.expiration_date,
        FROM supervision_cases sc 
        INNER JOIN supervision_level_start sl USING(person_id)
        INNER JOIN supervision_super_sessions ss USING(person_id)
        INNER JOIN `{project_id}.{state_dataset}.state_person` sp USING(person_id)
        LEFT JOIN phone_numbers ph USING(person_external_id)
    ),
    eligibility AS (
        SELECT
            external_id AS person_external_id,
            TRUE AS early_termination_eligible
        FROM `{project_id}.{workflows_dataset}.us_nd_complete_discharge_early_from_supervision_record_materialized`
    ),
    nd_clients AS (
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
            early_termination_eligible,
            # Not applicable to US_ND
            NULL AS current_balance,
            NULL AS last_payment_amount,
            CAST(NULL AS DATE) AS last_payment_date,
            CAST(NULL AS STRING) AS fee_exemptions,
            CAST(NULL AS ARRAY<string>) AS special_conditions,
            CAST(NULL AS DATE) AS next_special_conditions_check,
            CAST(NULL AS DATE) AS last_special_conditions_note,
            CAST(NULL AS DATE) AS special_conditions_terminated_date,
            CAST(NULL AS ARRAY<STRUCT<condition STRING, condition_description STRING>>) AS board_conditions,
            CAST(NULL AS STRING) AS compliant_reporting_eligible,
            NULL AS remaining_criteria_needed,
            CAST(NULL AS DATE) AS eligible_level_start,
            CAST(NULL AS ARRAY<string>) AS current_offenses,
            CAST(NULL AS ARRAY<string>) AS past_offenses,
            CAST(NULL AS ARRAY<string>) AS lifetime_offenses_expired,
            CAST(NULL AS STRING) AS judicial_district,
            CAST(NULL AS ARRAY<STRUCT<ContactNoteType STRING, contact_date DATE>>) AS drug_screens_past_year,
            CAST(NULL AS ARRAY<STRUCT<proposed_date DATE, ProposedSanction STRING>>) AS sanctions_past_year,
            CAST(NULL AS DATE) AS most_recent_arrest_check,
            CAST(NULL AS ARRAY<STRUCT<ContactNoteType STRING, contact_date DATE>>) AS zero_tolerance_codes,
            CAST(NULL AS STRING) AS fines_fees_eligible,
            CAST(NULL AS STRING) AS district,
            CAST(NULL AS STRING) AS special_conditions_flag,
            FALSE AS almost_eligible_time_on_supervision_level,
            CAST(NULL AS DATE) AS date_supervision_level_eligible,
            FALSE AS almost_eligible_drug_screen,
            FALSE AS almost_eligible_fines_fees,
            FALSE AS almost_eligible_recent_rejection,
            CAST(NULL AS ARRAY<string>) AS cr_rejections_past_3_months,
            FALSE AS almost_eligible_serious_sanctions,
            CAST(NULL AS DATE) AS date_serious_sanction_eligible,
        FROM clients
        LEFT JOIN eligibility USING(person_external_id)
    )
"""
