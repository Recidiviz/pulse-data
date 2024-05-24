# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""CTEs used to create resident record query."""

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
    revert_nonnull_start_date_clause,
)
from recidiviz.task_eligibility.utils.us_mo_query_fragments import current_bed_stay_cte

_RESIDENT_RECORD_INCARCERATION_CTE = """
    incarceration_cases AS (
        SELECT
            dataflow.state_code,
            dataflow.person_id,
            person_external_id,
            sp.full_name AS person_name,
            sp.gender AS gender,
            IF( dataflow.state_code IN ({level_2_state_codes}),
                COALESCE(locations.level_2_incarceration_location_external_id, dataflow.facility),
                dataflow.facility) AS facility_id,
            sessions.start_date as admission_date
        FROM `{project_id}.{dataflow_dataset}.most_recent_incarceration_population_span_metrics_materialized` dataflow
        INNER JOIN `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` sessions
            ON dataflow.state_code = sessions.state_code
            AND dataflow.person_id = sessions.person_id
            AND sessions.compartment_level_1 = "INCARCERATION"
            AND sessions.end_date IS NULL
        INNER JOIN `{project_id}.{normalized_state_dataset}.state_person` sp 
            ON dataflow.person_id = sp.person_id
        LEFT JOIN `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names` locations
            ON dataflow.facility = locations.level_1_incarceration_location_external_id
        WHERE dataflow.state_code IN ({workflows_incarceration_states}) AND dataflow.included_in_state_population
            AND dataflow.end_date_exclusive IS NULL
            AND NOT (dataflow.state_code = "US_TN"
                    AND locations.level_2_incarceration_location_external_id IN ({us_tn_excluded_facility_ids}))
            AND NOT (dataflow.state_code = "US_IX"
                    AND locations.level_2_incarceration_location_external_id IN ({us_ix_excluded_facility_types}))
    ),
"""

_RESIDENT_RECORD_INCARCERATION_DATES_CTE = f"""
    incarceration_dates AS (
        -- Adding a TN specific admission date that is admission to TDOC facility, in addition to overall incarceration
        -- admission date
        SELECT 
            ic.*,
            MAX(t.projected_completion_date_max) 
                    OVER(w) AS release_date,
            MAX(c.start_date) 
                    OVER(w) AS us_tn_facility_admission_date,
        FROM
            incarceration_cases ic
        LEFT JOIN (
            SELECT person_id, state_code, start_date
            FROM `{{project_id}}.sessions.location_type_sessions_materialized`
            WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date
                AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
                AND location_type = "STATE_PRISON"
        ) c
            USING(person_id, state_code)
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_projected_completion_date_spans_materialized` t
          ON ic.person_id = t.person_id
              AND ic.state_code = t.state_code
              AND CURRENT_DATE('US/Eastern') 
                BETWEEN t.start_date AND {nonnull_end_date_exclusive_clause('t.end_date_exclusive')} 
        WHERE ic.state_code IN ("US_TN")
        WINDOW w as (PARTITION BY ic.person_id)
        
        UNION ALL
        
        SELECT 
            ic.* EXCEPT(admission_date),
            MAX(t.start_date) 
                    OVER(w) AS admission_date,
            MAX({nonnull_end_date_clause('t.end_date')}) 
                    OVER(w) AS release_date,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
            --TODO(#16175) ingest intake and release dates
        FROM
            incarceration_cases ic
        -- Use raw_table to get admission and release dates
        LEFT JOIN `{{project_id}}.{{analyst_dataset}}.us_me_sentence_term_materialized` t
          ON ic.person_id = t.person_id
          -- subset the possible start and end_dates to those consistent with
          -- the current date
              AND CURRENT_DATE('US/Eastern') 
                    BETWEEN {nonnull_start_date_clause('t.start_date')} 
                        AND {nonnull_end_date_clause('t.end_date')} 
              AND t.status='1' -- only 'Active terms'
        WHERE ic.state_code="US_ME"
        WINDOW w as (PARTITION BY ic.person_id)

        UNION ALL

        SELECT
            ic.* EXCEPT(admission_date),
            NULL AS admission_date,
            NULL AS release_date,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
        FROM incarceration_cases ic
        WHERE state_code="US_MO"
        
        UNION ALL

        SELECT 
            ic.*,
            MAX(t.projected_completion_date_max) 
                    OVER(w) AS release_date,
            CAST(NULL AS DATE) AS us_tn_facility_admission_date,
        FROM
            incarceration_cases ic
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.incarceration_projected_completion_date_spans_materialized` t
          ON ic.person_id = t.person_id
              AND ic.state_code = t.state_code
              AND CURRENT_DATE('US/Eastern') 
                BETWEEN t.start_date AND {nonnull_end_date_exclusive_clause('t.end_date_exclusive')} 
        WHERE ic.state_code NOT IN ("US_ME", "US_MO", "US_TN")
        WINDOW w as (PARTITION BY ic.person_id)
    ),
"""

_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_CTE = f"""
    incarceration_cases_wdates AS (
        SELECT 
            * EXCEPT(release_date, admission_date),
            {revert_nonnull_start_date_clause('admission_date')} AS admission_date, 
            {revert_nonnull_end_date_clause('release_date')} AS release_date
        FROM incarceration_dates
        GROUP BY 1,2,3,4,5,6,7,8,9
    ),
"""

_RESIDENT_RECORD_CUSTODY_LEVEL_CTE = f"""
    custody_level AS (
        SELECT
            pei.person_id,
            UPPER(cs.CLIENT_SYS_DESC) AS custody_level,
        FROM `{{project_id}}.{{us_me_raw_data_dataset}}.CIS_112_CUSTODY_LEVEL` cl
        INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_1017_CLIENT_SYS_latest` cs
            ON cl.CIS_1017_CLIENT_SYS_CD = cs.CLIENT_SYS_CD
        INNER JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
            ON cl.CIS_100_CLIENT_ID = pei.person_external_id
            AND pei.state_code = "US_ME"
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY CAST(LEFT(cl.CUSTODY_DATE, 19) AS DATETIME) DESC
        ) = 1
        
        UNION ALL
        
        SELECT 
            pei.person_id,
            BL_ICA AS custody_level
        FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBAKRDTA_TAK015_latest` tak015
        INNER JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
            ON BL_DOC = pei.person_external_id
            AND pei.state_code = "US_MO"
        -- We want to keep the latest Custody Assessment date. When there are two assessments on the same day,
        -- we deduplicate using CNO which is part of the primary key. Finally, there's still a very small number of
        -- duplicates where the same person has the same BL_IC and BL_CNO, but different cycle numbers, so we further
        -- prioritize the latest cycle
        QUALIFY ROW_NUMBER() OVER(PARTITION BY BL_DOC ORDER BY
                                                        SAFE.PARSE_DATE('%Y%m%d', tak015.BL_IC) DESC,
                                                        tak015.BL_CNO DESC,
                                                        tak015.BL_CYC DESC) = 1
        UNION ALL

        SELECT
            person_id,
            custody_level,
        FROM `{{project_id}}.{{sessions_dataset}}.custody_level_sessions_materialized`
        WHERE state_code NOT IN ("US_ME", "US_MO")
        AND CURRENT_DATE('US/Eastern') 
            BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')} 
    ),
"""

_RESIDENT_RECORD_HOUSING_UNIT_CTE = f"""
    {current_bed_stay_cte()},
    housing_unit AS (
      SELECT
        person_id,
        CAST(NULL AS STRING) AS facility_id,
        housing_unit AS unit_id
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` 
      WHERE
          release_date IS NULL
          AND state_code NOT IN ('US_MO','US_TN')
      QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY admission_date DESC) = 1

      UNION ALL

      SELECT
        person_id,
        CAST(NULL AS STRING) AS facility_id,
        IF(complex_number=building_number, complex_number, complex_number || " " || building_number) AS unit_id
      FROM current_bed_stay
      
      UNION ALL
      
      -- TODO(#27428): Once source of facility ID in TN is reconciled, this can be removed
      SELECT
        person_id,
        facility_id,
        unit_id
      FROM `{{project_id}}.analyst_data.us_tn_cellbed_assignment_raw_materialized`
      
    ),
"""

_RESIDENT_RECORD_OFFICER_ASSIGNMENTS_CTE = """
    officer_assignments AS (
        SELECT DISTINCT
            "US_ME" AS state_code,
            IFNULL(ids.external_id_mapped, Cis_900_Employee_Id) AS officer_id,
            Cis_100_Client_Id as person_external_id
        FROM `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_124_SUPERVISION_HISTORY_latest` sp
        LEFT JOIN `{project_id}.{static_reference_dataset}.agent_multiple_ids_map` ids
            ON Cis_900_Employee_Id = ids.external_id_to_map AND 'US_ME' = ids.state_code 
        WHERE Supervision_End_Date IS NULL
            -- Ignore assignments from the future
            AND SAFE_CAST(Assignment_Date AS DATETIME) <= CURRENT_DATE('US/Eastern')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY Cis_100_Client_Id 
                        /* Prioritize cases in the following order 
                                1) Case Workers (1) and Correctional Care and Treatment Worker (4)
                                2) Latest assignment date
                                3) Status of officer: primary, secondary, temporary */
                                    ORDER BY IF(Cis_1240_Supervision_Type_Cd IN ('1', '4'), 0, 1) , 
                                            Assignment_Date DESC,
                                            Cis_1241_Super_Status_Cd) = 1

        UNION ALL

        SELECT
            state_code,
            -- In MO and TN we treat facilities as officers to allow searching by facility
            ic.facility_id AS officer_id,
            ic.person_external_id
        FROM incarceration_cases ic
        WHERE state_code IN ({search_by_location_states})
    ),
"""

_RESIDENT_PORTION_NEEDED_CTE = """
    portion_needed AS (
      SELECT state_code, person_id,
      REPLACE(TO_JSON_STRING(JSON_EXTRACT(reason, '$.x_portion_served')), '"', "")
          AS portion_served_needed,
      REPLACE(TO_JSON_STRING(JSON_EXTRACT(reason, '$.eligible_date')), '"', "")
          AS portion_needed_eligible_date,
      FROM `{project_id}.{us_me_task_eligibility_criteria_dataset}.served_x_portion_of_sentence_materialized`
          AS served_x
      WHERE CURRENT_DATE("US/Eastern")
      BETWEEN served_x.start_date AND IFNULL(DATE_SUB(served_x.end_date, INTERVAL 1 DAY), "9999-12-31")
    ),
"""

_RESIDENT_MONTHS_REMAINING_NEEDED_CTE = """
    months_remaining AS (
        SELECT state_code, person_id,
        REPLACE(TO_JSON_STRING(JSON_EXTRACT(reason, '$.eligible_date')), '"', "")
            AS months_remaining_eligible_date,
        FROM `{project_id}.{us_me_task_eligibility_criteria_dataset}.x_months_remaining_on_sentence_materialized` 
          AS months_remaining
      WHERE CURRENT_DATE("US/Eastern")
      BETWEEN months_remaining.start_date AND IFNULL(DATE_SUB(months_remaining.end_date, INTERVAL 1 DAY), "9999-12-31")
    ),
"""

_RESIDENT_RECORD_JOIN_RESIDENTS_CTE = """
    join_residents AS (
        SELECT DISTINCT
            ic.state_code,
            ic.person_name,
            ic.person_id,
            ic.person_external_id,
            ic.gender,
            officer_id,
            -- TODO(#27428): Once source of facility ID in TN is reconciled, this can be removed
            CASE WHEN ic.state_code = 'US_TN' THEN hu.facility_id ELSE ic.facility_id END AS facility_id,
            unit_id,
            custody_level.custody_level,
            ic.admission_date,
            ic.release_date,
            ic.us_tn_facility_admission_date,
        FROM
            incarceration_cases_wdates ic
        LEFT JOIN custody_level
            USING(person_id)
        LEFT JOIN housing_unit hu
          USING(person_id)
        LEFT JOIN officer_assignments
          USING(state_code, person_external_id)
    ),
"""

_RESIDENTS_CTE = """
    residents AS (
        SELECT
            person_external_id,
            person_external_id as display_id,
            state_code,
            person_name,
            person_id,
            gender,
            officer_id,
            facility_id,
            unit_id,
            CONCAT(facility_id, "-_-", IFNULL(unit_id, "")) as facility_unit_id,
            custody_level,
            admission_date,
            release_date,
            us_tn_facility_admission_date,
            opportunities_aggregated.all_eligible_opportunities,
            portion_served_needed,
            portion_needed_eligible_date AS us_me_portion_needed_eligible_date,
            GREATEST(portion_needed_eligible_date, months_remaining_eligible_date) AS sccp_eligibility_date,
        FROM join_residents
        LEFT JOIN opportunities_aggregated USING (state_code, person_external_id)
        LEFT JOIN portion_needed USING (state_code, person_id)
        LEFT JOIN months_remaining USING (state_code, person_id)
        WHERE officer_id IS NOT NULL
    )
"""


def full_resident_record() -> str:
    return f"""
    {_RESIDENT_RECORD_INCARCERATION_CTE}
    {_RESIDENT_RECORD_INCARCERATION_DATES_CTE}
    {_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_CTE}
    {_RESIDENT_RECORD_CUSTODY_LEVEL_CTE}
    {_RESIDENT_RECORD_HOUSING_UNIT_CTE}
    {_RESIDENT_RECORD_OFFICER_ASSIGNMENTS_CTE}
    {_RESIDENT_PORTION_NEEDED_CTE}
    {_RESIDENT_MONTHS_REMAINING_NEEDED_CTE}
    {_RESIDENT_RECORD_JOIN_RESIDENTS_CTE}
    {_RESIDENTS_CTE}
    """
