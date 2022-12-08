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
"""View logic to prepare US_ME Workflows supervision clients."""

from recidiviz.calculator.query.bq_utils import (
    array_concat_with_null,
    nonnull_end_date_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
    revert_nonnull_start_date_clause,
)
from recidiviz.task_eligibility.utils.raw_table_import import cis_319_term_cte

US_ME_INCARCERATION_RESIDENTS_QUERY_TEMPLATE = f"""
{cis_319_term_cte()},
    us_me_incarceration_cases AS (
        SELECT
            dataflow.state_code,
            dataflow.person_id,
            person_external_id,
            sp.full_name AS person_name,
            dataflow.facility AS facility_id,
            MIN(t.start_date) 
                    OVER(w) AS admission_date,
            MAX({nonnull_end_date_clause('t.end_date')}) 
                    OVER(w) AS release_date
            --TODO(#16175) ingest intake and release dates
        FROM `{{project_id}}.{{dataflow_dataset}}.most_recent_incarceration_population_span_metrics_materialized` dataflow
        INNER JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` sessions
            ON dataflow.state_code = sessions.state_code
            AND dataflow.person_id = sessions.person_id
            AND sessions.compartment_level_1 = "INCARCERATION"
            AND sessions.end_date IS NULL
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` sp 
            ON dataflow.person_id = sp.person_id
        -- Use raw_table to get admission and release dates
        LEFT JOIN term_cte t
          ON dataflow.person_id = t.person_id
          -- subset the possible start and end_dates to those consistent with
          -- the current date
              AND CURRENT_DATE('US/Eastern') 
                    BETWEEN {nonnull_start_date_clause('t.start_date')} 
                        AND {nonnull_end_date_clause('t.end_date')} 
        WHERE dataflow.state_code = 'US_ME' AND dataflow.included_in_state_population
            AND dataflow.end_date_exclusive IS NULL
        WINDOW w as (PARTITION BY dataflow.state_code, dataflow.person_id)
    ),
    us_me_incarceration_cases_wdates AS (
        SELECT 
            * EXCEPT(release_date, admission_date),
            {revert_nonnull_start_date_clause('admission_date')} AS admission_date, 
            {revert_nonnull_end_date_clause('release_date')} AS release_date
        FROM us_me_incarceration_cases
        GROUP BY 1,2,3,4,5,6,7
    )
    , custody_level AS (
        SELECT
            pei.person_id,
            UPPER(cs.CLIENT_SYS_DESC) AS custody_level,
        FROM `{{project_id}}.{{us_me_raw_data_dataset}}.CIS_112_CUSTODY_LEVEL` cl
        INNER JOIN `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_1017_CLIENT_SYS_latest` cs
            ON cl.CIS_1017_CLIENT_SYS_CD = cs.CLIENT_SYS_CD
        INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON cl.CIS_100_CLIENT_ID = pei.external_id
            AND pei.state_code = "US_ME"
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY CAST(LEFT(cl.CUSTODY_DATE, 19) AS DATETIME) DESC
        ) = 1
        ORDER BY
            pei.person_id,
            CAST(LEFT(cl.CUSTODY_DATE, 19) AS DATETIME)
    )
    , housing_unit AS (
      SELECT
        person_id,
        housing_unit AS unit_id,
        ROW_NUMBER() over (partition by person_id order by admission_date desc) rn 
      FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_period` 
      where
          release_date is null
          and state_code='US_ME'
      qualify rn = 1
    )
    , officer_assignments AS (
        SELECT DISTINCT
            Cis_900_Employee_Id as officer_id,
            Cis_100_Client_Id as person_external_id,
            row_number() OVER (PARTITION BY Cis_100_Client_Id ORDER BY Assignment_Date DESC) rn
        FROM `{{project_id}}.{{us_me_raw_data_up_to_date_dataset}}.CIS_124_SUPERVISION_HISTORY_latest` sp
        WHERE Supervision_End_Date IS NULL
        QUALIFY rn = 1
    )
    , resident_info AS (
        SELECT DISTINCT
            ic.state_code,
            ic.person_name,
            ic.person_id,
            ic.person_external_id,
            officer_id,
            facility_id,
            unit_id,
            custody_level.custody_level,
            ic.admission_date,
            ic.release_date,
        FROM
            us_me_incarceration_cases_wdates ic
        LEFT JOIN custody_level
            USING(person_id)
        LEFT JOIN housing_unit
          USING(person_id)
        LEFT JOIN officer_assignments
          USING(person_external_id)

    ),
    me_sccp_eligibility AS (
        SELECT
            external_id AS person_external_id,
            ["usMeSCCP"] AS eligible_opportunities,
        FROM `{{project_id}}.{{workflows_dataset}}.us_me_complete_transfer_to_sccp_form_record_materialized`
    ),
    me_residents AS (
        SELECT
            * EXCEPT(eligible_opportunities),
             {array_concat_with_null(["me_sccp_eligibility.eligible_opportunities"])} AS all_eligible_opportunities,
        FROM resident_info
        LEFT JOIN me_sccp_eligibility USING(person_external_id)
        WHERE officer_id IS NOT NULL
    )
"""
