# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Historical incarceration population for ID by month only counting the commitments that are paid by the IDOC"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_VIEW_NAME = (
    "us_id_monthly_paid_incarceration_population"
)

US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_VIEW_DESCRIPTION = (
    """"Historical population by compartment and month"""
)

US_ID_INCARCERATION_DISAGGREGATED_COUNTY_JAILS = [
    "BONNEVILLE COUNTY SHERIFF DEPARTMENT",
    "JEFFERSON COUNTY SHERIFF DEPARTMENT",
]

US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_QUERY_TEMPLATE = """
    WITH parsed_movements AS (
        SELECT
            state_code,
            person_id,
            CONCAT(fac_cd, lu_cd) AS faclu_code,
            COALESCE(ofndr_loc.assgn_rsn_cd, 'INTERNAL_UNKNOWN') AS assgn_rsn_cd,
            CAST(SUBSTR(move_dtd, 1, 10) AS DATE) AS movement_start_date,
        # TODO(#5142): use the paid logic from a more ingested source
        FROM `{project_id}.us_id_raw_data_up_to_date_views.movement_latest` move
        LEFT JOIN `{project_id}.us_id_raw_data_up_to_date_views.ofndr_loc_hist_latest` ofndr_loc
            ON move.docno = ofndr_loc.ofndr_num
            AND SUBSTR(move.move_dtd, 1, 10) = SUBSTR(ofndr_loc.assgn_dt, 1, 10)
        LEFT JOIN `{project_id}.{state_base_dataset}.state_person_external_id` pei
          ON move.docno = pei.external_id
          AND state_code = 'US_ID'
    ), movement_sessions AS (
        -- Turn the ledger style movements table into a period/session style table with start & end dates
        -- in order to join with compartment_sub_sessions
        SELECT
            state_code,
            person_id,
            -- Collapse all the rows with the same date
            ARRAY_AGG(faclu_code) AS faclu_list,
            ARRAY_AGG(assgn_rsn_cd) AS assgn_rsn_list,
            movement_start_date,
            -- Pull the move date from the following row as the period end date
            LEAD(movement_start_date)
                OVER (PARTITION BY state_code, person_id ORDER BY movement_start_date) AS movement_end_date
        FROM parsed_movements
        GROUP BY state_code, person_id, movement_start_date
    ),
    paid_status_per_movement_session AS (
        -- Set the pay flag for each session
        SELECT
            state_code, person_id,
            movement_start_date,
            movement_end_date,
            -- If any of the rows have one of these codes then the movement period is not paid by the IDOC
            NOT LOGICAL_OR(faclu IN ('RTSX', 'RTUT', 'CJVS', 'CJCT')
                           OR assgn_rsn_cd IN ('7', '16', '18', '43', '81')
                           OR (faclu = 'RTAN' AND assgn_rsn_cd = '56')
            ) AS pay_flag,
        FROM movement_sessions,
        UNNEST(faclu_list) AS faclu,
        UNNEST(assgn_rsn_list) AS assgn_rsn_cd
        GROUP BY state_code, person_id, movement_start_date, movement_end_date
    ),
    paid_status_on_compartment_sessions AS (
        -- Join the movement sessions to compartment_sub_sessions to get the corresponding sub-session data
        -- (admission/release dates, facility, and demographic info) for the periods that overlap the first of the month
        SELECT DISTINCT
            -- Use DISTINCT to collapse start/end date movement sessions into 1 row
            sessions.state_code,
            sessions.person_id,
            sessions.gender,
            report_month.run_date AS report_month,
            -- Group the majority of the county jail locations into the "COUNTY JAIL" category for counting purposes
            CASE WHEN compartment_location IN ('{disaggregated_county_jails}') THEN compartment_location
                 WHEN (compartment_location LIKE '%COUNTY SHERIFF%') OR (compartment_location = 'JAIL BACKLOG') THEN 'COUNTY JAIL'
                 ELSE compartment_location
            END AS facility,
            sessions.compartment_level_1,
            sessions.compartment_level_2,
            pay_flag
        FROM `{project_id}.{population_projection_dataset}.simulation_run_dates` AS report_month
        INNER JOIN `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized` sessions
            ON report_month.run_date BETWEEN sessions.start_date AND COALESCE(DATE_ADD(sessions.end_date, INTERVAL 1 DAY), '9999-01-01')
        -- Drop incarceration locations that should not be counted (mostly out of state incarcerations)
        INNER JOIN `{project_id}.{static_reference_dataset}.population_projection_facilities` facilities
            ON sessions.compartment_location = facilities.facility
            AND sessions.state_code = facilities.state_code
        LEFT JOIN paid_status_per_movement_session paid_status
            ON paid_status.state_code = sessions.state_code
            AND paid_status.person_id = sessions.person_id
            AND report_month.run_date BETWEEN paid_status.movement_start_date AND COALESCE(paid_status.movement_end_date, '9999-01-01')
        WHERE sessions.state_code = 'US_ID'
            AND gender IN ('FEMALE', 'MALE')
            AND compartment_level_2 NOT IN ('OTHER', 'INTERNAL_UNKNOWN')
            AND compartment_level_1 = 'INCARCERATION'
    )
    SELECT
        state_code,
        person_id,
        gender,
        facility,
        compartment_level_1,
        compartment_level_2,
        report_month,
    FROM paid_status_on_compartment_sessions
    -- Only use the pay_flag for the county jails
    WHERE pay_flag OR (facility NOT IN ('COUNTY JAIL', '{disaggregated_county_jails}'))
    ORDER BY state_code, person_id, report_month
    """

US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_VIEW_NAME,
    view_query_template=US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_QUERY_TEMPLATE,
    description=US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    state_base_dataset=dataset_config.STATE_BASE_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    disaggregated_county_jails="', '".join(
        US_ID_INCARCERATION_DISAGGREGATED_COUNTY_JAILS
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_MONTHLY_PAID_INCARCERATION_POPULATION_VIEW_BUILDER.build_and_print()
