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
#   =============================================================================
"""A template for building views to get attributes of a supervision population defined by some grouping variable."""


def supervision_population_attributes_template(grouping_var: str) -> str:
    return f"""
    /* Query that aggregates supervision population attributes by {grouping_var} at the start of every month */
    WITH dataflow_metrics_unnested AS 
        (
        SELECT 
            snapshot_date,
            state_code,
            person_id,
            compartment_level_1,
            compartment_level_2,
            supervision_office,
            supervision_district,
            correctional_level,
            supervising_officer_external_id,
            case_type,
            last_day_of_data,
        FROM 
            `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized` s,
            UNNEST(session_attributes) AS session_attributes,
            UNNEST(
                GENERATE_DATE_ARRAY(
                    DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 3 YEAR), 
                    DATE_TRUNC(CURRENT_DATE('US/Eastern'), 
                    MONTH
                ), INTERVAL 1 MONTH)
            ) AS snapshot_date
        WHERE 
            snapshot_date BETWEEN s.start_date 
            AND IFNULL(s.end_date, CURRENT_DATE('US/Eastern'))
    )
    ,
    caseloads_by_{grouping_var}_cte AS 
    (
    -- Retrieves caseload size per officer across all {grouping_var} units, and maintains association between officer and {grouping_var}
        SELECT
            state_code,
            {grouping_var},
            supervising_officer_external_id,
            snapshot_date,
            COUNT(DISTINCT person_id) OVER 
                (PARTITION BY state_code, supervising_officer_external_id, snapshot_date) 
            AS caseload,
        FROM
            dataflow_metrics_unnested
        WHERE TRUE QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY state_code, supervising_officer_external_id, snapshot_date, {grouping_var}) = 1
    )

    SELECT
        {grouping_var},
        state_code,
        snapshot_date,

        -- Number of clients
        num_clients,

        --  Number of officers
        COUNT(DISTINCT supervising_officer_external_id) AS num_officers,

        -- Average caseload of officers
        AVG(caseload) AS avg_caseload,

        -- Percent clients on compartment_level_2
        COUNT(DISTINCT IF(compartment_level_2 = "PROBATION", person_id, NULL)) / num_clients AS pct_compartment_level_2_probation,
        COUNT(DISTINCT IF(compartment_level_2 = "PAROLE", person_id, NULL)) / num_clients AS pct_compartment_level_2_parole,
        COUNT(DISTINCT IF(compartment_level_2 = "DUAL", person_id, NULL)) / num_clients AS pct_compartment_level_2_dual,
        COUNT(DISTINCT IF(compartment_level_2 = "COMMUNITY_CONFINEMENT", person_id, NULL)) / num_clients AS pct_compartment_level_2_community_confinement,

        -- Percent clients on specialized case types
        COUNT(DISTINCT IF(case_type = "SEX_OFFENSE", person_id, NULL)) / num_clients AS pct_case_type_sex_offense,
        COUNT(DISTINCT IF(case_type !=  "GENERAL" AND case_type IS NOT NULL, person_id, NULL)) / num_clients AS pct_case_type_specialized,

        -- Percent non-white clients
        COUNT(DISTINCT IF(prioritized_race_or_ethnicity != "WHITE", person_id, NULL)) / num_clients AS pct_race_non_white,

        -- Percent female clients
        COUNT(DISTINCT IF(gender = "FEMALE", person_id, NULL)) / num_clients AS pct_gender_female,

        -- Percent clients under age 30
        COUNT(DISTINCT IF(DATE_DIFF(last_day_of_data, birthdate, YEAR) < 30, person_id, NULL)) / num_clients AS pct_age_under_30,

        -- Average age of clients
        AVG(DATE_DIFF(last_day_of_data, birthdate, YEAR)) AS avg_age,

        -- Percent clients in supervision level
        COUNT (DISTINCT IF(correctional_level = "MINIMUM", person_id, NULL)) / num_clients AS pct_supervision_level_minimum,
        COUNT (DISTINCT IF(correctional_level = "MEDIUM", person_id, NULL)) / num_clients AS pct_supervision_level_medium,
        COUNT (DISTINCT IF(correctional_level = "HIGH", person_id, NULL)) / num_clients AS pct_supervision_level_high,
        COUNT (DISTINCT IF(correctional_level = "MAXIMUM", person_id, NULL)) / num_clients AS pct_supervision_level_maximum,
        COUNT (DISTINCT IF(correctional_level = "LIMITED", person_id, NULL)) / num_clients AS pct_supervision_level_limited,
    FROM
        (
        SELECT 
            *, 
            COUNT(DISTINCT person_id) OVER (PARTITION BY {grouping_var}, state_code, snapshot_date) AS num_clients
        FROM
            dataflow_metrics_unnested
        )
    INNER JOIN 
        `{{project_id}}.{{sessions_dataset}}.person_demographics_materialized`
    USING 
        (state_code, person_id)
    LEFT JOIN
        caseloads_by_{grouping_var}_cte
    USING
        (state_code, snapshot_date, supervising_officer_external_id, {grouping_var})
    WHERE 
        compartment_level_1 = "SUPERVISION"
    GROUP BY 1,2,3,4
    """
