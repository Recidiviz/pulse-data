# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""View tracking daily metrics at the officer-office level"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_NAME = "supervision_officer_office_metrics"

SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_DESCRIPTION = """
Tracks daily officer-office level metrics
"""

SUPERVISION_OFFICER_OFFICE_METRICS_QUERY_TEMPLATE = """
/*{description}*/

WITH date_array AS (
    SELECT
        date,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH),
            DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
            INTERVAL 1 DAY
        )) AS date
)
# Unnested cte of officer-office per person and date
, officer_office_sessions_unnested AS ( 
    SELECT * 
    FROM 
        `{project_id}.{sessions_dataset}.supervision_officer_office_sessions_materialized` 
    INNER JOIN 
        date_array 
    ON 
        date BETWEEN start_date AND end_date 
)

###############
# Person events
###############

# Transitions from supervision to release
, successful_completions AS (
    SELECT 
        a.state_code, 
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        date,
    FROM
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.end_date = b.date
    WHERE
        a.compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
        AND a.outflow_to_level_1 = "RELEASE"
)
# Valid earned discharge requests
, earned_discharge_requests AS (
    SELECT DISTINCT
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        date,
    FROM
        `{project_id}.{base_dataset}.state_early_discharge` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.request_date = b.date
    WHERE
        decision_status != "INVALID"
)
# Changes in supervision level, along with direction of change
, supervision_level_changes AS (
    SELECT DISTINCT
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        IF(a.supervision_downgrade > 0, "DOWNGRADE", "UPGRADE") AS change_type,
        date,
    FROM
        `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.start_date = b.date
    WHERE
        a.supervision_downgrade > 0 OR a.supervision_upgrade > 0
)
# Violation responses, with violation type and response decision
, violations AS (
    SELECT 
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        most_serious_violation_type,
        most_severe_response_decision AS response_decision,
        date,
    FROM
        `{project_id}.{sessions_dataset}.violations_sessions_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.response_date = b.date
)
# Transitions from supervision to incarceration
, incarcerations AS (
    SELECT 
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        outflow_to_level_2 IN ("PAROLE_BOARD_HOLD", "PENDING_CUSTODY", 
            "TEMPORARY_CUSTODY", "SUSPENSION", "SHOCK_INCARCERATION") AS temporary_flag,
        date,
    FROM
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.end_date = b.date
    WHERE
        compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
        AND outflow_to_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE")
)
# Changes in employment status from employment to unemployment, or vice versa
, employment_changes AS (
  SELECT DISTINCT
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        date,
        is_employed,
    FROM
        `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.employment_status_start_date = b.date
    QUALIFY 
        # only keep dates where the person gained or lost employment
        # edge case: treat jobs at supervision start as employment gains
        # but no job at supervision start is not an employment loss
        is_employed != IFNULL(LAG(is_employed) OVER (
            PARTITION BY person_id, supervising_officer_external_id, district, office, date
            ORDER BY employment_status_start_date
        ), FALSE)
)
# All drug screen dates, along with whether at least one test on a given day had a positive result
, drug_screens AS (
    SELECT 
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        date,
        LOGICAL_OR(is_positive_result) AS is_positive_result,
    FROM
        `{project_id}.{sessions_dataset}.drug_screens_preprocessed_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.drug_screen_date = b.date
    GROUP BY 1,2,3,4,5,6
)

# skip revocations for now because the lag from temporary hold to actual revocation 
# makes it challenging to associate revocation with an officer

#################
# Window Metrics
# Calculates metrics such as days incarcerated, days employed, and employment stability, 
# over a year window following initial assignment to officer-office.
#################

, window_metrics AS (
    # For all metrics below, we include all date indexes from other joined tables besides 
    # the relevant table for the current metric in the window partition, 
    # to avoid counting duplicates.
    SELECT
        sss.supervision_super_session_id,
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        date,

        # Number of days incarcerated within a year of first assignment to officer
        SUM(
            DATE_DIFF(
                LEAST(
                    IFNULL(c.end_date, "9999-01-01"), 
                    DATE_ADD(a.start_date, INTERVAL 365 DAY)
                ), c.start_date, DAY
            )
        ) OVER (
            PARTITION BY sss.supervision_super_session_id, a.state_code, a.person_id, 
            supervising_officer_external_id, district, office, date, 
            d.employment_status_start_date, e.employment_start_date
        ) AS days_incarcerated_1yr,

        # Number of days employed within a year of first assignment to officer
        SUM(
            CASE WHEN is_employed
            THEN DATE_DIFF(
                LEAST(
                    IFNULL(d.employment_status_end_date, "9999-01-01"), 
                    DATE_ADD(a.start_date, INTERVAL 365 DAY)
                ), 
                GREATEST(
                    d.employment_status_start_date, 
                    a.start_date
                ), DAY
            )
            WHEN is_employed = FALSE THEN 0 END
        ) OVER (
            PARTITION BY sss.supervision_super_session_id, a.state_code, a.person_id, 
            supervising_officer_external_id, district, office, date, 
            c.start_date, e.employment_start_date
        ) AS days_employed_1yr,

        # Number of days at the longest stint with a consistent employer within a year 
        # of first assignment to officer
        MAX(
            CASE WHEN NOT is_unemployed
            THEN DATE_DIFF(
                LEAST(
                    IFNULL(e.employment_end_date, "9999-01-01"), 
                    DATE_ADD(a.start_date, INTERVAL 365 DAY)
                ), 
                GREATEST(
                    e.employment_start_date, 
                    a.start_date
                ), DAY
            )
            WHEN is_unemployed THEN 0 END
        ) OVER (
            PARTITION BY sss.supervision_super_session_id, a.state_code, a.person_id, 
            supervising_officer_external_id, district, office, date, 
            c.start_date, d.employment_status_start_date
        ) AS max_days_stable_employment_1yr,

        # Number of unique employers within a year of first assignment to officer
        COUNT(DISTINCT IF(is_unemployed, NULL, employer_name)) OVER (
            PARTITION BY sss.supervision_super_session_id, a.state_code, a.person_id, 
            supervising_officer_external_id, district, office, date, 
            c.start_date, d.employment_status_start_date
        ) AS num_unique_employers_1yr,

    # first date client associated with officer-office during SSS
    FROM
        `{project_id}.{sessions_dataset}.supervision_officer_office_sessions_materialized` a
    INNER JOIN
        date_array
    ON
        date = a.start_date
    INNER JOIN
        `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` sss
    ON
        sss.state_code = a.state_code
        AND sss.person_id = a.person_id
        AND a.start_date BETWEEN sss.start_date AND IFNULL(sss.end_date, "9999-01-01")

    # join compartment level 1 super sessions for calculating days incarcerated
    LEFT JOIN
        `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized` c
    ON
        a.state_code = c.state_code
        AND a.person_id = c.person_id
        # count days incarcerated in year following officer assignment
        AND c.start_date BETWEEN a.start_date AND
            DATE_ADD(a.start_date, INTERVAL 365 DAY)
        AND c.compartment_level_1 IN ("INCARCERATION", "INCARCERATION_OUT_OF_STATE")
    # join employment sessions for calculating days employed
    LEFT JOIN
        `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized` d
    ON
        a.state_code = d.state_code
        AND a.person_id = d.person_id
        # count days incarcerated in year following officer assignment
        AND IFNULL(d.employment_status_end_date, "9999-01-01") > a.start_date 
        AND d.employment_status_start_date < DATE_ADD(a.start_date, INTERVAL 365 DAY)

    # join employment periods for calculating employment stability and volatility metrics
    LEFT JOIN
        `{project_id}.{sessions_dataset}.employment_periods_preprocessed_materialized` e
    ON
        a.state_code = e.state_code
        AND a.person_id = e.person_id
        AND IFNULL(e.employment_end_date, "9999-01-01") > a.start_date 
        AND e.employment_start_date < DATE_ADD(a.start_date, INTERVAL 365 DAY)
    # Keep first assignment of officer-office to client within SSS only
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY sss.supervision_super_session_id, a.state_code, 
            a.person_id, supervising_officer_external_id, district, office 
            ORDER BY date
        ) = 1
)

#####################
# Caseload attributes
#####################

, caseload_attributes AS (
    SELECT
        b.state_code,
        b.supervising_officer_external_id,
        district,
        office,
        date,
        DATE_DIFF(date, first_officer_date, DAY) AS officer_tenure_days,
        COUNT(DISTINCT c.person_id) AS caseload_all,
        COUNT(DISTINCT IF(compartment_level_1 = "SUPERVISION_OUT_OF_STATE", 
            c.person_id, NULL)) AS caseload_out_of_state,
        COUNT(DISTINCT IF(compartment_level_2 IN ("PAROLE", "DUAL"), c.person_id, NULL)
            ) AS caseload_parole,
        COUNT(DISTINCT IF(compartment_level_2 IN ("PROBATION", "INFORMAL_PROBATION"), 
            c.person_id, NULL)) AS caseload_probation,
        COUNT(DISTINCT IF(compartment_level_2 IN ("BENCH_WARRANT", "ABSCONSION",
            "INTERNAL_UNKNOWN"), c.person_id, NULL)) AS caseload_other_supervision_type,
        COUNT(DISTINCT IF(c.gender = "FEMALE", c.person_id, NULL)
            ) AS caseload_female,
        COUNT(DISTINCT IF(c.prioritized_race_or_ethnicity != "WHITE", c.person_id, NULL)
            ) AS caseload_nonwhite,
        COUNT(DISTINCT IF(case_type_start = "GENERAL", c.person_id, NULL)
            ) AS caseload_general,
        COUNT(DISTINCT IF(case_type_start = "DOMESTIC_VIOLENCE", c.person_id, NULL)
            ) AS caseload_domestic_violence,
        COUNT(DISTINCT IF(case_type_start = "SEX_OFFENSE", c.person_id, NULL)
            ) AS caseload_sex_offense,
        COUNT(DISTINCT IF(case_type_start = "DRUG_COURT", c.person_id, NULL)
            ) AS caseload_drug,
        COUNT(DISTINCT IF(case_type_start IN ("SERIOUS_MENTAL_ILLNESS", 
            "MENTAL_HEALTH_COURT"), c.person_id, NULL)) AS caseload_mental_health,
        COUNT(DISTINCT IF(case_type_start NOT IN ("GENERAL", "DOMESTIC_VIOLENCE",
            "SEX_OFFENSE", "DRUG_COURT", "SERIOUS_MENTAL_ILLNESS", 
            "MENTAL_HEALTH_COURT"), c.person_id, NULL)) AS caseload_other_case_type,
        COUNT(DISTINCT IF(case_type_start IS NULL, c.person_id, NULL)
            ) AS caseload_unknown,
        AVG(assessment_score) AS avg_lsir_score,
        COUNT(DISTINCT IF(assessment_score IS NULL, score.person_id, NULL)) 
            AS caseload_no_lsir_score,
        COUNT(DISTINCT IF(assessment_level IN ("LOW", "LOW_MEDIUM", "MINIMUM"), score.person_id,
            NULL)) AS caseload_low_risk_level,
        COUNT(DISTINCT IF(assessment_level IN ("HIGH", "MEDIUM_HIGH", "MAXIMUM", "VERY_HIGH"), 
            score.person_id, NULL)) AS caseload_high_risk_level,
        COUNT(DISTINCT IF(assessment_level IS NULL OR assessment_level LIKE "%UNKNOWN",
            score.person_id, NULL)) AS caseload_unknown_risk_level,
        AVG(DATE_DIFF(date, birthdate, DAY) / 365.25) AS avg_age,
        COUNT(DISTINCT IF(is_employed, e.person_id, NULL)) AS caseload_is_employed,
    FROM
        date_array d
    INNER JOIN
        `{project_id}.{sessions_dataset}.supervision_officer_office_sessions_materialized` b
    ON
        d.date BETWEEN b.start_date AND IFNULL(b.end_date, "9999-01-01")
    INNER JOIN (
        SELECT 
            state_code,
            supervising_officer_external_id,
            MIN(start_date) AS first_officer_date
        FROM
            `{project_id}.{sessions_dataset}.supervision_officer_office_sessions_materialized`
        GROUP BY 1, 2
    ) first_officer_tbl
    ON
        first_officer_tbl.state_code = b.state_code
        AND first_officer_tbl.supervising_officer_external_id = b.supervising_officer_external_id
    LEFT JOIN
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` c
    ON
        b.state_code = c.state_code
        AND b.person_id = c.person_id
        AND d.date BETWEEN c.start_date AND IFNULL(c.end_date, "9999-01-01")
        AND c.compartment_level_1 LIKE "SUPERVISION%"
    LEFT JOIN
        `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` score
    ON
        score.state_code = c.state_code
        AND score.person_id = c.person_id
        AND date BETWEEN assessment_date AND IFNULL(score_end_date, "9999-01-01")
        AND assessment_type = "LSIR"
    LEFT JOIN
        `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized` e
    ON 
        e.state_code = c.state_code
        AND e.person_id = c.person_id
        AND date BETWEEN employment_status_start_date AND 
            IFNULL(employment_status_end_date, "9999-01-01")
    LEFT JOIN
        `{project_id}.{sessions_dataset}.person_demographics_materialized` bday
    ON
        bday.state_code = c.state_code
        AND bday.person_id = c.person_id
    GROUP BY 1, 2, 3, 4, 5, 6
)

##################
# Combined metrics
##################

# Null metrics replaced with zeros
# I split this into two sets of joins to avoid a massive GROUP BY clause
SELECT
    *
FROM
    caseload_attributes
LEFT JOIN (
    SELECT
        # index columns
        {join_columns},

        # successful_completions
        COUNT(DISTINCT successful_completions.person_id) AS successful_completions,

        # earned_discharge_requests
        COUNT(DISTINCT earned_discharge_requests.person_id) AS earned_discharge_requests,

        # supervision_level_changes
        COUNT(DISTINCT IF(
            supervision_level_changes.change_type = "DOWNGRADE",
            supervision_level_changes.person_id, NULL)) AS supervision_downgrades,
        COUNT(DISTINCT IF(
            supervision_level_changes.change_type = "UPGRADE",
            supervision_level_changes.person_id, NULL)) AS supervision_upgrades,

        # violations (max one per person per day per type)
        COUNT(DISTINCT violations.person_id) AS violations,
        COUNT(DISTINCT IF(violations.most_serious_violation_type IN ("ESCAPED", 
            "ABSCONDED"), violations.person_id, NULL)) AS violations_absconded,
        COUNT(DISTINCT IF(violations.most_serious_violation_type IN ("FELONY", "LAW", 
            "MISDEMEANOR", "MUNICIPAL"), violations.person_id, NULL)) AS violations_legal,
        COUNT(DISTINCT IF(violations.most_serious_violation_type = "TECHNICAL",
            violations.person_id, NULL)) AS violations_technical,

        # incarcerations
        COUNT(DISTINCT IF(incarcerations.temporary_flag, incarcerations.person_id, 
            NULL)) AS incarcerations_temporary,
        COUNT(DISTINCT incarcerations.person_id) AS incarcerations_all,

        # drug screens
        COUNT(DISTINCT IF(drug_screens.is_positive_result, drug_screens.person_id, 
            NULL)) AS drug_screens_positive,
        COUNT(DISTINCT drug_screens.person_id) AS drug_screens_all,

        # employment starts (transitions from unemployed to employed)
        COUNT(DISTINCT IF(is_employed, employment_changes.person_id, 
            NULL)) AS gained_employment,

        # employment ends (transitions from employed to unemployed)
        COUNT(DISTINCT IF(NOT is_employed, employment_changes.person_id, 
            NULL)) AS lost_employment,

        # assignments to officer-office within year
        COUNT(DISTINCT window_metrics.person_id) AS new_clients_assigned,

        # window metrics since initial officer-office assignment
        SUM(window_metrics.days_employed_1yr) AS days_employed_1yr,
        SUM(window_metrics.max_days_stable_employment_1yr) AS max_days_stable_employment_1yr,
        SUM(window_metrics.num_unique_employers_1yr) AS num_unique_employers_1yr,

        IFNULL(SUM(window_metrics.days_incarcerated_1yr), 0) AS days_incarcerated_1yr,
        COUNT(window_metrics.person_id) * DATE_DIFF(
            LEAST(
                CURRENT_DATE("US/Eastern"),
                DATE_ADD(date, INTERVAL 365 DAY)
            ), date, DAY) AS days_since_assignment_1yr,

    FROM caseload_attributes
    LEFT JOIN successful_completions USING({join_columns})
    LEFT JOIN earned_discharge_requests USING({join_columns})
    LEFT JOIN supervision_level_changes USING({join_columns})
    LEFT JOIN violations USING({join_columns})
    LEFT JOIN incarcerations USING({join_columns})
    LEFT JOIN employment_changes USING({join_columns})
    LEFT JOIN drug_screens USING({join_columns})
    LEFT JOIN window_metrics USING({join_columns})
    GROUP BY 1, 2, 3, 4, 5
) 
USING ({join_columns})
# no ORDER BY because table too large, too computationally expensive
"""

SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_OFFICE_METRICS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    join_columns="state_code, supervising_officer_external_id, district, office, date",
    clustering_fields=["state_code", "supervising_officer_external_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_BUILDER.build_and_print()
