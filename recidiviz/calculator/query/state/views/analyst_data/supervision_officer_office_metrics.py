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
        date BETWEEN start_date AND IFNULL(end_date, CURRENT_DATE("US/Eastern"))
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
        a.supervision_level,
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
        `{project_id}.{sessions_dataset}.violation_responses_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON 
        a.state_code = b.state_code 
        AND a.person_id = b.person_id 
        AND a.response_date = b.date
)
# Absconsion or bench warrant periods
, absconsions_bench_warrants AS (
    SELECT DISTINCT * 
    FROM
    (
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
            AND a.start_date = b.date
        WHERE
            compartment_level_1 IN ("SUPERVISION", "SUPERVISION_OUT_OF_STATE")
            AND compartment_level_2 IN ("ABSCONSION", "BENCH_WARRANT")
        UNION ALL
        SELECT
            a.state_code,
            a.person_id,
            supervising_officer_external_id,
            district,
            office,
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
            supervision_level IN ("ABSCONDED", "WARRANT")
    )
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
            PARTITION BY person_id, supervising_officer_external_id, district, office
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
    GROUP BY 1, 2, 3, 4, 5, 6
)
, lsir_assessments AS (
    SELECT DISTINCT
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        date,
        a.assessment_score - LAG(a.assessment_score) OVER (
            PARTITION BY a.state_code, a.person_id, supervising_officer_external_id, district, office ORDER BY date
        ) AS lsir_score_change,
    FROM `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON
        a.state_code = b.state_code
        AND a.person_id = b.person_id
        AND a.assessment_date = b.date
    WHERE a.assessment_type = "LSIR"
)
, contacts AS (
    SELECT
        a.state_code,
        a.person_id,
        supervising_officer_external_id,
        district,
        office,
        date,
        LOGICAL_OR(
            location = "RESIDENCE"
            AND contact_type IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")
            AND status = "COMPLETED"
        ) AS any_home_visit_contact,
        LOGICAL_OR(
            contact_type IN ("DIRECT", "BOTH_COLLATERAL_AND_DIRECT")
            AND status = "COMPLETED"
        ) AS any_face_to_face_contact,
        LOGICAL_OR(status = "COMPLETED") AS any_completed_contact,
        LOGICAL_OR(status = "ATTEMPTED") AS any_attempted_contact,
    FROM `{project_id}.{base_dataset}.state_supervision_contact` a
    INNER JOIN 
        officer_office_sessions_unnested b
    ON
        a.state_code = b.state_code
        AND a.person_id = b.person_id
        AND a.contact_date = b.date
    GROUP BY 1, 2, 3, 4, 5, 6
)
# The following cte creates spans of contact dates and the subsequent contact date,
# to help us identify the most recent completed contact in the `caseload_attributes` cte.
, contacts_completed_sessionized AS (
    SELECT
        state_code, 
        person_id,
        contact_date,
        DATE_SUB(
            LEAD(contact_date) OVER (
                PARTITION BY person_id
                ORDER BY contact_date
            ), INTERVAL 1 DAY
        ) AS next_contact_date
    FROM (
        SELECT DISTINCT
            state_code,
            person_id,
            contact_date
        FROM `{project_id}.{base_dataset}.state_supervision_contact`
        WHERE status = "COMPLETED"
    )
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
                    IFNULL(c.end_date, CURRENT_DATE("US/Eastern")), 
                    DATE_ADD(a.start_date, INTERVAL 365 DAY)
                ), c.start_date, DAY
            )
        ) OVER (
            PARTITION BY sss.supervision_super_session_id, a.state_code, a.person_id, 
            supervising_officer_external_id, district, office, date, 
            d.employment_status_start_date, e.employer_name, e.employment_start_date
        ) AS days_incarcerated_1yr,

        # Number of days employed within a year of first assignment to officer
        SUM(
            CASE WHEN is_employed
            THEN DATE_DIFF(
                LEAST(
                    IFNULL(d.employment_status_end_date, CURRENT_DATE("US/Eastern")), 
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
            c.start_date, e.employer_name, e.employment_start_date
        ) AS days_employed_1yr,

        # Number of days at the longest stint with a consistent employer within a year 
        # of first assignment to officer
        MAX(
            DATE_DIFF(
                LEAST(
                    IFNULL(e.employment_end_date, CURRENT_DATE("US/Eastern")), 
                    DATE_ADD(a.start_date, INTERVAL 365 DAY)
                ), 
                GREATEST(
                    e.employment_start_date, 
                    a.start_date
                ), DAY
            )
        ) OVER (
            PARTITION BY sss.supervision_super_session_id, a.state_code, a.person_id, 
            supervising_officer_external_id, district, office, date, 
            c.start_date, d.employment_status_start_date
        ) AS max_days_stable_employment_1yr,

        # Number of unique employers within a year of first assignment to officer
        COUNT(DISTINCT employer_name) OVER (
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
    (
        # Deduplicate to a single employment period for each employment period start,
        # taking the longest employment period starting on that date.
        SELECT
            state_code,
            person_id,
            employer_name,
            employment_start_date,
            NULLIF(
                MAX(IFNULL(employment_end_date, "9999-01-01")) 
                OVER (PARTITION BY state_code, person_id, employer_name, employment_start_date)
            , "9999-01-01") AS employment_end_date,
        FROM
            `{project_id}.{sessions_dataset}.employment_periods_preprocessed_materialized`
        WHERE 
            is_unemployed = FALSE
        QUALIFY 
            ROW_NUMBER() OVER (PARTITION BY state_code, person_id, employer_name, employment_start_date) = 1
    ) e
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
        AVG(score_initial.assessment_score) AS avg_lsir_score_at_assignment,
        AVG(score.assessment_score) AS avg_lsir_score,
        AVG(DATE_DIFF(date, score.assessment_date, DAY)) AS avg_days_since_latest_lsir,
        COUNT(DISTINCT IF(score.assessment_score IS NULL, score.person_id, NULL)) 
            AS caseload_no_lsir_score,
        COUNT(DISTINCT IF(score.assessment_level IN ("LOW", "LOW_MEDIUM", "MINIMUM"), score.person_id,
            NULL)) AS caseload_low_risk_level,
        COUNT(DISTINCT IF(score.assessment_level IN ("HIGH", "MEDIUM_HIGH", "MAXIMUM", "VERY_HIGH"), 
            score.person_id, NULL)) AS caseload_high_risk_level,
        COUNT(DISTINCT IF(score.assessment_level IS NULL OR score.assessment_level LIKE "%UNKNOWN",
            score.person_id, NULL)) AS caseload_unknown_risk_level,
        AVG(DATE_DIFF(date, birthdate, DAY) / 365.25) AS avg_age,
        COUNT(DISTINCT IF(is_employed, e.person_id, NULL)) AS caseload_is_employed,
        AVG(DATE_DIFF(date, f.contact_date, DAY)) AS avg_days_since_latest_completed_contact,
        COUNT(DISTINCT IF(COALESCE(DATE_DIFF(date, f.contact_date, DAY) > 365, TRUE), f.person_id, NULL)) 
            AS caseload_no_completed_contact_past_1yr,
    FROM
        date_array d
    INNER JOIN
        `{project_id}.{sessions_dataset}.supervision_officer_office_sessions_materialized` b
    ON
        d.date BETWEEN b.start_date AND IFNULL(b.end_date, "9999-01-01")
    INNER JOIN
        `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` sss
    ON
        sss.state_code = b.state_code
        AND sss.person_id = b.person_id
        AND b.start_date BETWEEN sss.start_date AND IFNULL(sss.end_date, "9999-01-01")
    INNER JOIN
        `{project_id}.{sessions_dataset}.system_sessions_materialized` sys
    ON
        sys.state_code = b.state_code
        AND sys.person_id = b.person_id
        AND b.start_date BETWEEN sys.start_date AND IFNULL(sys.end_date, "9999-01-01")
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
        AND date BETWEEN score.assessment_date AND IFNULL(score.score_end_date, "9999-01-01")
        AND score.assessment_type = "LSIR"
        # Only consider assessments occurring within the same system session as date of evaluation
        AND score.assessment_date BETWEEN sys.start_date AND IFNULL(sys.end_date, "9999-01-01")
    # Get assessment scores of caseload at time of assignment
    LEFT JOIN
        `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` score_initial
    ON
        score_initial.state_code = c.state_code
        AND score_initial.person_id = c.person_id
        AND b.start_date BETWEEN score_initial.assessment_date AND IFNULL(score_initial.score_end_date, "9999-01-01")
        AND score_initial.assessment_type = "LSIR"
    LEFT JOIN
        `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized` e
    ON 
        e.state_code = c.state_code
        AND e.person_id = c.person_id
        AND date BETWEEN employment_status_start_date AND 
            IFNULL(employment_status_end_date, "9999-01-01")
    LEFT JOIN
        contacts_completed_sessionized f
    ON
        f.state_code = c.state_code
        AND f.person_id = c.person_id
        AND date BETWEEN f.contact_date AND IFNULL(f.next_contact_date, "9999-01-01")
        # Only consider completed contacts occurring within the same supervision super session as date of evaluation
        AND f.contact_date BETWEEN sss.start_date AND IFNULL(sss.end_date, "9999-01-01")
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

# Aggregate all event metrics. All aggregation functions should account for duplicated rows (i.e., using COUNT DISTINCT).
,
event_metrics_agg AS (
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
        COUNT(DISTINCT IF(
            supervision_level_changes.supervision_level = "LIMITED",
            supervision_level_changes.person_id, NULL)) AS supervision_downgrades_to_limited,

        # violations (max one per person per day per type)
        # TODO(#13180): Rename violation metrics to violation _response_ metrics 
        COUNT(DISTINCT violations.person_id) AS violations,
        COUNT(DISTINCT IF(violations.most_serious_violation_type IN ("ESCAPED", 
            "ABSCONDED"), violations.person_id, NULL)) AS violations_absconded,
        COUNT(DISTINCT IF(violations.most_serious_violation_type IN ("FELONY", "LAW", 
            "MISDEMEANOR", "MUNICIPAL"), violations.person_id, NULL)) AS violations_legal,
        COUNT(DISTINCT IF(violations.most_serious_violation_type = "TECHNICAL",
            violations.person_id, NULL)) AS violations_technical,
        
        # absconsions/bench warrants
        COUNT(DISTINCT absconsions_bench_warrants.person_id) AS absconsions_bench_warrants,

        # incarcerations
        COUNT(DISTINCT IF(incarcerations.temporary_flag, incarcerations.person_id, 
            NULL)) AS incarcerations_temporary,
        COUNT(DISTINCT incarcerations.person_id) AS incarcerations_all,

        # drug screens
        COUNT(DISTINCT IF(drug_screens.is_positive_result, drug_screens.person_id, 
            NULL)) AS drug_screens_positive,
        COUNT(DISTINCT drug_screens.person_id) AS drug_screens_all,
        
        # risk assessments
        COUNT(DISTINCT lsir_assessments.person_id) AS lsir_assessments,
        COUNT(DISTINCT IF(lsir_assessments.lsir_score_change > 0, lsir_assessments.person_id, NULL)) AS lsir_risk_increase,
        COUNT(DISTINCT IF(lsir_assessments.lsir_score_change < 0, lsir_assessments.person_id, NULL)) AS lsir_risk_decrease,
        AVG(lsir_score_change) AS lsir_score_change,

        # contacts
        COUNT(DISTINCT IF(contacts.any_completed_contact, contacts.person_id, NULL)) AS contacts_completed,
        COUNT(DISTINCT IF(contacts.any_attempted_contact, contacts.person_id, NULL)) AS contacts_attempted,
        COUNT(DISTINCT IF(contacts.any_face_to_face_contact, contacts.person_id, NULL)) AS contacts_face_to_face,
        COUNT(DISTINCT IF(contacts.any_home_visit_contact, contacts.person_id, NULL)) AS contacts_home_visit,

        # employment starts (transitions from unemployed to employed)
        COUNT(DISTINCT IF(is_employed, employment_changes.person_id, 
            NULL)) AS gained_employment,

        # employment ends (transitions from employed to unemployed)
        COUNT(DISTINCT IF(NOT is_employed, employment_changes.person_id, 
            NULL)) AS lost_employment,

    FROM caseload_attributes
    LEFT JOIN successful_completions USING({join_columns})
    LEFT JOIN earned_discharge_requests USING({join_columns})
    LEFT JOIN supervision_level_changes USING({join_columns})
    LEFT JOIN violations USING({join_columns})
    LEFT JOIN absconsions_bench_warrants USING({join_columns})
    LEFT JOIN incarcerations USING({join_columns})
    LEFT JOIN employment_changes USING({join_columns})
    LEFT JOIN drug_screens USING({join_columns})
    LEFT JOIN lsir_assessments USING({join_columns})
    LEFT JOIN contacts USING({join_columns})
    GROUP BY 1, 2, 3, 4, 5
)

# Aggregate all window metrics
,
window_metrics_agg AS
(
    SELECT
        # index columns
        {join_columns},
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
    LEFT JOIN window_metrics USING({join_columns})
    GROUP BY 1, 2, 3, 4, 5
)


# Join caseload attributes, event metrics, and window metrics
SELECT
    *
FROM
    caseload_attributes
LEFT JOIN event_metrics_agg
USING ({join_columns})
LEFT JOIN window_metrics_agg
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
