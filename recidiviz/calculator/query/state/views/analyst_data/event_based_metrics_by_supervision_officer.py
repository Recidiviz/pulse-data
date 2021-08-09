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
"""View tracking daily positive and backstop metrics, along with demographic attributes, at the officer/district level
for use in impact measurement"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_NAME = (
    "event_based_metrics_by_supervision_officer"
)

EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_DESCRIPTION = "Reports officer-level metrics about caseload health based on counts of events over rolling time scales"

EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_QUERY_TEMPLATE = """
    /*{description}*/
    WITH date_array AS
    (
        SELECT date, EXTRACT(YEAR FROM date) as year, EXTRACT(MONTH FROM date) as month,
        FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), CURRENT_DATE(), INTERVAL 1 DAY)) AS date
    ),
    completions AS
    (
        SELECT 
            state_code, 
            person_id,
            supervising_officer_external_id,
            end_date as completion_date,
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers
        USING (state_code, person_id, end_date)
        WHERE compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
        AND outflow_to_level_1 = 'RELEASE'
        AND end_reason IN ('COMMUTED', 'DISMISSED', 'DISCHARGE', 'EXPIRATION', 'PARDONED')
    ),
    earned_discharge_requests AS
    (
        SELECT
            ed.state_code,
            ed.person_id,
            supervising_officer_external_id,
            ed.request_date,

        FROM `{project_id}.{base_dataset}.state_early_discharge` ed
        /* Join with overlapping session to get supervision type at time of request */
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers
        ON ed.state_code = officers.state_code
            AND ed.person_id = officers.person_id
            AND ed.request_date BETWEEN officers.start_date AND COALESCE(officers.end_date, "9999-01-01")
        WHERE decision_status != 'INVALID'
    ),
    supervision_downgrades AS
    (
        SELECT 
            levels.state_code,
            levels.person_id,
            supervising_officer_external_id,
            levels.start_date as supervision_downgrade_date
        FROM `{project_id}.{analyst_dataset}.supervision_level_sessions_materialized` levels
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers
        ON levels.state_code = officers.state_code
        AND levels.person_id = officers.person_id
        AND levels.start_date BETWEEN officers.start_date AND COALESCE(officers.end_date, CURRENT_DATE())
        WHERE levels.supervision_downgrade > 0
    ),
    violations AS
    (
        SELECT 
            violations.state_code,
            violations.person_id,
            supervising_officer_external_id,
            response_date,
            most_serious_violation_type,
            most_severe_response_decision as response_decision
        FROM `{project_id}.{analyst_dataset}.violations_sessions_materialized` violations
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers
        ON violations.state_code = officers.state_code
        AND violations.person_id = officers.person_id
        AND violations.response_date BETWEEN officers.start_date AND COALESCE(officers.end_date, CURRENT_DATE())
    ),
    revocations AS
    (
        SELECT 
            revocations.state_code,
            revocations.person_id, 
            supervising_officer_external_id, 
            revocation_date, 
            sessions.start_sub_reason AS revocation_type,
            sessions.compartment_level_2, 
            sessions.inflow_from_level_2
        FROM `{project_id}.{analyst_dataset}.revocation_sessions_materialized` revocations
        LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
        USING (state_code, person_id)
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` officers
        ON revocations.person_id = officers.person_id
        AND revocation_date BETWEEN officers.start_date AND DATE_ADD(officers.end_date, INTERVAL 1 DAY)
        WHERE revocations.revocation_session_id = sessions.session_id
    ),
    all_metrics AS
    /* Combines all metrics into a single view for each day in the last 2 years. Null count metrics replaced with zero. */
      (
        SELECT population.state_code, date, population.supervising_officer_external_id, district as primary_district, rolling_window_days,
          DATE_DIFF(date, CURRENT_DATE(), DAY) AS absolute_date,
          COUNT(completions.person_id) AS num_successful_completions,
          COUNT(earned_discharge_requests.person_id) AS num_earned_discharge_requests,
          COUNT(supervision_downgrades.supervision_downgrade_date) AS num_supervision_downgrades,
          COUNTIF(revocations.revocation_type IN ('TECHNICAL', 'ABSCONDED')) AS num_technical_or_absconsion_revocations,
          COUNTIF(revocations.revocation_type IN ('FELONY', 'MISDEMEANOR', 'LAW')) AS num_new_crime_revocations,
          COUNTIF(revocations.compartment_level_2 = 'TREATMENT_IN_PRISON') AS num_rider_revocations,
          COUNTIF(revocations.inflow_from_level_2 = 'PAROLE_BOARD_HOLD') AS num_parole_board_hold_revocations,
          COUNTIF(violations.most_serious_violation_type = 'ABSCONDED') AS num_absconsions,
          COUNT(violations.person_id) AS num_violations,
          supervision_population,
        FROM `{project_id}.{analyst_dataset}.supervision_population_by_officer_daily_windows` population
        JOIN `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` district_officer
          ON district_officer.state_code = population.state_code
          AND district_officer.year = population.year
          AND district_officer.month = population.month
          AND district_officer.officer_external_id = population.supervising_officer_external_id
        LEFT JOIN completions
          ON population.state_code = completions.state_code
          AND population.supervising_officer_external_id = completions.supervising_officer_external_id
          AND population.date = completions.completion_date
        LEFT JOIN earned_discharge_requests
          ON population.state_code = earned_discharge_requests.state_code
          AND population.supervising_officer_external_id = earned_discharge_requests.supervising_officer_external_id
          AND population.date = earned_discharge_requests.request_date
        LEFT JOIN supervision_downgrades
          ON population.state_code = supervision_downgrades.state_code
          AND population.supervising_officer_external_id = supervision_downgrades.supervising_officer_external_id
          AND population.date = supervision_downgrades.supervision_downgrade_date
        LEFT JOIN violations
          ON population.state_code = violations.state_code
          AND population.supervising_officer_external_id = violations.supervising_officer_external_id
          AND population.date = violations.response_date
         LEFT JOIN revocations
          ON population.state_code = revocations.state_code
          AND population.supervising_officer_external_id = revocations.supervising_officer_external_id
          AND population.date = revocations.revocation_date
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
        GROUP BY population.state_code, date, population.supervising_officer_external_id, district, rolling_window_days, supervision_population
      )
    /* Calculates raw metrics and population adjusted counts across rolling windows for all metrics */
    SELECT m1.state_code, m1.date, m1.supervising_officer_external_id, m1.primary_district, m1.rolling_window_days, 
      m1.supervision_population,
      SUM(m2.num_successful_completions) AS num_successful_completions,
      SUM(m2.num_successful_completions)*100/m1.supervision_population AS num_successful_completions_per_100,
      SUM(m2.num_earned_discharge_requests) AS num_earned_discharge_requests,
      SUM(m2.num_earned_discharge_requests)*100/m1.supervision_population AS num_earned_discharge_requests_per_100,
      SUM(m2.num_supervision_downgrades) AS num_supervision_downgrades,
      SUM(m2.num_supervision_downgrades)*100/m1.supervision_population AS num_supervision_downgrades_pop,
      SUM(m2.num_technical_or_absconsion_revocations) AS num_technical_or_absconsion_revocations,
      SUM(m2.num_technical_or_absconsion_revocations)*100/m1.supervision_population AS num_technical_or_absconsion_revocations_per_100,
      SUM(m2.num_new_crime_revocations) AS num_new_crime_revocations,
      SUM(m2.num_new_crime_revocations)*100/m1.supervision_population AS num_new_crime_revocations_per_100,
      SUM(m2.num_rider_revocations) AS num_rider_revocations,
      SUM(m2.num_rider_revocations)*100/m1.supervision_population AS num_rider_revocations_per_100,
      SUM(m2.num_parole_board_hold_revocations) AS num_parole_board_hold_revocations,
      SUM(m2.num_parole_board_hold_revocations)*100/m1.supervision_population AS num_parole_board_hold_revocations_per_100,
      SUM(m2.num_absconsions) AS num_absconsions,
      SUM(m2.num_absconsions)*100/m1.supervision_population AS num_absconsions_per_100,
      SUM(m2.num_violations) AS num_violations,
      SUM(m2.num_violations)*100/m1.supervision_population AS num_violations_per_100,
    FROM all_metrics m1
    /* Self join on the window of dates that fall within the rolling window days period, and calculate average */
    JOIN all_metrics m2
      USING (state_code, supervising_officer_external_id, primary_district, rolling_window_days)
    WHERE m1.date
      BETWEEN m2.date AND DATE_ADD(m2.date, INTERVAL m1.rolling_window_days - 1 DAY)
    AND m1.date >= DATE_ADD(DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), INTERVAL m1.rolling_window_days DAY)
    GROUP BY m1.state_code, m1.date, m1.supervising_officer_external_id, m1.primary_district, m1.rolling_window_days, m1.supervision_population
    """

EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_NAME,
    view_query_template=EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_QUERY_TEMPLATE,
    description=EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_BUILDER.build_and_print()
