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
"""Creates the view builder and view for metrics from case triage."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import (
    EXPERIMENTS_DATASET,
    CASE_TRIAGE_SEGMENT_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
    ANALYST_VIEWS_DATASET,
    PO_REPORT_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION

CASE_TRIAGE_EVENTS_VIEW_NAME = "case_triage_metrics"

CASE_TRIAGE_EVENTS_VIEW_DESCRIPTION = (
    "Officer-day, week, and month-level event metrics for case triage"
)

CASE_TRIAGE_EVENTS_QUERY_TEMPLATE = """
      WITH column_wise AS
     (
      SELECT user_id, EXTRACT(DATE from timestamp) as date_measurement, action_taken, count(*) as counts
        from `{case_triage_metrics_project}.{case_triage_segment}.frontend_person_action_taken`
        group by 1,2,3
     ),
      
      action_set AS (
            --updates that a PO makes on a given date 
            SELECT user_id
            , date_measurement
            , SUM(CASE WHEN action_taken = "FOUND_EMPLOYMENT" THEN 1 ELSE 0 END) AS actions_found_employment
            , SUM(CASE WHEN action_taken = "INFORMATION_DOESNT_MATCH_OMS" THEN 1 ELSE 0 END) AS actions_doesnt_match_oms
            , SUM(CASE WHEN action_taken = "SCHEDULED_FACE_TO_FACE" THEN 1 ELSE 0 END) AS actions_face_to_face
            , SUM(CASE WHEN action_taken = "FILED_REVOCATION_OR_VIOLATION" THEN 1 ELSE 0 END) AS actions_filed_revocation
            , SUM(CASE WHEN action_taken = "COMPLETED_ASSESSMENT" THEN 1 ELSE 0 END) AS actions_completed_assessment
            , SUM(CASE WHEN action_taken = "OTHER_DISMISSAL" THEN 1 ELSE 0 END) AS actions_other_dismissal
            , SUM(CASE WHEN action_taken = "NOT_ON_CASELOAD" THEN 1 ELSE 0 END) AS actions_not_on_caseload
      FROM column_wise 
      GROUP BY user_id, date_measurement
      ),
      
      baseline AS (
          --match main events on page with POs
          SELECT recipients.state_code
                , EXTRACT(DATE FROM metrics.timestamp) AS date
                , district.district
                , recipients.officer_external_id
                , metrics.user_id AS segment_id
                , CASE WHEN COUNT(DISTINCT page.timestamp) > 0 THEN 1 ELSE 0 END AS page_count
                , COUNT(DISTINCT selected.timestamp) AS persons_selected_events
                , COUNT(DISTINCT scrolled.timestamp) AS scrolled_to_bottom_events
                , COUNT(DISTINCT updated.timestamp) AS case_updated_events
          FROM `{case_triage_metrics_project}.{case_triage_segment}.tracks` metrics
          INNER JOIN `{project_id}.{static_reference_tables_dataset}.case_triage_users` recipients
                ON metrics.user_id = recipients.segment_id
          LEFT JOIN `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` district
                ON district.officer_external_id = recipients.officer_external_id
                AND district.state_code = recipients.state_code 
                AND EXTRACT(MONTH FROM metrics.timestamp) = district.month
                AND EXTRACT(YEAR FROM metrics.timestamp) = district.year
           LEFT JOIN `{case_triage_metrics_project}.{case_triage_segment}.pages` page
                ON page.user_id = recipients.segment_id
           LEFT JOIN `{case_triage_metrics_project}.{case_triage_segment}.frontend_person_selected` selected 
                ON selected.user_id = recipients.segment_id
                AND EXTRACT(DATE FROM selected.timestamp) = EXTRACT(DATE FROM metrics.timestamp)
          LEFT JOIN `{case_triage_metrics_project}.{case_triage_segment}.frontend_scrolled_to_bottom` scrolled
                ON scrolled.user_id = recipients.segment_id
                AND EXTRACT(DATE FROM scrolled.timestamp) = EXTRACT(DATE FROM metrics.timestamp)
          LEFT JOIN `{case_triage_metrics_project}.{case_triage_segment}.frontend_person_case_updated` updated
                ON updated.user_id = recipients.segment_id
                AND EXTRACT(DATE FROM updated.timestamp) = EXTRACT(DATE FROM metrics.timestamp)
    
          GROUP BY state_code, date, district, officer_external_id, segment_id
      ),

      one_rolling_window AS (
          --loop which options are being updated
          SELECT state_code
                , date
                , district
                , officer_external_id
                , segment_id
                , page_count
                , persons_selected_events
                , scrolled_to_bottom_events
                , case_updated_events
                , IFNULL(actions_found_employment, 0) AS actions_found_employment
                , IFNULL(actions_face_to_face, 0) AS actions_face_to_face
                , IFNULL(actions_completed_assessment, 0) AS actions_completed_assessment
                , IFNULL(actions_filed_revocation, 0) AS actions_filed_revocation
                , IFNULL(actions_doesnt_match_oms, 0) AS actions_doesnt_match_oms
                , IFNULL(actions_other_dismissal, 0) AS actions_other_dismissal
                , IFNULL(actions_not_on_caseload, 0) AS actions_not_on_caseload
                , CASE WHEN (actions_filed_revocation + actions_doesnt_match_oms + actions_other_dismissal + actions_not_on_caseload > 0) 
                            THEN 1 ELSE 0 END AS actions_any_feedback_given
          FROM baseline
          LEFT JOIN action_set 
                ON action_set.user_id = baseline.segment_id
                AND action_set.date_measurement = baseline.date
          ORDER BY officer_external_id, date
      ),
      
      -- number of persons under supervision of a PO grouped by supervision levels and types
      officer_caseload AS (
          WITH unnested_level AS (
            SELECT state_code
                  , person_id
                  , supervision_level
                  , start_date
                  , end_date
                  , date_of_supervision 
            FROM `{project_id}.{analyst_dataset}.supervision_level_sessions_materialized` AS level, 
            UNNEST(GENERATE_DATE_ARRAY(start_date, COALESCE(end_date, CURRENT_DATE()))) AS date_of_supervision 
      ),
      
      unnested_level_officer AS (
        SELECT state_code
            , person_id
            , supervising_officer_external_id
            , start_date
            , end_date
            , date_of_supervision
        FROM `{project_id}.{analyst_dataset}.supervision_officer_sessions_materialized` AS officer,
        UNNEST(GENERATE_DATE_ARRAY(start_date, COALESCE(end_date, CURRENT_DATE()))) AS date_of_supervision 
      ),

      supervision_by_day AS (
        SELECT officer.state_code
            , officer.supervising_officer_external_id
            , officer.person_id
            , officer.start_date
            , officer.end_date
            , officer.date_of_supervision
            , level.supervision_level
        FROM unnested_level_officer officer
        JOIN unnested_level level
            ON officer.date_of_supervision = level.date_of_supervision
            AND officer.state_code = level.state_code
            AND officer.person_id = level.person_id
      ),

      officers_and_client AS (
        SELECT officer.state_code
            , officer.person_id
            , officer.supervising_officer_external_id
            , officer.date_of_supervision 
            , officer.supervision_level
            , compart.compartment_level_2
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` compart
        RIGHT JOIN supervision_by_day officer
            ON compart.state_code = officer.state_code
            AND compart.person_id = officer.person_id
            AND officer.date_of_supervision >= compart.start_date
            AND officer.date_of_supervision <= COALESCE(compart.end_date, CURRENT_DATE())
        GROUP BY state_code, supervising_officer_external_id, date_of_supervision, supervision_level, compartment_level_2, person_id
      )

      SELECT state_code
             , supervising_officer_external_id AS officer_external_id
             , date_of_supervision AS date
             , COUNTIF(compartment_level_2 = "PROBATION") AS caseload_probation
             , COUNTIF(compartment_level_2 = "PAROLE") AS caseload_parole
             , COUNTIF(compartment_level_2 = "DUAL") AS caseload_dual
             , COUNTIF(supervision_level = "HIGH" OR supervision_level = "MAXIMUM") AS caseload_high_supervision
             , COUNTIF(supervision_level = "MEDIUM") AS caseload_medium_supervision
             , COUNTIF(supervision_level = "MINIMUM") AS caseload_low_supervision
      FROM officers_and_client
      GROUP BY state_code, supervising_officer_external_id, date_of_supervision
      ),
      
      --employment periods of persons under supervision
      employ AS (
          SELECT state_code
          , person_external_id
          , recorded_start_date
          -- verify that overlapping employment periods (new start date, but null end date on earlier emplyoment) 
          -- is due to missing data and not multiple employers/jobs
          , IF((recorded_end_date IS NULL AND (recorded_start_date != MAX(recorded_start_date) OVER (PARTITION BY person_external_id) )),
            LEAD(recorded_start_date) OVER(PARTITION BY person_external_id ORDER BY recorded_start_date) - 1,
            recorded_end_date) AS recorded_end
          FROM `{project_id}.case_triage.employment_periods` 
          GROUP BY state_code, person_external_id, recorded_start_date, recorded_end_date
      ),
      
      correct_levels_select AS (
          SELECT * 
          FROM (
            SELECT metrics.state_code
             , selected.person_external_id
             , EXTRACT(DATE FROM selected.timestamp) as date
             , ROW_NUMBER() OVER (PARTITION BY selected.person_external_id, EXTRACT(DATE FROM selected.timestamp) ORDER BY date_of_supervision desc) AS rn
             , metrics.supervision_type
             , metrics.supervision_level 
             FROM `{case_triage_metrics_project}.{case_triage_segment}.frontend_person_selected` selected 
             LEFT JOIN `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_population_metrics_materialized` metrics
                ON selected.person_external_id = metrics.person_external_id 
                AND metrics.date_of_supervision < EXTRACT(DATE FROM selected.timestamp)
             ORDER BY person_external_id
          )this 
          WHERE rn = 1
      ),

      --match the timing of click events of a PO with whether a person was employed 
      selected_employment AS (
          SELECT selected.person_external_id
                , EXTRACT(date from selected.timestamp) AS date_measurement
                , selected.user_id
                , employ.recorded_start_date
                , employ.recorded_end AS recorded_end_date
                , correct_levels_select.supervision_type
                , correct_levels_select.supervision_level
            --if they have employment during the timestamp period 
          FROM `{case_triage_metrics_project}.{case_triage_segment}.frontend_person_selected` selected 
          LEFT JOIN employ
                  ON employ.person_external_id = selected.person_external_id 
                  AND employ.recorded_start_date <= EXTRACT(date from selected.timestamp) 
                  AND COALESCE(employ.recorded_end, CURRENT_DATE()) >= EXTRACT(date from selected.timestamp)
          LEFT JOIN correct_levels_select
             ON selected.person_external_id = correct_levels_select.person_external_id 
             AND correct_levels_select.date = EXTRACT(DATE FROM selected.timestamp)
      ),
     
      client_narrow_select AS (
          SELECT selected.person_external_id
                 , date_measurement  
                 , selected.user_id
                 , CASE WHEN (DATE_DIFF(CURRENT_DATE(), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 30) THEN 1 ELSE 0 END AS months_last_face_to_face
                 , CASE WHEN (DATE_DIFF(CURRENT_DATE(), CAST(client.most_recent_face_to_face_date AS DATE), DAY) < 30 
                   AND DATE_DIFF(CURRENT_DATE(), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 0)  
                   THEN 1 ELSE 0 END AS weeks_last_face_to_face
                 , client.assessment_score
                 , selected.recorded_start_date
                 , selected.supervision_level
                 , selected.supervision_type
          FROM selected_employment selected
          LEFT JOIN `{project_id}.case_triage.etl_clients` client
            ON client.person_external_id = selected.person_external_id
      ),
      
      --the number of click events of a PO for people with different attributes of supervision 
      selected_given AS (
        SELECT client_narrow_select.user_id        
            , client_narrow_select.date_measurement
            , SUM(IF(recorded_start_date IS NULL, 1, 0)) AS persons_selected_given_no_employment_indicated
            , SUM(IF(assessment_score IS NULL, 1, 0)) AS persons_selected_given_no_assessment_indicated
            , SUM(IF(months_last_face_to_face = 1, 1, 0)) AS persons_selected_given_months_last_face_to_face
            , SUM(IF(weeks_last_face_to_face = 1, 1, 0)) AS persons_selected_given_weeks_last_face_to_face
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_type, "PROBATION")) > 0 AS INT64)) AS persons_selected_given_probation
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_type, "PAROLE")) > 0 AS INT64)) AS persons_selected_given_parole
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_type, "DUAL")) > 0 AS INT64)) AS persons_selected_given_dual
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "HIGH|MAXIMUM")) > 0 AS INT64)) AS persons_selected_given_high_supervision
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "MEDIUM")) > 0 AS INT64)) AS persons_selected_given_medium_supervision
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "MINIMUM")) > 0 AS INT64)) AS persons_selected_given_low_supervision
        FROM client_narrow_select
        GROUP BY user_id, date_measurement
      ),
          
      correct_levels_update AS (
          SELECT * 
          FROM (
            SELECT 
             updated.person_external_id,
             EXTRACT(DATE FROM updated.timestamp) as date,
             ROW_NUMBER() OVER (PARTITION BY updated.person_external_id, 
                EXTRACT(DATE FROM updated.timestamp) ORDER BY date_of_supervision desc) AS rn,
             metrics.supervision_type, 
             metrics.supervision_level, 
             FROM `{case_triage_metrics_project}.{case_triage_segment}.frontend_person_case_updated` updated 
             LEFT JOIN `{project_id}.{dataflow_metrics_materialized_dataset}.most_recent_supervision_population_metrics_materialized` metrics
                ON updated.person_external_id = metrics.person_external_id 
                AND metrics.date_of_supervision < EXTRACT(DATE FROM updated.timestamp)
             ORDER BY person_external_id
          )this 
          WHERE rn = 1
      ),

     updated_employment AS (
          SELECT employ.state_code
                , updated.person_external_id
                , EXTRACT(date from updated.timestamp) AS date_measurement
                , updated.user_id
                , employ.recorded_start_date
                , employ.recorded_end AS recorded_end_date
                , correct_levels_update.supervision_type
                , correct_levels_update.supervision_level
            --if they have employment during the timestmap period 
          FROM `{case_triage_metrics_project}.{case_triage_segment}.frontend_person_case_updated` updated
          LEFT JOIN employ
            ON employ.person_external_id = updated.person_external_id 
            AND employ.recorded_start_date <= EXTRACT(date from updated.timestamp) 
            AND COALESCE(employ.recorded_end, CURRENT_DATE()) >= EXTRACT(date from updated.timestamp)
          LEFT JOIN correct_levels_update
            ON updated.person_external_id = correct_levels_update.person_external_id 
            AND correct_levels_update.date = EXTRACT(DATE FROM updated.timestamp)
      ),

      client_narrow_update AS (
         SELECT updated.person_external_id
             , updated.date_measurement 
             , updated.user_id
             , CASE WHEN (DATE_DIFF(CURRENT_DATE(), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 30) THEN 1 ELSE 0 END AS months_last_face_to_face
             , CASE WHEN (DATE_DIFF(CURRENT_DATE(), CAST(client.most_recent_face_to_face_date AS DATE), DAY) < 30 
              AND DATE_DIFF(CURRENT_DATE(), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 0)  THEN 1 ELSE 0 END AS weeks_last_face_to_face
             , client.assessment_score
             , updated.recorded_start_date
             , updated.supervision_level
             , updated.supervision_type
         FROM updated_employment updated
         LEFT JOIN `{project_id}.case_triage.etl_clients` client
            ON client.person_external_id = updated.person_external_id
      ),

      updated_given AS (
        SELECT client_narrow_update.user_id        
            , client_narrow_update.date_measurement
            , COUNTIF(recorded_start_date IS NULL) AS persons_updated_given_no_employment_indicated
            , COUNTIF(assessment_score IS NULL) AS persons_updated_given_no_assessment_indicated
            , COUNTIF(months_last_face_to_face = 1) AS persons_updated_given_months_last_face_to_face
            , COUNTIF(weeks_last_face_to_face = 1) AS persons_updated_given_weeks_last_face_to_face
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_type, "PROBATION")) > 0 AS INT64)) AS persons_updated_given_probation
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_type, "PAROLE")) > 0 AS INT64)) AS persons_updated_given_parole
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_type, "DUAL")) > 0 AS INT64)) AS persons_updated_given_dual
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "HIGH|MAXIMUM")) > 0 AS INT64)) AS persons_updated_given_high_supervision
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "MEDIUM")) > 0 AS INT64)) AS persons_updated_given_medium_supervision
            , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "MINIMUM")) > 0 AS INT64)) AS persons_updated_given_low_supervision
        FROM client_narrow_update
        GROUP BY user_id, date_measurement
      )
      
      --combine base user data, aggregated PO caseload, click events, update events, 
      SELECT *  EXCEPT(user_id, date_measurement, segment_id)
      FROM one_rolling_window 
      LEFT JOIN officer_caseload
            USING(officer_external_id, date, state_code)
      LEFT JOIN selected_given
            ON selected_given.user_id = one_rolling_window.segment_id 
            AND selected_given.date_measurement = one_rolling_window.date
      LEFT JOIN updated_given
            ON updated_given.user_id = one_rolling_window.segment_id 
            AND updated_given.date_measurement = one_rolling_window.date
      ORDER BY date, district, officer_external_id

      --for adding rolling windows later if desired
      /*
      total AS (
      SELECT * 
            , 1 AS rolling_window_days 
      FROM one_rolling_window

      UNION ALL
      
      SELECT * 
            , 7 AS rolling_window_days 
      FROM one_rolling_window

      UNION ALL

      SELECT * 
            , 30 AS rolling_window_days 
      FROM one_rolling_window
      ORDER BY
            officer_external_id, date, rolling_window_days 
      )

      SELECT m1.state_code, m1.date, m1.district, m1.officer_external_id, m1.rolling_window_days, m1.segment_id
            , CASE WHEN SUM(m2.page_count) > 0 THEN 1 ELSE 0 END AS page_count
            , SUM(m2.person_selected_events) AS person_selected_events
            , SUM(m2.scrolled_to_bottom_events) AS scrolled_to_bottom_events
            , SUM(m2.case_updated_events) AS case_updated_events
            , SUM(m2.actions_found_employment) AS actions_found_employment
            , SUM(m2.actions_filed_revocation) AS actions_filed_revocation
            , SUM(m2.actions_doesnt_match_oms) AS actions_doesnt_match_oms
            , SUM(m2.actions_face_to_face) AS actions_face_to_face
            , SUM(m2.actions_completed_assessment) AS actions_completed_assessment
            , SUM(m2.actions_other_dismissal) AS actions_other_dismissal
            , SUM(m2.actions_not_on_caseload) AS actions_not_on_caseload
      FROM total m1
      JOIN total m2
            USING (state_code, district, officer_external_id, rolling_window_days, segment_id)
      WHERE m1.date
            BETWEEN m2.date AND DATE_ADD(m2.date, INTERVAL m1.rolling_window_days - 1 DAY) 
      GROUP BY m1.state_code, m1.date, m1.district, m1.officer_external_id, m1.rolling_window_days, m1.segment_id
    
      */
"""

CASE_TRIAGE_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=CASE_TRIAGE_EVENTS_VIEW_NAME,
    view_query_template=CASE_TRIAGE_EVENTS_QUERY_TEMPLATE,
    description=CASE_TRIAGE_EVENTS_VIEW_DESCRIPTION,
    dataflow_metrics_materialized_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    po_report_dataset=PO_REPORT_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    case_triage_segment=CASE_TRIAGE_SEGMENT_DATASET,
    case_triage_metrics_project=GCP_PROJECT_PRODUCTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_TRIAGE_EVENTS_VIEW_BUILDER.build_and_print()
