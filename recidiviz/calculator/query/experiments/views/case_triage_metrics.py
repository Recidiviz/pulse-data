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
    CASE_TRIAGE_SEGMENT_DATASET,
    EXPERIMENTS_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    PO_REPORT_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.case_triage.views.dataset_config import (
    VIEWS_DATASET as CASE_TRIAGE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASE_TRIAGE_EVENTS_VIEW_NAME = "case_triage_metrics"

CASE_TRIAGE_EVENTS_VIEW_DESCRIPTION = "Case triage event metrics per officer and day"

CASE_TRIAGE_EVENTS_QUERY_TEMPLATE = """

    WITH updates_per_officer_day AS (
        SELECT state_code
            , officer_external_id
            , district
            , segment_id
            , feedback_date
            , COUNT(*) AS case_updated_events
        FROM `{project_id}.{experiments_dataset}.case_triage_feedback_actions`
        GROUP BY state_code, officer_external_id, district, segment_id, feedback_date
    ),
    baseline AS (
        --match main events on page with POs
        SELECT recipients.state_code
              , EXTRACT(DATE FROM tracks.timestamp AT TIME ZONE 'US/Eastern') AS date
              , district.district
              , recipients.officer_external_id
              , tracks.user_id AS segment_id
              , CASE WHEN COUNT(DISTINCT page.timestamp) > 0 THEN 1 ELSE 0 END AS page_count
              , COUNT(DISTINCT selected.timestamp) AS persons_selected_events
              , COUNT(DISTINCT scrolled.timestamp) AS scrolled_to_bottom_events
              , COALESCE(MAX(case_updated_events), 0) AS case_updated_events
        FROM `{project_id}.{case_triage_segment_dataset}.tracks` tracks
        INNER JOIN `{project_id}.{static_reference_tables_dataset}.case_triage_users` recipients
            ON tracks.user_id = recipients.segment_id
        LEFT JOIN `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` district
            ON district.officer_external_id = recipients.officer_external_id
            AND district.state_code = recipients.state_code
            AND EXTRACT(MONTH FROM tracks.timestamp) = district.month
            AND EXTRACT(YEAR FROM tracks.timestamp) = district.year
        LEFT JOIN `{project_id}.{case_triage_segment_dataset}.pages` page
            ON tracks.user_id = page.user_id
            AND EXTRACT(DATE FROM tracks.timestamp AT TIME ZONE 'US/Eastern') = EXTRACT(DATE FROM page.timestamp AT TIME ZONE 'US/Eastern')
        LEFT JOIN `{project_id}.{case_triage_segment_dataset}.frontend_person_selected` selected
            ON tracks.user_id = selected.user_id
            AND EXTRACT(DATE FROM tracks.timestamp AT TIME ZONE 'US/Eastern') = EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern')
        LEFT JOIN `{project_id}.{case_triage_segment_dataset}.frontend_scrolled_to_bottom` scrolled
            ON page.user_id = scrolled.user_id
            AND EXTRACT(DATE FROM tracks.timestamp AT TIME ZONE 'US/Eastern') = EXTRACT(DATE FROM scrolled.timestamp AT TIME ZONE 'US/Eastern')
        LEFT JOIN updates_per_officer_day updated
            ON tracks.user_id = updated.segment_id
            AND EXTRACT(DATE FROM tracks.timestamp AT TIME ZONE 'US/Eastern') = updated.feedback_date

        GROUP BY state_code, date, district, officer_external_id, segment_id
    ),

    -- number of persons under supervision per PO and day grouped by supervision levels and types
    officer_caseload AS (
        SELECT
            officer.state_code,
            officer.supervising_officer_external_id AS officer_external_id,
            date_of_supervision AS date,
            COUNTIF(session.compartment_level_2 = "PROBATION") AS caseload_probation,
            COUNTIF(session.compartment_level_2 = "PAROLE") AS caseload_parole,
            COUNTIF(session.compartment_level_2 = "DUAL") AS caseload_dual,
            COUNTIF(level.supervision_level IN ("HIGH", "MAXIMUM")) AS caseload_high_supervision,
            COUNTIF(level.supervision_level = "MEDIUM") AS caseload_medium_supervision,
            COUNTIF(level.supervision_level = "MINIMUM") AS caseload_low_supervision,
            COUNTIF(COALESCE(level.supervision_level, "INTERNAL_UNKNOWN") = "INTERNAL_UNKNOWN") AS caseload_unknown_supervision_level,
        FROM
            `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` AS officer,
        UNNEST(
            -- Unnest supervision officer sessions that were open in 2021 or later
            GENERATE_DATE_ARRAY(
                GREATEST(officer.start_date, DATE("2021-01-01")),
                COALESCE(officer.end_date, CURRENT_DATE)
            )) AS date_of_supervision
        INNER JOIN
            `{project_id}.{sessions_dataset}.compartment_sessions_materialized` session
            ON session.state_code = officer.state_code
            AND session.person_id = officer.person_id
            AND date_of_supervision BETWEEN session.start_date
                AND COALESCE(session.end_date, CURRENT_DATE)
        LEFT JOIN
            `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` AS level
            ON officer.state_code = level.state_code
            AND officer.person_id = level.person_id
            AND date_of_supervision BETWEEN level.start_date
                AND COALESCE(level.end_date, CURRENT_DATE)
        -- Drop clients that are not on a PO caseload
        WHERE officer.supervising_officer_external_id IS NOT NULL
        GROUP BY 1, 2, 3
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
        FROM `{project_id}.{case_triage_dataset}.employment_periods_materialized`
        GROUP BY state_code, person_external_id, recorded_start_date, recorded_end_date
    ),

    correct_levels_select AS (
        SELECT DISTINCT
           compartment_metrics.state_code
           , selected.person_external_id
           , EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern') as date
           , compartment_metrics.compartment_level_2 AS supervision_type
           , COALESCE(supervision_level_metrics.supervision_level, 'INTERNAL_UNKNOWN') AS supervision_level
        FROM `{project_id}.{case_triage_segment_dataset}.frontend_person_selected` selected
        INNER JOIN `{project_id}.{static_reference_tables_dataset}.case_triage_users` recipients
            ON selected.user_id = recipients.segment_id
        LEFT JOIN `{project_id}.{state_base_dataset}.state_person_external_id` client
            ON selected.person_external_id = client.external_id
            AND recipients.state_code = client.state_code
        LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` supervision_level_metrics
            ON client.state_code = supervision_level_metrics.state_code
            AND client.person_id = supervision_level_metrics.person_id
            AND EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern')
                BETWEEN supervision_level_metrics.start_date AND COALESCE(supervision_level_metrics.end_date, CURRENT_DATE('US/Eastern'))
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` compartment_metrics
            ON client.state_code = compartment_metrics.state_code
            AND client.person_id = compartment_metrics.person_id
            AND EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern')
                BETWEEN compartment_metrics.start_date AND COALESCE(compartment_metrics.end_date, CURRENT_DATE('US/Eastern'))
    ),

    --match the timing of click events of a PO with whether a person was employed
    selected_employment AS (
        SELECT selected.person_external_id
             , EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern') AS date
             , selected.user_id AS segment_id
             , employ.recorded_start_date
             , employ.recorded_end AS recorded_end_date
             , correct_levels_select.supervision_type
             , correct_levels_select.supervision_level
             --if they have employment during the timestamp period
        FROM `{project_id}.{case_triage_segment_dataset}.frontend_person_selected` selected
        LEFT JOIN employ
            ON employ.person_external_id = selected.person_external_id
            AND employ.recorded_start_date <= EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern')
            AND COALESCE(employ.recorded_end, CURRENT_DATE('US/Eastern')) >= EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern')
        LEFT JOIN correct_levels_select
            ON selected.person_external_id = correct_levels_select.person_external_id
            AND correct_levels_select.date = EXTRACT(DATE FROM selected.timestamp AT TIME ZONE 'US/Eastern')
    ),

    client_narrow_select AS (
        SELECT selected.person_external_id
             , date
             , selected.segment_id
             , CASE WHEN (DATE_DIFF(CURRENT_DATE('US/Eastern'), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 30)
               THEN 1 ELSE 0 END AS months_last_face_to_face
             , CASE WHEN (DATE_DIFF(CURRENT_DATE('US/Eastern'), CAST(client.most_recent_face_to_face_date AS DATE), DAY) < 30
               AND DATE_DIFF(CURRENT_DATE('US/Eastern'), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 0)
               THEN 1 ELSE 0 END AS weeks_last_face_to_face
             , client.assessment_score
             , selected.recorded_start_date
             , selected.supervision_level
             , selected.supervision_type
        FROM selected_employment selected
        LEFT JOIN `{project_id}.{case_triage_dataset}.etl_clients_materialized` client
            ON client.person_external_id = selected.person_external_id
    ),

    --the number of click events of a PO for people with different attributes of supervision
    selected_given AS (
        SELECT client_narrow_select.segment_id
             , client_narrow_select.date
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
             , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "INTERNAL_UNKNOWN")) > 0 AS INT64)) AS persons_selected_given_unknown_supervision
        FROM client_narrow_select
        GROUP BY segment_id, date
    ),

    client_data_updated AS (
        -- Collect the supervision/employment info for the client that had a case update
        SELECT DISTINCT updated.state_code
            , updated.segment_id
            , updated.person_external_id
            , updated.feedback_date AS date
            , compartment_metrics.compartment_level_2 AS supervision_type
            , COALESCE(supervision_level_metrics.supervision_level, "INTERNAL_UNKNOWN") AS supervision_level
            -- indicate if the client had employment when the feedback was given
            , employ.recorded_start_date
            , employ.recorded_end AS recorded_end_date
        FROM `{project_id}.{experiments_dataset}.case_triage_feedback_actions` updated
        LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized` supervision_level_metrics
            ON updated.state_code = supervision_level_metrics.state_code
            AND updated.person_id = supervision_level_metrics.person_id
            AND updated.feedback_date BETWEEN supervision_level_metrics.start_date AND COALESCE(supervision_level_metrics.end_date, CURRENT_DATE('US/Eastern'))
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` compartment_metrics
            ON updated.state_code = compartment_metrics.state_code
            AND updated.person_id = compartment_metrics.person_id
            AND updated.feedback_date BETWEEN compartment_metrics.start_date AND COALESCE(compartment_metrics.end_date, CURRENT_DATE('US/Eastern'))
        LEFT JOIN employ
            ON employ.state_code = updated.state_code
            AND employ.person_external_id = updated.person_external_id
            AND updated.feedback_date BETWEEN employ.recorded_start_date
                AND COALESCE(employ.recorded_end, CURRENT_DATE('US/Eastern'))
    ),

    client_narrow_update AS (
        SELECT updated.person_external_id
             , updated.date
             , updated.segment_id
             , CASE WHEN (DATE_DIFF(CURRENT_DATE('US/Eastern'), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 30) THEN 1 ELSE 0 END AS months_last_face_to_face
             , CASE WHEN (DATE_DIFF(CURRENT_DATE('US/Eastern'), CAST(client.most_recent_face_to_face_date AS DATE), DAY) < 30
               AND DATE_DIFF(CURRENT_DATE('US/Eastern'), CAST(client.most_recent_face_to_face_date AS DATE), DAY) > 0)  THEN 1 ELSE 0 END AS weeks_last_face_to_face
             , client.assessment_score
             , updated.recorded_start_date
             , updated.supervision_level
             , updated.supervision_type
        FROM client_data_updated updated
        -- This may cause issues since this table only has the client's current status
        LEFT JOIN `{project_id}.{case_triage_dataset}.etl_clients_materialized` client
            ON client.person_external_id = updated.person_external_id
    ),

    updated_given AS (
        SELECT client_narrow_update.segment_id
             , client_narrow_update.date
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
             , SUM(CAST(ARRAY_LENGTH(REGEXP_EXTRACT_ALL(supervision_level, "INTERNAL_UNKNOWN")) > 0 AS INT64)) AS persons_updated_given_unknown_supervision
        FROM client_narrow_update
        GROUP BY segment_id, date
    )

    --combine base user data, aggregated PO caseload, click events, update events,
    SELECT *
    FROM baseline
    LEFT JOIN officer_caseload
        USING(officer_external_id, date, state_code)
    LEFT JOIN selected_given
        USING (segment_id, date)
    LEFT JOIN updated_given
        USING (segment_id, date)
"""

CASE_TRIAGE_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=CASE_TRIAGE_EVENTS_VIEW_NAME,
    view_query_template=CASE_TRIAGE_EVENTS_QUERY_TEMPLATE,
    description=CASE_TRIAGE_EVENTS_VIEW_DESCRIPTION,
    static_reference_tables_dataset=STATIC_REFERENCE_TABLES_DATASET,
    po_report_dataset=PO_REPORT_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    case_triage_dataset=CASE_TRIAGE_DATASET,
    case_triage_segment_dataset=CASE_TRIAGE_SEGMENT_DATASET,
    experiments_dataset=EXPERIMENTS_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_TRIAGE_EVENTS_VIEW_BUILDER.build_and_print()
