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
"""Creates a view for collapsing raw ID employment data into contiguous periods of employment or unemployment overlapping with a supervision super session"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_EMPLOYMENT_SESSIONS_VIEW_NAME = "us_id_employment_sessions"

US_ID_EMPLOYMENT_SESSIONS_VIEW_DESCRIPTION = """View of continuous periods of unemployment or employment, along with attributes of employment, overlapping with time on supervision"""

US_ID_EMPLOYMENT_SESSIONS_QUERY_TEMPLATE = """
    WITH
      date_array AS (
      SELECT
        *
      FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR), CURRENT_DATE())) supervision_date ),
      employment_clean AS (
      /* Gathers raw data on employment and employers */
      SELECT
        'US_ID' AS state_code,
        person.person_id,
        SAFE_CAST(SPLIT(startdate, ' ')[OFFSET(0)] AS DATE) AS employment_start_date,
        SAFE_CAST(SPLIT(enddate, ' ')[OFFSET(0)] AS DATE) AS employment_end_date,
        SAFE_CAST(SPLIT(e.verifydate, ' ')[OFFSET(0)] AS DATE) AS last_verified_date,
        COALESCE(jobtitle, 'UNKNOWN') AS job_title,
        COALESCE(name, 'UNKNOWN') AS employer_name,
        SAFE_CAST(hoursperweek AS INT64) as hours_per_week,
        SAFE_CAST(wage AS INT64) as wage,
        l.codedescription as employment_end_reason,
        'CIS' AS metric_source,
        -- Infer if an employment entry indicates unemployment
        REGEXP_CONTAINS(LOWER(jobtitle), 'unempl') OR REGEXP_CONTAINS(LOWER(name), 'unempl') AS is_unemployed,
      FROM `{project_id}.us_id_raw_data_up_to_date_views.cis_offender_latest` o
      LEFT JOIN `{project_id}.us_id_raw_data_up_to_date_views.cis_employment_latest` e
      ON e.personemploymentid = o.id
      FULL JOIN `{project_id}.us_id_raw_data_up_to_date_views.cis_personemployment_latest` p
      ON e.personemploymentid = p.id
      LEFT JOIN `{project_id}.us_id_raw_data_up_to_date_views.cis_employer_latest` AS emp
      ON e.employerid = emp.id
      LEFT JOIN `{project_id}.us_id_raw_data_up_to_date_views.cis_codeemploymentreasonleft_latest` AS l
      ON e.codeemploymentreasonleftid = l.id
      LEFT JOIN `{project_id}.state.state_person_external_id` person
      ON o.offendernumber = person.external_id ),
      employment_daily AS (
      /* Unnests employment periods into daily employment data per person and employment */
      SELECT
        s.state_code,
        supervision_date,
        s.person_id,
        employment_start_date,
        employment_end_date,
        last_verified_date,
        job_title,
        employer_name,
        hours_per_week,
        employment_end_reason,
        -- If no employment period overlaps with given date, mark source as "INFERRED" and assume unemployment
        COALESCE(is_unemployed,TRUE) is_unemployed,
        COALESCE(metric_source,'INFERRED') metric_source,
        supervision_super_session_id,
        wage,
      FROM date_array d
      JOIN `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` s
      ON d.supervision_date BETWEEN s.start_date AND COALESCE(s.end_date, CURRENT_DATE())
      LEFT JOIN employment_clean e
      ON d.supervision_date BETWEEN e.employment_start_date AND COALESCE(e.employment_end_date, CURRENT_DATE())
        AND s.person_id = e.person_id
        AND s.state_code = e.state_code
      ),
      dedup_employment_daily AS (
      /* De-duplicates employment data to a single row per person and date, and aggregates multiple job titles/employers
       into a single array */
      SELECT
        state_code,
        person_id,
        is_employed,
        supervision_super_session_id,
        last_verified_date,
        employment_start_date,
        supervision_date,
        DATE_SUB(supervision_date, INTERVAL ROW_NUMBER() 
            OVER(PARTITION BY person_id, is_employed, supervision_super_session_id 
                ORDER BY supervision_date ASC) DAY
            ) AS group_continuous_dates_with_status
      FROM (
        SELECT
          state_code,
          supervision_date,
          person_id,
          supervision_super_session_id,
          -- Get earliest employment start date of all active employment periods on this day
          MIN(employment_start_date) AS employment_start_date,
          -- Get most recent date on which employment was verified for the given period of continuous employment
          MAX(last_verified_date) AS last_verified_date,
          -- If there is at least one period of employment on a given day, mark as employed
          LOGICAL_AND(is_unemployed) IS FALSE AS is_employed,
          -- Collection of attributes of every employment period within continuous period of employment
        FROM employment_daily
        GROUP BY state_code, person_id, supervision_date, supervision_super_session_id
      ) 
    ),
    employment_attributes AS (
    /* Gets distinct raw periods of employment along with information about employment */
        SELECT DISTINCT
            person_id,
            employer_name,
            job_title,
            employment_start_date,
            employment_end_date,
            wage,
            hours_per_week,
            employment_end_reason,
            metric_source
        FROM employment_daily
    ),
    employment_sessions AS (
    /* Aggregates daily data into sessions of continuous employment */
        SELECT DISTINCT
            state_code,
            person_id,
            is_employed,
            supervision_super_session_id, 
            MAX(last_verified_date) OVER(PARTITION BY person_id, is_employed, supervision_super_session_id, group_continuous_dates_with_status) AS last_verified_date,
            MIN(employment_start_date) OVER(PARTITION BY person_id, is_employed, supervision_super_session_id, group_continuous_dates_with_status) AS earliest_employment_period_start_date,
            MIN(supervision_date) OVER(PARTITION BY person_id, is_employed, supervision_super_session_id, group_continuous_dates_with_status) AS employment_status_start_date,
            NULLIF(MAX(supervision_date) OVER(PARTITION BY person_id, is_employed, supervision_super_session_id, group_continuous_dates_with_status), CURRENT_DATE()) AS employment_status_end_date,
        FROM dedup_employment_daily
    ),
    employment_sessions_with_attributes AS (
    /* Joins employment sessions with all overlapping raw periods of employment to get employment attributes */
        SELECT DISTINCT
            s.*, a.* EXCEPT (person_id),
        FROM employment_sessions s
        LEFT JOIN employment_attributes a
            ON s.person_id = a.person_id 
            AND (
                a.employment_start_date BETWEEN s.employment_status_start_date AND COALESCE(s.employment_status_end_date, '9999-01-01')
                OR COALESCE(a.employment_end_date, CURRENT_DATE()) BETWEEN s.employment_status_start_date AND COALESCE(s.employment_status_end_date, '9999-01-01')
            )
            AND COALESCE(employer_name, job_title) IS NOT NULL
    )
    /* Aggregate all employment periods into an array of structs for a single session of continuous employment status */
    SELECT 
        state_code,
        person_id,
        is_employed,
        supervision_super_session_id,
        last_verified_date,
        earliest_employment_period_start_date,
        employment_status_start_date,
        employment_status_end_date,
        ARRAY_AGG(
            STRUCT(employer_name, job_title, employment_start_date, employment_end_date, wage, hours_per_week, employment_end_reason, metric_source)
            ) AS employment_attributes,
    FROM employment_sessions_with_attributes
    GROUP BY 1,2,3,4,5,6,7,8
"""

US_ID_EMPLOYMENT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ID_EMPLOYMENT_SESSIONS_VIEW_NAME,
    description=US_ID_EMPLOYMENT_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_ID_EMPLOYMENT_SESSIONS_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_EMPLOYMENT_SESSIONS_VIEW_BUILDER.build_and_print()
