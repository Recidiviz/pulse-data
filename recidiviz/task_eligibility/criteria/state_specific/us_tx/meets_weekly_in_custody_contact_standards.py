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

"""Defines a criteria view that shows spans of time for which in-custody supervision 
clients are compliant with their weekly invesstigative F2F contacts.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TX_MEETS_WEEKLY_IN_CUSTODY_CONTACT_STANDARDS"

_DESCRIPTION = """Defines a criteria view that shows spans of time for which in custody
supervision clients meet standards for their weekly investigative contacts."""

_QUERY_TEMPLATE = f"""
WITH
-- Create periods with case type and supervision level information
person_info AS (
   SELECT 
      sp.person_id,
      start_date,
      termination_date AS end_date,
      supervision_level,
      case_type,
      case_type_raw_text,
      sp.state_code,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_case_type_entry` ct
      USING(supervision_period_id)
    WHERE sp.state_code = "US_TX" AND sp.supervision_level = "IN_CUSTODY"
),
-- Aggregate above periods by supervision_level and case_type
person_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='person_info',
        attribute=['supervision_level','case_type','case_type_raw_text'],
        session_id_output_name='person_info_agg',
        end_date_field_name='end_date'
    )}
),
-- Creates table of all "Investigative" contacts that were completed
contact_info AS (
    SELECT 
        person_id,
        contact_date,
        external_id,
        Contact_reason,
        contact_method_raw_text,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact` 
    WHERE status = "COMPLETED" AND contact_reason_raw_text = "INVESTIGATIVE"
),
-- Begins calculating how many week long periods there have been since the person began
-- their in custody period. 
contacts_compliance AS (
    SELECT DISTINCT
        p.person_id,
        p.start_date,
        p.end_date,
        p.supervision_level,
        DATE_TRUNC(start_date, WEEK(MONDAY)) + INTERVAL 1 WEEK AS week_start,
        DATE_DIFF(COALESCE(end_date, CURRENT_DATE), start_date, WEEK) + 1 AS total_weeks
    FROM person_info_agg p
),
-- Creates week long contact compliance periods
empty_weekly_periods AS (
    SELECT 
        person_id,
        supervision_level,
        start_date,
        end_date,
        week_start + INTERVAL week_index WEEK AS week_start,
        week_start + INTERVAL (week_index + 1) WEEK - INTERVAL 1 DAY AS week_end
    FROM contacts_compliance,
    UNNEST(GENERATE_ARRAY(0, total_weeks - 1)) AS week_index
),
-- Associates contacts with the given week they were conducted during
period_compliance AS (
    SELECT
        ewp.person_id,
        supervision_level,
        start_date,
        end_date,
        week_start,
        week_end,
        ci.contact_date
    FROM empty_weekly_periods ewp
    LEFT JOIN contact_info ci
        ON ewp.person_id = ci.person_id
        AND ci.contact_date BETWEEN week_start AND week_end
    WHERE week_start < end_date OR end_date IS NULL

),
-- Unions all critical dates for periods
critical_dates AS 
(
    SELECT
        person_id,
        start_date,
        week_start,
        week_end,
        contact_date as critical_date
    FROM period_compliance
    WHERE contact_date IS NOT NULL

    UNION DISTINCT
    
    SELECT
        person_id,
        start_date,
        week_start,
        week_end,
        week_start as critical_date
    FROM period_compliance

    UNION DISTINCT

    SELECT
        person_id,
        start_date,
        week_start,
        week_end,
        week_end as critical_date
    FROM period_compliance
),
-- Begins dividing periods into smaller periods depending on when contacts were done
divided_periods as (
    SELECT
        week_start,
        week_end,
        person_id,
        critical_date as period_start,
        LEAD (critical_date) OVER(PARTITION BY week_start,person_id ORDER BY critical_date) AS period_end,
    FROM critical_dates
),
-- Associates contacts within their more specific periods
divided_periods_with_contacts as (
    SELECT
        p.person_id,
        COUNT(ci.external_id) AS contact_count,
        period_start,
        period_end,
        week_start,
        week_end,
    FROM divided_periods p
    LEFT JOIN contact_info ci
        ON p.person_id = ci.person_id
        AND ci.contact_date BETWEEN p.week_start and DATE_SUB(p.period_end, INTERVAL 1 DAY)
    WHERE period_end IS NOT NULL
    GROUP BY p.person_id, week_start, week_end, period_start, period_end
),
-- Creates periods of time in which a person is compliance given a contact and it's cadence
compliance_periods as (
    SELECT 
        person_id,
        contact_count,
        period_start,
        period_end,
        week_start,
        week_end,
    FROM divided_periods_with_contacts
),
-- Creates final periods of compliance
periods AS (
    SELECT 
        lc.person_id,
        "US_TX" as state_code,
        contact_count >= 1 AS meets_criteria,
        contact_count < 1 AND CURRENT_DATE > week_end AS overdue_flag,
        CASE
            WHEN period_start = week_start
                THEN "start"
            WHEN period_end = week_end
                THEN "end"
            ELSE "contact"
        END AS period_type,
        week_end AS contact_due_date,
        contact_count,
        period_start AS start_date,
        period_end AS end_date,
        (SELECT MAX(contact_date) FROM contact_info ci 
            WHERE ci.person_id = lc.person_id AND ci.contact_date < lc.period_end) 
        AS last_contact_date,
        TO_JSON(STRUCT(contact_count >= 1 AS compliance)) AS reason,
        "Weekly In-Custody" AS reason_for_contact,
    FROM compliance_periods lc
)
SELECT 
  *
FROM periods
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        state_code=StateCode.US_TX,
        normalized_state_dataset="us_tx_normalized_state",
        reasons_fields=[
            ReasonsField(
                name="contact_due_date",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Due date of the contact.",
            ),
            ReasonsField(
                name="reason_for_contact",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type of period.",
            ),
            ReasonsField(
                name="overdue_flag",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether contact was missed.",
            ),
            ReasonsField(
                name="meets_criteria",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Flag that indicates whether the the criteria was met.",
            ),
            ReasonsField(
                name="last_contact_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last contact.",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
