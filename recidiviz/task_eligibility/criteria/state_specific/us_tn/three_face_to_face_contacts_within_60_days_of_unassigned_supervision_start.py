# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""Spans of time during which a client in TN had 3 face to face supervision contacts
within 60 days of being on Unassigned supervision level
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import FACE_TO_FACE_CONTACTS
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = (
    "US_TN_THREE_FACE_TO_FACE_CONTACTS_WITHIN_60_DAYS_OF_UNASSIGNED_SUPERVISION_START"
)

_QUERY_TEMPLATE = f"""
WITH spans_between_unassigned_status AS (
    SELECT 
           state_code,
           person_id,
           start_date,
           -- Set the next unassigned start date as the end date 
           LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date) AS end_date_exclusive,
           start_date AS latest_unassigned_start_date,
    FROM `{{project_id}}.sessions.supervision_level_sessions_materialized` sl
    WHERE state_code = "US_TN"
        AND supervision_level = 'UNASSIGNED'
    ),
    f2f_contacts AS (
          SELECT
            state_code,
            person_id,
            DATE(ContactNoteDateTime) AS contact_date,
            ARRAY_AGG(ContactNoteType ORDER BY ContactNoteType) AS contact_type                    
          FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest` contact
          INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            ON contact.OffenderID=pei.external_id                
            AND pei.state_code="US_TN"
            AND pei.id_type="US_TN_DOC"
          WHERE contact.ContactNoteType IN ('{{face_contact_codes}}')
          GROUP BY 1, 2, 3
    ),
    criterion_spans AS (
        SELECT
            f.state_code,
            f.person_id,
            f.contact_date AS start_date,
            c.latest_unassigned_start_date,
            -- This ensures that the span ends either when the next contact (within the assigned span) occurs or
            -- when the unassigned span ends
            COALESCE(
                    LEAD(f.contact_date) OVER(PARTITION BY f.person_id, c.start_date 
                        ORDER BY f.contact_date),
                    c.end_date_exclusive
            ) AS end_date,
            -- If contact date is within 60 days of start, and there are 3 or more, then the criteria is met
            (COUNTIF(DATE_DIFF(f.contact_date, c.start_date, DAY)<=60) 
                OVER person_unassigned_window_cumulative) >= 3 AS meets_criteria,
            -- This includes all contacts since the unassigned start
            ARRAY_AGG(STRUCT(f.contact_type, f.contact_date)) 
                OVER person_unassigned_window_cumulative AS face_to_face_contacts_array,
        FROM f2f_contacts f
        INNER JOIN spans_between_unassigned_status c
            ON c.person_id = f.person_id
            AND f.contact_date BETWEEN c.start_date AND {nonnull_end_date_exclusive_clause('end_date_exclusive')}
        WINDOW person_unassigned_window_cumulative AS (
            PARTITION BY c.person_id, c.start_date
            ORDER BY f.contact_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) 
    )
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        meets_criteria,
        latest_unassigned_start_date,
        face_to_face_contacts_array,
        TO_JSON(STRUCT(
            latest_unassigned_start_date,
            face_to_face_contacts_array
        )) AS reason,
      FROM criterion_spans 
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    state_code=StateCode.US_TN,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
    face_contact_codes="', '".join(FACE_TO_FACE_CONTACTS),
    reasons_fields=[
        ReasonsField(
            name="face_to_face_contacts_array",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Array that contains dates and contact type of face to face contacts since starting unassigned",
        ),
        ReasonsField(
            name="latest_unassigned_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest unassigned status start",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
