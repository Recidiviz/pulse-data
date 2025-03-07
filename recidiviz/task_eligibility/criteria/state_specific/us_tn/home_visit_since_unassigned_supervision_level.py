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
"""Spans of time during which a client in TN has had a home visit since
the latest time someone was on Unassigned supervision level
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_HOME_VISIT_SINCE_UNASSIGNED_SUPERVISION_LEVEL"
_QUERY_TEMPLATE = f"""
WITH spans_between_unassigned_status AS (
    SELECT 
       state_code,
       person_id,
       start_date,
       -- Set the next unassigned start date as the end date 
       LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date) AS end_date,
       start_date AS latest_unassigned_start_date,
    FROM `{{project_id}}.sessions.supervision_level_sessions_materialized` sl
    WHERE state_code = "US_TN"
        AND supervision_level = 'UNASSIGNED'
    ),
    critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date AS start_datetime,
            end_date AS end_datetime,
            latest_unassigned_start_date,
            MIN(DATE(ContactNoteDateTime)) AS critical_date,
        FROM spans_between_unassigned_status
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            USING(person_id,state_code)
        LEFT JOIN `{{project_id}}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest` contact
            ON pei.external_id = contact.OffenderID
            -- Confirmed with TN that HOMF (face to face) counts, but HOMC (collateral contact) and HOMV (virtual) don't
            AND contact.ContactNoteType IN ("HOMF")
            AND DATE(ContactNoteDateTime) BETWEEN start_date AND {nonnull_end_date_exclusive_clause('end_date')}
        GROUP BY 1,2,3,4,5
    ),
    {critical_date_has_passed_spans_cte(attributes=['latest_unassigned_start_date'])}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
                    critical_date AS eligible_date,
                    latest_unassigned_start_date
                    )
                ) AS reason,
        critical_date AS eligible_date,
        latest_unassigned_start_date,
    FROM 
        critical_date_has_passed_spans
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        state_code=StateCode.US_TN,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=False,
        reasons_fields=[
            ReasonsField(
                name="eligible_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of first home visit since intake",
            ),
            ReasonsField(
                name="latest_unassigned_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest unassigned status start",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
