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
"""Describes spans of time when someone is ineligible due to a classification as homeless
in their release plan."""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_NOT_HOMELESS_IN_RELEASE_PLAN"

_QUERY_TEMPLATE = f"""
    WITH
      -- Filters to grab the most recent home plan reason status for a given resident in a given incarceration stint
      -- Also creates spans using start_dates only since home plan status is set during incarceration and 
      -- needs to exist continuously through supervision
      -- This method ensures that we see changes in home plan status and can later get accurate spans of homeless statuses
      recent_hp_status AS (
          SELECT
            iss.state_code,
            hp.person_id,
            hp.start_date,
            LEAD(hp.start_date) OVER (PARTITION BY hp.person_id ORDER BY hp.start_date) AS end_date,
            hp.is_homeless_request,
          FROM
            `{{project_id}}.analyst_data.us_az_home_plan_preprocessed_materialized` hp
          INNER JOIN
            `{{project_id}}.sessions.incarceration_super_sessions_materialized` iss
          ON
            iss.person_id = hp.person_id
            AND hp.start_date BETWEEN iss.start_date
            AND IFNULL(iss.end_date, '9999-12-31')
            AND iss.state_code = 'US_AZ'
          QUALIFY
            ROW_NUMBER() OVER (PARTITION BY hp.person_id, iss.incarceration_super_session_id ORDER BY hp.start_date DESC ) = 1 ),
    remove_zero_days AS (
        -- There are some overlapping spans in the home plan view that lead to zero day spans in the above CTE
          SELECT
            *
          FROM 
            recent_hp_status
          WHERE 
            start_date != end_date
    )
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      FALSE AS meets_criteria,
      TO_JSON(STRUCT(
                is_homeless_request
            )) AS reason,
      is_homeless_request,
    FROM
      ({aggregate_adjacent_spans("remove_zero_days", attribute='is_homeless_request')})
    WHERE is_homeless_request = 'Y'
    """

_REASONS_FIELDS = [
    ReasonsField(
        name="is_homeless_request",
        type=bigquery.enums.StandardSqlTypeNames.BOOL,
        description="A boolean indicator for when a resident has a homeless request in their home plan during a given span",
    )
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
