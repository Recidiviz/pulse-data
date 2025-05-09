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
"""Shows spans of time during which a client has served at least 90 days on their supervision case
(a case is a combination of supervision sessions & sentences)"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_serving_start_date import (
    SENTENCE_SERVING_START_DATE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_0_super_sessions import (
    COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER,
)
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

_CRITERIA_NAME = "US_IA_SERVING_SUPERVISION_CASE_AT_LEAST_90_DAYS"

_DESCRIPTION = __doc__

_REASON_QUERY = f"""
WITH
-- Use compartment level 0 super sessions to get the date each client
-- started a supervision session regardless of type
-- (parole, probation, and out of state supervision are all collapsed)
supervision_sessions AS (
  SELECT
      state_code,
      person_id,
      start_date,
      end_date_exclusive,
  FROM
      `{{project_id}}.{COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER.table_for_query.to_str()}`
  WHERE
      state_code = "US_IA"
      AND compartment_level_0 = "SUPERVISION"
)
,
-- Create the critical date CTE by combining supervision sessions with
-- any sentences that became effective within the supervision session
-- to represent the supervision case start dates:
--    * supervision compartment level 0 sessions start date - the start of supervision while not in custody
--    * sentence serving period start date - the start of a new charge/sentence
--
-- This criteria is met when at least 90 days have passed since the most recent critical date
critical_date_spans AS (
    SELECT DISTINCT
        supervision_sessions.state_code,
        supervision_sessions.person_id,
        -- Start the criteria span on the supervision case start date
        critical_date AS start_datetime,
        -- Close the criteria span when the supervision session ends
        supervision_sessions.end_date_exclusive AS end_datetime,
        critical_date,
    FROM
        supervision_sessions
    LEFT JOIN
        `{{project_id}}.{SENTENCE_SERVING_START_DATE_VIEW_BUILDER.table_for_query.to_str()}` serving_periods
    ON
        supervision_sessions.state_code = serving_periods.state_code
        AND supervision_sessions.person_id = serving_periods.person_id
        -- Join sentences that started within the supervision super session
        AND serving_periods.effective_date BETWEEN supervision_sessions.start_date
            AND {nonnull_end_date_exclusive_clause("supervision_sessions.end_date_exclusive")},
    UNNEST
        -- Unnest each relevant supervision case date (supervision super session start & sentence serving start)
        ([supervision_sessions.start_date, serving_periods.effective_date]) AS critical_date
    WHERE
        -- Do not include NULL values for clients who did not get a new sentence during the supervision session
        critical_date IS NOT NULL
)
,
-- Set the critical date to 90 days after the case start date,
-- so the "leading window" = -90 days
{critical_date_has_passed_spans_cte(
    meets_criteria_leading_window_time=-90,
    date_part="DAY",
)}
,
-- Sub-sessionize the critical date output to remove the overlapping spans
-- created when a supervision session has multiple supervision case start dates
{create_sub_sessions_with_attributes(table_name="critical_date_has_passed_spans")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    -- Criteria is met when all relevant critical dates have passed the 90 day window
    LOGICAL_AND(critical_date_has_passed) AS meets_criteria,
    -- Use the latest case start date for the reasons field
    MAX(critical_date) AS supervision_case_start_date,
    TO_JSON(STRUCT(MAX(critical_date) AS supervision_case_start_date)) AS reason,
FROM
    sub_sessions_with_attributes
GROUP BY
    1, 2, 3, 4
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        state_code=StateCode.US_IA,
        criteria_spans_query_template=_REASON_QUERY,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="supervision_case_start_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date the latest supervision case began",
            )
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
