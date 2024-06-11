# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time during which
someone is past their semi-annual reclassification date"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    completion_event_state_specific_dataset,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_me_query_fragments import (
    meetings_cte,
    reclassification_shared_logic,
    six_years_remaining_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_ME_INCARCERATION_PAST_SEMI_ANNUAL_CLASSIFICATION_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their semi-annual reclassification date"""

_QUERY_TEMPLATE = f"""
  /* The following CTE unions together different date boundaries: year-span starts, year-span ends, reclassification meeting dates.
  Meeting dates contribute -1 and year-span starts contribute 1 in order to count how many reclassification meetings
  someone has due at a given time. The original session is spanified into yearly sections in order to keep track
   of reclassification meeting debt. */
    WITH annual_reclasses AS (
        SELECT 
            * 
        FROM `{{project_id}}.{{task_eligibility_criteria_us_me}}.incarceration_past_annual_classification_date_materialized`
    ),
    super_sessions_with_6_years_remaining_premerged AS (
        -- This CTE is used to merge the super_sessions with the date at which they will 
        -- have 6 years or less on their sentence. That way it only includes spans of time where 
        -- they require an semi-annual reclassification
         {six_years_remaining_cte(reclass_type='Semi-annual')}
      ),
      super_sessions_with_6_years_remaining AS (
        SELECT 
          syr.* EXCEPT (reclasses_needed),
          IFNULL(GREATEST(
            CAST(JSON_EXTRACT_SCALAR(ar.reason,'$.reclasses_needed') AS INT64),
            syr.reclasses_needed
          ), 0) AS reclasses_needed,
        FROM super_sessions_with_6_years_remaining_premerged syr
        LEFT JOIN annual_reclasses ar
          ON syr.start_date = ar.end_date
            AND syr.person_id = ar.person_id
            AND syr.state_code = ar.state_code
        WHERE NOT (syr.start_date = '1000-01-01')
      ),
      meetings AS (
          {meetings_cte()}
      ), 
      reclass_is_due AS (
          -- Residents start owing 1 reclass after they've been six months in and 1 reclass every six months thereafter
          SELECT
            state_code,
            person_id,
            LEAST(GREATEST(
                    DATE_ADD(actual_start_date, INTERVAL OFFSET MONTH), 
                    start_date),
                 {nonnull_end_date_clause("end_date")}) AS change_date,
            1 AS reclass_type,
          FROM
            super_sessions_with_6_years_remaining,
            UNNEST(GENERATE_ARRAY(6, 720, 6)) AS OFFSET
          WHERE
            OFFSET <= DATE_DIFF({nonnull_end_date_clause("end_date")}, actual_start_date, DAY) / 30
          GROUP BY 1,2,3,4
      ),
      {reclassification_shared_logic(reclass_type='Semi-annual')}"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=True,
    completion_event_us_me_dataset=completion_event_state_specific_dataset(
        StateCode.US_ME
    ),
    task_eligibility_criteria_us_me=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ME
    ),
    reasons_fields=[
        ReasonsField(
            name="reclass_type",
            type=bigquery.enums.SqlTypeNames.STRING,
            description="#TODO(#29059): Add reasons field description",
        ),
        ReasonsField(
            name="reclasses_needed",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="#TODO(#29059): Add reasons field description",
        ),
        ReasonsField(
            name="latest_classification_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="#TODO(#29059): Add reasons field description",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="#TODO(#29059): Add reasons field description",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
