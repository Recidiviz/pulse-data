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
# ============================================================================
"""Defines a criteria span view that shows spans of time during which
someone is past their annual reclassification date"""
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

_CRITERIA_NAME = "US_ME_INCARCERATION_PAST_ANNUAL_CLASSIFICATION_DATE"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone is past their annual reclassification date"""

_QUERY_TEMPLATE = f"""
    #TODO(#42446): simplify query, consider using general critical date logic
      /* The following CTEs take the time between an individual's start and end date and spanifies it into yearly 
        spans. The logic checks whether a given span has a meeting date within its start and end date 
        OR if a reclassification meeting took place within 60 days of the span's start date. A one-to-one mapping 
        between reclassification dates and spans is created, favoring the earliest span a meeting date can be 
        associated with. 
      */
      WITH super_sessions_with_6_years_remaining AS (
      -- This CTE finds all critical dates for when an individual has 6 years or more remaining on their sentence
          {six_years_remaining_cte(reclass_type='Annual')}
      ),
      meetings AS (
       -- This CTE grabs all past reclassification meeting dates for all individuals 
          {meetings_cte()}
      ), 
      reclass_due_dates AS (
      -- This CTE generates a list of due dates for each individual during their incarceration stint
      -- We use a 120 year generation period in order to ensure we capture all possible future spans 
          SELECT
            state_code,
            person_id,
            incarceration_super_session_id,
            LEAST(DATE_ADD(start_date, INTERVAL OFFSET YEAR), 
                {nonnull_end_date_clause("end_date")}) AS reclass_is_due_date,
          FROM
            super_sessions_with_6_years_remaining,
            UNNEST(GENERATE_ARRAY(0, 100, 1)) AS OFFSET
          WHERE
            OFFSET <= (DATE_DIFF({nonnull_end_date_clause("end_date")}, start_date, DAY) / 365) + 1
          GROUP BY 1,2,3,4
      ),
      {reclassification_shared_logic(reclass_type='Annual')}
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    state_code=StateCode.US_ME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    meets_criteria_default=False,
    completion_event_us_me_dataset=completion_event_state_specific_dataset(
        StateCode.US_ME
    ),
    task_eligibility_criteria_us_me=task_eligibility_criteria_state_specific_dataset(
        StateCode.US_ME
    ),
    reasons_fields=[
        ReasonsField(
            name="reclass_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Reclass type: annual or semi-annual",
        ),
        ReasonsField(
            name="latest_classification_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Most recent classification date",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when the resident is past their annual/semi-annual reclassification date",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
